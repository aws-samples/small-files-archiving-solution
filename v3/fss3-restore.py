"""
fss3-restore.py
===============
Restore an individual file that was bundled into a tar by
`fss3-archiver-s3tables-mp.py`, without downloading the whole tar.

Two modes:

  DIRECT  -- you already have the tar location and byte range (e.g. from an
             Athena / PyIceberg query against the manifest). Pass them in:
                 --tar-bucket --tar-key --byte-start --byte-end
             No S3 Tables access is needed.

  LOOKUP  -- you only know the original file's relative path. The script queries
             the S3 Tables (Iceberg) manifest for the tar + byte range, then
             restores it:
                 --region --table-bucket-arn --source-key --tar-bucket
             (Uses an exact match with Iceberg predicate pushdown -- no full scan.)

How it works: the archiver records byte_start at the file's 512-byte tar HEADER
and byte_end at the end of its padding, so the byte range is a self-contained
single-member tar. A ranged GET of that slice can be opened directly with
tarfile and the member extracted. The manifest md5 (when present) is the md5 of
the ORIGINAL file content, so it is verified against the extracted bytes.

Requires: pip install boto3           (DIRECT mode)
          pip install "pyiceberg[pyarrow]" boto3   (LOOKUP mode)

Examples:
  # DIRECT -- values taken from a manifest query
  python fss3-restore.py \
    --tar-bucket my-bucket \
    --tar-key 20260804/20260804/archive_20260804_120000_00001.tar \
    --byte-start 1536 --byte-end 21503 \
    --out ./restored/file0499.dat

  # LOOKUP -- exact source_key, offsets fetched from the manifest
  python fss3-restore.py \
    --region ap-northeast-2 \
    --table-bucket-arn arn:aws:s3tables:ap-northeast-2:<account>:bucket/<table-bucket> \
    --namespace archive --table-name manifest \
    --source-key sub/file0499.dat \
    --tar-bucket my-bucket \
    --out-dir ./restored/ --verify-md5
"""
import os
import io
import sys
import hashlib
import argparse
import tarfile

import boto3


# --------------------------------------------------------------------------- #
# Manifest lookup (LOOKUP mode only): exact source_key match with pushdown.
# --------------------------------------------------------------------------- #
def lookup_offsets(region, table_bucket_arn, namespace, table_name,
                   source_key, profile_name=None):
    """Return a dict with tar_key/byte_start/byte_end/md5 for source_key.

    Uses an equality row filter so Iceberg prunes to matching data files instead
    of scanning the whole manifest. Errors on no match; on multiple matches
    (same path archived in more than one run) picks the newest archive_ts and
    warns, so the choice is always visible.
    """
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import EqualTo

    props = {
        "type": "rest",
        "warehouse": table_bucket_arn,
        "uri": f"https://s3tables.{region}.amazonaws.com/iceberg",
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "s3tables",
        "rest.signing-region": region,
    }
    if profile_name:
        # pyiceberg's S3 FileIO / SigV4 signer picks up the boto3 default
        # session; select the profile via the environment for the child call.
        os.environ["AWS_PROFILE"] = profile_name

    catalog = load_catalog("s3tables", **props)
    table = catalog.load_table(f"{namespace}.{table_name}")

    rows = (table.scan(
                row_filter=EqualTo("source_key", source_key),
                selected_fields=("tar_key", "source_key", "byte_start",
                                 "byte_end", "md5", "filesize", "archive_ts"))
            .to_arrow().to_pylist())

    if not rows:
        raise SystemExit(
            f"ERROR: no manifest row for source_key={source_key!r} "
            f"in {namespace}.{table_name}")
    if len(rows) > 1:
        rows.sort(key=lambda r: r["archive_ts"], reverse=True)
        print(f"WARNING: {len(rows)} manifest rows match source_key="
              f"{source_key!r}; using the newest (archive_ts="
              f"{rows[0]['archive_ts']}). Query the manifest and use DIRECT "
              f"mode with an explicit --tar-key to pick a different one.",
              file=sys.stderr)
    return rows[0]


# --------------------------------------------------------------------------- #
# Core restore: ranged GET of a single-member tar slice, then extract.
# --------------------------------------------------------------------------- #
def restore_range(s3, tar_bucket, tar_key, byte_start, byte_end,
                  expected_name=None):
    """Fetch [byte_start, byte_end] from the tar and return (name, data)."""
    if byte_start < 0 or byte_end < 0:
        raise SystemExit(
            "ERROR: byte offsets are -1, which means this file was archived "
            "with --compress (gzip). Byte-range restore is not possible for "
            "compressed tars; download and extract the whole .tar.gz instead.")
    if byte_end < byte_start:
        raise SystemExit(
            f"ERROR: byte_end ({byte_end}) < byte_start ({byte_start}).")

    try:
        resp = s3.get_object(Bucket=tar_bucket, Key=tar_key.lstrip("/"),
                             Range=f"bytes={byte_start}-{byte_end}")
    except s3.exceptions.NoSuchKey:
        raise SystemExit(
            f"ERROR: tar object not found: s3://{tar_bucket}/{tar_key}")
    except s3.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "InvalidObjectState":
            raise SystemExit(
                f"ERROR: s3://{tar_bucket}/{tar_key} is in an archive storage "
                f"class (Glacier Flexible / Deep Archive) and must be restored "
                f"before a ranged GET works. GLACIER_IR needs no restore.")
        raise

    chunk = resp["Body"].read()
    # The slice is exactly one tar member (header + data + padding), so a plain
    # (uncompressed) tar read of the first member is sufficient.
    with tarfile.open(fileobj=io.BytesIO(chunk), mode="r:") as tf:
        member = tf.next()
        if member is None:
            raise SystemExit(
                "ERROR: no tar member found in the fetched byte range; the "
                "offsets may not match this tar.")
        if expected_name is not None and member.name != expected_name:
            print(f"WARNING: extracted member name {member.name!r} does not "
                  f"match expected {expected_name!r}.", file=sys.stderr)
        data = tf.extractfile(member).read()
    return member.name, data


def write_output(data, out, out_dir, member_name):
    """Resolve the destination path and write bytes to it."""
    if out:
        dest = out
    else:
        # Preserve the relative path from the archive under out_dir, but never
        # let a crafted member name escape out_dir.
        safe_rel = os.path.normpath(member_name).lstrip(os.sep)
        if safe_rel.startswith(".." + os.sep) or safe_rel == "..":
            raise SystemExit(f"ERROR: unsafe member name {member_name!r}")
        dest = os.path.join(out_dir, safe_rel)
    os.makedirs(os.path.dirname(os.path.abspath(dest)), exist_ok=True)
    with open(dest, "wb") as f:
        f.write(data)
    return dest


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main():
    p = argparse.ArgumentParser(
        description="Restore one bundled file from an S3 tar via byte-range GET.",
        formatter_class=argparse.RawDescriptionHelpFormatter)

    # Where the tar lives + local output (both modes)
    p.add_argument("--tar-bucket", required=True,
                   help="S3 bucket holding the tar objects (not stored in the "
                        "manifest, so it must be supplied)")
    p.add_argument("--out", help="exact output file path")
    p.add_argument("--out-dir", help="output directory; the file's archived "
                                     "relative path is preserved under it")
    p.add_argument("--profile-name", default=None, help="AWS profile")

    # DIRECT mode
    d = p.add_argument_group("DIRECT mode (offsets already known)")
    d.add_argument("--tar-key", help="S3 key of the tar")
    d.add_argument("--byte-start", type=int, help="range start (inclusive)")
    d.add_argument("--byte-end", type=int, help="range end (inclusive)")

    # LOOKUP mode
    l = p.add_argument_group("LOOKUP mode (query the manifest by source_key)")
    l.add_argument("--source-key", help="exact original relative path to find")
    l.add_argument("--region", help="AWS region of the S3 Tables bucket")
    l.add_argument("--table-bucket-arn", help="S3 Tables table bucket ARN")
    l.add_argument("--namespace", default="archive")
    l.add_argument("--table-name", default="manifest")

    p.add_argument("--verify-md5", nargs="?", const=True, default=False,
                   help="verify the extracted bytes against md5. In LOOKUP mode "
                        "pass the flag alone to use the manifest md5; in either "
                        "mode pass --verify-md5 <hex> to check an explicit value")
    args = p.parse_args()

    if not args.out and not args.out_dir:
        p.error("provide --out or --out-dir")
    if args.out and args.out_dir:
        p.error("use only one of --out / --out-dir")

    direct = args.tar_key is not None or args.byte_start is not None \
        or args.byte_end is not None
    lookup = args.source_key is not None

    if lookup and direct:
        p.error("choose one mode: DIRECT (--tar-key/--byte-start/--byte-end) "
                "OR LOOKUP (--source-key), not both")

    session = boto3.Session(profile_name=args.profile_name)
    expected_name = None
    manifest_md5 = None

    if lookup:
        if not (args.region and args.table_bucket_arn):
            p.error("LOOKUP mode needs --region and --table-bucket-arn")
        row = lookup_offsets(args.region, args.table_bucket_arn,
                             args.namespace, args.table_name,
                             args.source_key, args.profile_name)
        tar_key = row["tar_key"]
        byte_start, byte_end = row["byte_start"], row["byte_end"]
        manifest_md5 = row.get("md5")
        expected_name = args.source_key
        print(f"manifest: {args.source_key} -> s3://{args.tar_bucket}/{tar_key} "
              f"bytes {byte_start}-{byte_end}", file=sys.stderr)
    elif direct:
        if args.tar_key is None or args.byte_start is None or args.byte_end is None:
            p.error("DIRECT mode needs --tar-key, --byte-start and --byte-end")
        tar_key = args.tar_key
        byte_start, byte_end = args.byte_start, args.byte_end
    else:
        p.error("specify a mode: DIRECT (--tar-key/--byte-start/--byte-end) "
                "or LOOKUP (--source-key)")

    s3 = session.client("s3", region_name=args.region)
    name, data = restore_range(s3, args.tar_bucket, tar_key,
                               byte_start, byte_end, expected_name)

    # Resolve which md5 to check against.
    expected_md5 = None
    if args.verify_md5 is True:            # bare --verify-md5
        expected_md5 = manifest_md5
        if expected_md5 is None:
            print("WARNING: --verify-md5 given but no md5 available (archived "
                  "with --no-md5, or DIRECT mode without an explicit value); "
                  "skipping verification.", file=sys.stderr)
    elif isinstance(args.verify_md5, str):  # --verify-md5 <hex>
        expected_md5 = args.verify_md5

    if expected_md5:
        got = hashlib.md5(data).hexdigest()
        if got != expected_md5:
            raise SystemExit(
                f"ERROR: md5 mismatch: expected {expected_md5}, got {got}")
        print(f"md5 OK ({got})", file=sys.stderr)

    dest = write_output(data, args.out, args.out_dir, name)
    print(f"restored {name} ({len(data):,} bytes) -> {dest}")


if __name__ == "__main__":
    main()
