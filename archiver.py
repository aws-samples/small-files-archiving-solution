#!/usr/bin/env python3

import argparse
import logging
from datetime import datetime
from archivers.fs2s3archiver import FS2S3Archiver
from archivers.fs2fsarchiver import FS2FSArchiver
from archivers.s32s3archiver import S32S3Archiver
from archivers.s32fsarchiver import S32FSArchiver

def setup_logging(log_level):
    logging.basicConfig(level=getattr(logging, log_level),
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler()])
    return logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description="File Archiving Tool")
    parser.add_argument("--src-type", choices=["fs", "s3"], required=True, help="Source type: filesystem or S3")
    parser.add_argument("--dst-type", choices=["fs", "s3"], required=True, help="Destination type: filesystem or S3")
    parser.add_argument("--src-path", help="Source filesystem path or S3 prefix")
    parser.add_argument("--src-bucket", help="Source S3 bucket")
    parser.add_argument("--dst-path", help="Destination filesystem path or S3 prefix")
    parser.add_argument("--dst-bucket", help="Destination S3 bucket")
    parser.add_argument("--input-file", help="Path to input file containing list of files to archive")
    parser.add_argument("--profile-name", help="AWS profile name")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--max-process", type=int, default=4, help="Maximum number of parallel processes")
    parser.add_argument("--combine", choices=["count", "size"], default="size", help="Combine files by count or size")
    parser.add_argument("--max-file-number", type=int, default=1000, help="Maximum number of files per archive")
    parser.add_argument("--max-tarfile-size", type=int, default=1024*1024*1024, help="Maximum size of tar file in bytes")
    parser.add_argument("--region", help="AWS region")
    return parser.parse_args()

def generate_report(args, start_time, end_time, archiver):
    duration = end_time - start_time
    
    if args.src_type == 'fs':
        source = f"fs://{args.src_path or 'N/A'}"
    else:  # s3
        source = f"s3://{args.src_bucket or 'N/A'}/{args.src_path or ''}"
    
    if args.dst_type == 'fs':
        destination = f"fs://{args.dst_path or 'N/A'}"
    else:  # s3
        destination = f"s3://{args.dst_bucket or 'N/A'}/{args.dst_path or ''}"
    
    report = f"""
Job Summary Report
==================
Start time: {start_time}
End time: {end_time}
Duration: {duration}
Total transferred files: {archiver.total_files}
Failed files: {archiver.failed_files}
Total tar files: {archiver.total_tar_files}
Total manifest files: {archiver.total_manifest_files}
Source: {source}
Destination: {destination}
Combine method: {args.combine}
Max processes: {args.max_process}
"""
    if args.combine == 'count':
        report += f"Max files per archive: {args.max_file_number}\n"
    else:
        report += f"Max archive size: {args.max_tarfile_size} bytes\n"
    
    return report

def main():
    args = parse_arguments()
    logger = setup_logging(args.log_level)

    start_time = datetime.now()

    if args.src_type == "fs" and args.dst_type == "s3":
        archiver = FS2S3Archiver(args)
    elif args.src_type == "fs" and args.dst_type == "fs":
        archiver = FS2FSArchiver(args)
    elif args.src_type == "s3" and args.dst_type == "s3":
        archiver = S32S3Archiver(args)
    elif args.src_type == "s3" and args.dst_type == "fs":
        archiver = S32FSArchiver(args)
    else:
        logger.error(f"Invalid combination of source and destination types: {args.src_type} to {args.dst_type}")
        return

    archiver.run()

    end_time = datetime.now()

    report = generate_report(args, start_time, end_time, archiver)
    print(report)

    # Optionally, you can also log the report
    logger.info("Job Summary:\n" + report)

if __name__ == "__main__":
    main()

