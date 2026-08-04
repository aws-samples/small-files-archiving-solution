# Small Files Archiver → S3 + S3 Tables Manifest

> 🌐 **English**: [README.md](README.md) · 한국어 문서입니다.

파일시스템의 대량 소용량 파일을 **tar로 번들링**하여 Amazon S3에 저장하고, 각 파일의 위치 정보(어느 tar의 어느 byte 범위에 있는지)를 **Amazon S3 Tables(관리형 Apache Iceberg)** 테이블에 기록하는 아카이빙 솔루션입니다.

원본은 [aws-samples/small-files-archiving-solution](https://github.com/aws-samples/small-files-archiving-solution)이며, 이 버전은 매니페스트 저장을 CSV 오브젝트에서 S3 Tables로 전환하고, 단일 읽기 MD5·스트리밍 tar·배치 커밋·멀티프로세스 병렬화를 적용해 재구성했습니다.

> ⚠️ 공식 AWS 문서가 아닙니다. 현장 테스트 기반 자료입니다.

---

## 1. 솔루션 소개

### 왜 번들링인가

수십 KB 단위 파일이 수십만~수십억 개인 아카이브를 S3에 개별 오브젝트로 올리면 두 가지 비용이 폭증합니다.

- **요청 비용**: 파일 1개당 PUT 1건 → 수백만~수십억 PUT
- **소용량 페널티**: Glacier Instant Retrieval·Standard-IA는 **128KB 최소 과금**, Glacier Flexible/Deep Archive는 **객체당 40KB 메타데이터 오버헤드**. 수십 KB 파일에는 실제 크기보다 훨씬 큰 요금이 부과됩니다.

파일들을 큰 tar(예: 20MB)로 묶으면 오브젝트 수가 수백~수천 배 줄어 위 두 비용이 사실상 사라집니다. 대신 "tar 안의 개별 파일을 어떻게 찾고 꺼내느냐"가 문제인데, 이를 **매니페스트(파일→tar+byte범위 인덱스)** 로 해결합니다.

### 아키텍처

```
파일 스캐너
   └─ 파일 목록을 용량(--max-size) 또는 개수(--max-files) 기준으로 배치 분할
        └─ ProcessPoolExecutor (N개 워커 프로세스)
              │  각 워커: tar 생성 (옵션 MD5) + S3 multipart 업로드
              │  → 매니페스트 행 반환
              └─ 단일 writer 스레드 → S3 Tables 배치 커밋 (기본 50,000행)
```

스크립트: `fss3-archiver-s3tables-mp.py` (멀티프로세스). tar 패킹·MD5 등 CPU 작업을 워커 프로세스로 병렬화하여 GIL을 우회합니다.

설계 포인트:

| 항목 | 방식 | 이유 |
|---|---|---|
| MD5 | tar 스트리밍 중 1회 계산 (옵션) | 파일을 2번 읽지 않음 |
| tar 버퍼 | `SpooledTemporaryFile` (임계 초과 시 디스크 spill) | 대용량 tar의 RAM 상주 방지 |
| byte offset | tar 스트림 `tell()` 위치 | tar 헤더·패딩 포함 실제 물리 위치 → byte-range 복원 정확 |
| 매니페스트 쓰기 | **단일** writer가 배치 커밋 | Iceberg 낙관적 동시성(OCC) 충돌·작은 파일/스냅샷 폭증 방지 |
| 프로세스 시작 | `spawn` 컨텍스트 | writer 스레드가 락을 쥔 상태에서 fork 시 자식 데드락 방지 |
| S3 Tables 연결 | S3 Tables Iceberg REST endpoint 직접 사용 | Lake Formation 설정 불필요 |

---

## 2. 설치 방법

- **Python 3.9+** (테스트 환경: 3.12.1)
- 사전 생성된 S3 버킷(tar 저장용)과 S3 Tables 테이블 버킷(매니페스트용). 네임스페이스·테이블은 스크립트가 자동(idempotent) 생성.

```bash
pip install "pyiceberg[pyarrow]" boto3
# 테스트 검증: pyiceberg 0.11.1 / pyarrow 18.1.0 / boto3 1.39.17
```

필요 IAM 권한 (매니페스트, S3 Tables 직접 endpoint 기준):

```
s3tables:GetTableBucket, ListNamespaces, CreateNamespace, GetNamespace,
s3tables:ListTables, CreateTable, GetTable,
s3tables:GetTableMetadataLocation, UpdateTableMetadataLocation,
s3tables:PutTableData, GetTableData
+ tar 저장 버킷에 대한 s3:PutObject
```

---

## 3. 실행 방법

```bash
python fss3-archiver-s3tables-mp.py \
  --src-path         /data/nfsshare \
  --dst-bucket       your-own-dest-bucket \
  --dst-prefix       20260804 \
  --max-size         20MB \
  --num-workers      8 \
  --md5 \
  --region           ap-northeast-2 \
  --table-bucket-arn arn:aws:s3tables:ap-northeast-2:<account>:bucket/<table-bucket> \
  --namespace        archive \
  --table-name       manifest \
  --batch-size       50000 \
  --tar-storageclass GLACIER_IR
```

### 주요 파라미터

| 파라미터 | 설명 | 기본값 |
|---|---|---|
| `--src-path` | 아카이빙할 소스 디렉터리 (하위 재귀) | (필수) |
| `--dst-bucket` / `--dst-prefix` | tar 저장 S3 버킷 / prefix | (필수) |
| `--max-size` **또는** `--max-files` | tar 1개당 크기(예 `512MB`) 또는 파일 개수 (택1) | (필수) |
| `--num-workers` | tar 생성·전송 워커 프로세스 수 | `os.cpu_count()` |
| `--md5` / `--no-md5` | 파일별 MD5 계산 여부 (끄면 md5 컬럼 null, 약간 빠름) | off |
| `--region` | AWS 리전 | (필수) |
| `--table-bucket-arn` | S3 Tables 테이블 버킷 ARN | (필수) |
| `--namespace` / `--table-name` | 매니페스트 네임스페이스 / 테이블 | archive / manifest |
| `--batch-size` | 매니페스트 커밋당 행 수 | 50000 |
| `--tar-storageclass` | tar 오브젝트 스토리지 클래스 | GLACIER_IR |
| `--spool-max-mb` | tar를 메모리에 두는 임계(초과 시 디스크) | 256 |
| `--compress` | tar를 gzip 압축 (⚠️ 아래 제약 참고) | off |

저장 경로: tar는 `s3://<bucket>/<prefix>/<YYYYMMDD>/archive_*.tar`, 매니페스트는 S3 Tables 테이블에 기록.

### 매니페스트 스키마 (S3 Tables)

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `tar_key` | string | 파일이 담긴 tar의 S3 키 |
| `source_key` | string | 원본 파일 상대 경로 |
| `byte_start` / `byte_end` | long | tar 내 byte 범위 (Range GET용) |
| `md5` | string | 원본 파일 MD5 (`--md5` 시), 아니면 null |
| `filesize` | long | 원본 파일 크기 |
| `archive_ts` | timestamptz | 아카이빙 시각 (파티션: `day(archive_ts)`) |

---

## 4. 조회 및 복구 방법

### 4.1 조회 — Athena 또는 PyIceberg

```sql
SELECT tar_key, byte_start, byte_end, md5
FROM archive.manifest
WHERE source_key LIKE '%file0499%'
  AND archive_ts BETWEEN TIMESTAMP '2026-08-04' AND TIMESTAMP '2026-08-05';
```
> Athena 조회를 쓰려면 테이블 버킷의 AWS 분석 서비스(Glue Data Catalog) 통합이 활성화되어 있어야 합니다. PyIceberg로 직접 조회할 수도 있습니다(아래 4.2 참고).

### 4.2 복구 — byte-range GET으로 개별 파일 추출

```python
import boto3, io, tarfile
from pyiceberg.catalog import load_catalog

cat = load_catalog("s3tables", **{
    "type": "rest",
    "warehouse": "arn:aws:s3tables:ap-northeast-2:<account>:bucket/<table-bucket>",
    "uri": "https://s3tables.ap-northeast-2.amazonaws.com/iceberg",
    "rest.sigv4-enabled": "true", "rest.signing-name": "s3tables",
    "rest.signing-region": "ap-northeast-2"})
df = cat.load_table("archive.manifest").scan().to_pandas()
row = df[df.source_key.str.contains("file0499")].iloc[0]

s3 = boto3.client("s3")
resp = s3.get_object(Bucket="your-own-dest-bucket", Key=row.tar_key,
                     Range=f"bytes={int(row.byte_start)}-{int(row.byte_end)}")
tf = tarfile.open(fileobj=io.BytesIO(resp["Body"].read()))
data = tf.extractfile(tf.getnames()[0]).read()   # 개별 파일 내용
```

tar 전체를 내려받지 않고 해당 파일이 담긴 byte 범위만 가져옵니다. **단, tar가 Glacier Flexible Retrieval / Deep Archive에 있으면 먼저 tar 전체를 restore(수 시간)한 뒤에야 가능**합니다. 밀리초 즉시 복원이 필요하면 tar를 `GLACIER_IR`(기본값)에 두세요.

---

## 5. 성능 및 비용 비교 (테스트 환경 실측)

테스트 환경: EC2(Amazon Linux, 8 vCPU, ap-northeast-2), 소스 = NFS `/data/nfsshare`(파일당 평균 ~19KB), Python 3.12.1.

### 5.1 처리 성능

100만개(fs1+fs2, 19.49GB), 8 워커 기준:

| MD5 | 소요 | 처리율 | 실패 |
|---|---|---|---|
| off | 82.5s | **241.8 MB/s** | 0 |
| on | 89.0s | 224.3 MB/s | 0 |

### 5.2 MD5 on/off 비교 (동일 조건)

동일 데이터(100만개, 19.49GB) · 8 워커 · 멀티프로세스 · 연속 실행(파일 캐시 warm), **유일한 변수는 MD5**:

| | MD5 off | MD5 on | 차이 |
|---|---|---|---|
| 소요 시간 | 82.5s | 89.0s | **+7.9%** |
| 처리율 | 241.8 MB/s | 224.3 MB/s | -7.3% |

**MD5 오버헤드는 약 8%.** single-pass 해싱(어차피 tar용으로 읽는 김에 계산)이라 추가 읽기가 없고, 이 환경은 전송·I/O가 지배적이라 MD5 CPU 비용이 상대적으로 작습니다. 무결성 검증 이득 대비 저렴하므로, 복원 시 정합성 확인이 중요하면 **MD5 on을 권장**합니다.

### 5.3 오브젝트 통합 및 요청 절감

100만개 실행 기준:

| 방식 | 오브젝트 PUT | 통합비 |
|---|---|---|
| 개별 업로드 | 1,006,108건 | — |
| **번들링 (이 솔루션)** | **1,000건** (tar 979 + 매니페스트 커밋 21) | **약 1,028 : 1** |

→ PUT 요청 **약 1,000분의 1**. tar가 20MB이므로 GLACIER_IR 128KB 최소 과금 페널티도 원천 회피.

### 5.4 비용 (근사)

이 규모(원본 ~20GB, tar 979개, 매니페스트 100만행)의 월 저장 비용은 GLACIER_IR tar + S3 Tables 매니페스트를 합쳐 **매우 낮은 수준**(월 수십 센트 규모)입니다. 매니페스트를 S3 Tables에 둬도 **배치 커밋(50,000행/커밋)** 덕분에 작은 파일·스냅샷이 폭증하지 않아 비용이 번들링 이득을 상쇄하지 않습니다.

> 정확한 금액은 리전·기간별 요율로 별도 산정을 권합니다(위 수치는 근사).

### 5.5 정합성 검증

전체 tar 중 첫·중간·마지막 등 샘플에서 파일을 뽑아, S3 Tables 매니페스트의 offset으로 실제 tar를 byte-range 복원한 결과 **파일명·크기·(MD5 시)MD5 전부 일치**. single-pass MD5와 tar 물리 offset의 정확성을 실제 S3에서 확인했습니다.

---

## 제약 및 주의사항

- **`--compress`(gzip) 사용 시 byte-range 개별 복원 불가.** 압축하면 매니페스트 offset이 `-1`로 기록됩니다. 개별 파일 복원이 필요하면 압축을 끄세요(tar만).
- **개별 파일 즉시 복원이 필요하면 tar를 `GLACIER_IR`(기본값)에 두세요.** Glacier Flexible Retrieval / Deep Archive는 tar 전체 restore(수 시간)가 선행되어야 합니다.
- **Object Lock(WORM)이 필요하면 tar 저장 버킷에 버저닝 + Object Lock을 별도 설정하세요.** (S3 Tables 테이블 버킷은 Object Lock 미지원.)
- **처리율은 네트워크 대역폭에 좌우됩니다.** 온프레미스 → S3 전송 시에는 대역폭이 병목이 될 수 있습니다.
- **대량 처리 시 `--batch-size`를 크게 유지하세요** (기본 50,000). 작은 값은 매니페스트에 작은 파일이 많아져 조회 성능이 떨어질 수 있습니다.
- 테스트는 Python 3.12.1 / pyiceberg 0.11.1 기준입니다.
