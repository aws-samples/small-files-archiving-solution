# Spec for small file archiving solution
date: 2024.12.23

- source can be filesystem or s3 bucket.
- billions files on directories would be aggregated into tarfiles based on tarfile size or count of source files.
- Adapt producer and consumer model to create tarfile and upload it as soon as filelist is generated.
- consumer should support parallel processing.
- manifest file should be generated per tarfile.
- manifest file should contain md5 value of each file.
- manifest file contains {tarfile name, source file name, year, month, day, file size, start offset, end offset, md5}
- When source is s3 bucket, at least 256 concurrent downloading should be supported for performance.
- After job completed, result report should be shown in standard output and log file.
- every stdout log saves in log file. 
- log file will be saved in logs directory in local system and destination
- tarfiles and manifest files will be stored in "archives" directory and "manifests" directory respectively on destination path.
- tarfile should be generated in memory before sending to S3
- result report will show below items
    - start time:
    - end time:
    - duration
    - total transferred files:
    - total transferred size:
    - failed files:
    - Total tar files:
    - Total manifest files:
    - Source path
    - Destination path
    - combine method: size|count
    - max processes:
    - max archive size:

- producer should send file list to queue as soon as file size or file count reaches '--max-files' or '--max-count' variable, and then archiving process starts while producer still generates file list.
  - I'll help you modify the code to implement a more efficient producer-consumer pattern where the producer sends batches as soon as they meet the criteria, allowing parallel processing with the consumers
