## Small Files Archiving Solution
This solution is consist of two scripts. The one is to combine many small files into big TARfile, and the other one is to search and retrieve specific file from TAR archived file.

This feature will help user reduce cloud storage cost reducing PUT request cost, and transfer to cloud faster than transferring original files.

## Pre-requsites
- python3 >= 3.7
- linux

## How to use
### Discover source directory and file structure
Image files would be stored in local storage based on directory structure “Line/Equipment/Lot/Date”. Especially dividing files by date order is effective to search specific object with index.

* Directory Structure Example: /mnt/1Line/1E/1L/2022/08/01

### Combining small file into big file
User can combine files into big file with small-file-archiver.py (https://github.com/aws-samples/small-files-archiving-solution/blob/main/small-file-archiver.py) script. It will scan the source directory and generate 10GB TAR file by default, or you can generate TAR files based on number of source files. Also it generate a manifest file per each TAR file, and  manifest file contains file name, file size, and TAR file name, date, md5 hash which can be used to validate after. Below is running script, and I will explain each parameter.

#### running script
```bash
python3 small-file-archiver.py --src_dir '/src/dataset/fs1' --combine count --max_file_number 500 --max_process 10 --nfs_dir '/dest'
```

- --src_dir: specify source directory to be archived
- --combind count|size: Tarfile can be created by file size or number of files. if you specify **--combine size** and **--max_tarfile_size $((10*(1024**3)))***, this program will create 10GB TARfile each. if you specify **--combind count** and **--max_file_number 500**, TARfile will contain 500 original files.
- --max_process: number of concurrent job. default is 10. It means 10 process will create TAR files in parallel
- --max_tarfile_size: It means tarfile size. $((10*1024**3)) means 10*(1024*1024*1024)=10GB
- --nfs_dir: specify destination directory to store TAR files.

#### Running script
```bash
[ec2-user@ip-172-31-42-60 ~]$ sh run.sh
starting script...2023-02-14 06:00:55.726736
/src/dataset/fs1 directory is archived
archive-20230214_060055-V7N3GE.tar is combining based on count
archive-20230214_060055-PJO3S1.tar is combining based on count
archive-20230214_060055-YLVSEY.tar is combining based on count
archive-20230214_060055-Z08HBY.tar is combining based on count
archive-20230214_060055-4U86Q3.tar is combining based on count
archive-20230214_060055-MJ0OVE.tar is combining based on count
archive-20230214_060055-GBE44W.tar is combining based on count
archive-20230214_060055-WTA0PS.tar is combining based on count
archive-20230214_060055-MMRGE4.tar is combining based on count
archive-20230214_060055-LHWY1P.tar is combining based on count
archive-20230214_060055-YLVSEY.tar is archived successfully
archive-20230214_060055-8V0VSU.tar is combining based on count
archive-20230214_060055-Z08HBY.tar is archived successfully
archive-20230214_060055-LUL0IK.tar is combining based on count
archive-20230214_060055-4U86Q3.tar is archived successfully
...
...
...
```

#### Result of script
```bash
archive-20230214_055044-VOXBIY.tar is archived successfully

archive-20230214_055044-T6O20X.tar is archived successfully

archive-20230214_055044-E6KD87.tar is archived successfully

archive-20230214_055044-MR3ZC8.tar is archived successfully

/src/dataset/fs1 directory is archived
====================================
Combine: count
size or count: 500
Duration: 0:00:10.501529
Scanned file numbers: 503006
TAR files location: /dest/2023/2/14
END
====================================
```

### Finding manifiles
Newly generated tarfiles and manifest files will be stored in directory which provided by **--dest** argument. There would be **list** directory in destination location. In there, manifest files are stored. Each TARfile will have its own manifest files and it contains Tarfile name, original file name, date, and MD5 hash.

- example of manifest
```bash
[ec2-user@ip-xx-xx-xx-xx ~]$ cat /dest/2023/2/14/list/archive-20230214_060416-Z3H2CK.tar-contents.csv

archive-20230214_060416-Z3H2CK.tar|/src/dataset/fs1/d0001/dir0020/file0006|2023|2|14|57437901f53a493d8438fe9ec9a5a9d7
archive-20230214_060416-Z3H2CK.tar|/src/dataset/fs1/d0001/dir0020/file0005|2023|2|14|fdd080b1f84880f816b3e7082ea6c2e4
archive-20230214_060416-Z3H2CK.tar|/src/dataset/fs1/d0001/dir0020/file0004|2023|2|14|28c5d5c27741c41b47d8dee8bc1644a7
archive-20230214_060416-Z3H2CK.tar|/src/dataset/fs1/d0001/dir0020/file0003|2023|2|14|92e01bf93c9f2f206c91c78b062dd536
archive-20230214_060416-Z3H2CK.tar|/src/dataset/fs1/d0001/dir0020/file0002|2023|2|14|7f2ed0fd7be9c34fc10e9d39d9540672
archive-20230214_060416-Z3H2CK.tar|/src/dataset/fs1/d0001/dir0020/file0001|2023|2|14|945af4498a5c3013bb5e3bbee96b6669
archive-20230214_060416-Z3H2CK.tar|/src/dataset/fs1/d0001/dir0020/index.html|2023|2|14|dd566061ad4b3de2f62fe4ec7901a092
archive-20230214_060416-Z3H2CK.tar|/src/dataset/fs1/d0001/dir0020/.htaccess|2023|2|14|598b53e6af094697d9ce4b66099affc5
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
