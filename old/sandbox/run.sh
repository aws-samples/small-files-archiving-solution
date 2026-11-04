# run by count
python3 small-file-archiver-0.2.10.py --src_dir '/src/dataset/fs1' --combine count --max_file_number 500 --max_process 10 --nfs_dir '/dest'
# run by size
#python3 small-file-archiver-0.2.10.py --src_dir '/src/fac0/2022/10/04/' --combine size --max_tarfile_size $((10*(1024**3))) --max_process 10 --nfs_dir '/dest'
