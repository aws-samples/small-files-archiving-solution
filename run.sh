# run by count
#python3 small-file-archiver-0.2.8.py --src_dir '/src/fac0/2022/10/04/' --combine count --max_process 10 --max_file_number 10000 --nfs_dir '/dest'

python3 small-file-archiver-0.2.9.py --src_dir '/src/fac0/2022/10/04/' --combine count --max_process 10  --max_file_number 500 --nfs_dir '/dest'

# run by size
#python3 small-file-archiver-0.2.8.py --src_dir '/src/fac0/2022/10/04/' --combine size --max_process 10 --max_tarfile_size $((10*(1024**3))) --nfs_dir '/dest'
