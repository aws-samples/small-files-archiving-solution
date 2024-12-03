import os       
import tarfile  
from datetime import datetime
import io       
import concurrent.futures
import logging  
import hashlib  
import threading
                
DELIMITER = '|'
        
class FS2FSArchiver:
    def __init__(self, args):
        self.args = args
        self.logger = logging.getLogger(__name__)
        self.current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.tar_sequence = 1
        self.tar_sequence_lock = threading.Lock()
        self.total_files = 0
        self.failed_files = 0
        self.total_tar_files = 0
        self.total_manifest_files = 0
        self.src_prefix = args.src_path
        self.dst_prefix = args.dst_path
            
    def get_file_list(self):
        if self.args.input_file:
            return self._get_file_list_from_input_file()
        else:       
            return self._get_file_list_from_directory()
                    
    def _get_file_list_from_input_file(self):
        file_list = []
        with open(self.args.input_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split(',')
                    if len(parts) >= 2:
                        full_path, obj_name = parts[:2]
                    else:
                        full_path = parts[0]
                        obj_name = os.path.basename(full_path)
                    
                    if os.path.exists(full_path):
                        file_size = os.path.getsize(full_path)
                        file_list.append((full_path, obj_name, file_size))
                    else:
                        self.logger.warning(f"File not found: {full_path}")
        return file_list

    def _get_file_list_from_directory(self):
        file_list = []
        for root, _, files in os.walk(self.src_prefix):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, self.src_prefix)
                file_size = os.path.getsize(full_path)
                file_list.append((full_path, rel_path, file_size))
        return file_list

    @staticmethod
    def md5hash(filename):
        md5_hash = hashlib.md5()
        with open(filename, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()

    def create_tar_and_save(self, file_list):
        with self.tar_sequence_lock:
            tar_name = f"archive_{self.current_time}_{self.tar_sequence:08d}.tar"
            manifest_name = f"manifest_{self.current_time}_{self.tar_sequence:08d}.csv"
            self.tar_sequence += 1
    
        tar_path = os.path.join(self.dst_prefix, 'archives', tar_name)
        manifest_path = os.path.join(self.dst_prefix, 'manifests', manifest_name)

        tar_buffer = io.BytesIO()
        manifest_content = io.StringIO()
        
        os.makedirs(os.path.dirname(tar_path), exist_ok=True)
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    
        with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
            for file_name, obj_name, file_size in file_list:
                tar_cur_pos = tar_buffer.tell()
                tar.add(file_name, arcname=obj_name)
                tar_end_pos = tar_buffer.tell() - 1 
                md5 = self.md5hash(file_name)
                content_log = f"{tar_name}{DELIMITER}{file_name}{DELIMITER}"
                content_log += f"{datetime.now().strftime('%Y|%m|%d')}{DELIMITER}"
                content_log += f"{file_size}{DELIMITER}{tar_cur_pos}{DELIMITER}"
                content_log += f"{tar_end_pos}{DELIMITER}{md5}\n"
                manifest_content.write(content_log)
    
        self.total_files += len(file_list)
        self.total_tar_files += 1
        self.total_manifest_files += 1

        # Save tarfile
        with open(tar_path, 'wb') as f:
            f.write(tar_buffer.getvalue())

        # Save manifest file
        with open(manifest_path, 'w') as f:
            f.write(manifest_content.getvalue())
    
        return f"Created {tar_name} and {manifest_name}"

    def run(self):
        file_list = self.get_file_list()
        current_tar_files = []
        current_tar_size = 0 

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.max_process) as executor:
            future_to_tar = {}

            for file_info in file_list:
                _, _, file_size = file_info

                if self.args.combine == 'count' and len(current_tar_files) >= self.args.max_file_number:
                    future_to_tar[executor.submit(self.create_tar_and_save, current_tar_files)] = len(current_tar_files)
                    current_tar_files = []
                    current_tar_size = 0 
                elif self.args.combine == 'size' and current_tar_size + file_size > self.args.max_tarfile_size:
                    future_to_tar[executor.submit(self.create_tar_and_save, current_tar_files)] = len(current_tar_files)
                    current_tar_files = []
                    current_tar_size = 0 

                current_tar_files.append(file_info)
                current_tar_size += file_size

            if current_tar_files:
                future_to_tar[executor.submit(self.create_tar_and_save, current_tar_files)] = len(current_tar_files)

            for future in concurrent.futures.as_completed(future_to_tar):
                num_files = future_to_tar[future]
                try:
                    result = future.result()
                    self.logger.info(result)
                except Exception as exc:
                    self.logger.error(f"Error processing {num_files} files: {exc}")
                    self.failed_files += num_files

        self.logger.info(f"Total files processed: {self.total_files}")
        self.logger.info(f"Failed files: {self.failed_files}")
        self.logger.info(f"Total tar files created: {self.total_tar_files}")
        self.logger.info(f"Total manifest files created: {self.total_manifest_files}")
