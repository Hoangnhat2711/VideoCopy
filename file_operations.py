import os
import shutil
import hashlib
from datetime import datetime

def _calculate_sha256(file_path):
    """Calculates the SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(8192), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except IOError as e:
        print("Error reading file for hashing: {}".format(e))
        return None

def find_videos_on_drive(drive_path):
    """Scans a drive and yields information about video files found."""
    video_extensions = {".mp4", ".avi", ".mov", ".mkv"}
    for root, _, files in os.walk(drive_path):
        for file in files:
            if os.path.splitext(file)[1].lower() in video_extensions:
                full_path = os.path.join(root, file)
                try:
                    file_stat = os.stat(full_path)
                    file_size = "{:.2f} MB".format(file_stat.st_size / (1024*1024))
                    mod_time = datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                    yield file, file_size, mod_time, full_path
                except Exception as e:
                    print("Could not stat file {}: {}".format(full_path, e))
                    continue

def copy_verify_delete_file(source_path, destination_folder, should_delete, status_callback):
    """
    Handles the entire process for a single file: copy, verify, and optionally delete.
    Invokes the status_callback with progress updates.
    """
    file_name = os.path.basename(source_path)
    dest_path = os.path.join(destination_folder, file_name)

    try:
        # 1. Copy
        status_callback("copying", "Đang sao chép...")
        shutil.copy2(source_path, dest_path)
        
        # 2. Verify
        status_callback("verifying", "Đang xác minh...")
        source_hash = _calculate_sha256(source_path)
        dest_hash = _calculate_sha256(dest_path)
        if not (source_hash and dest_hash and source_hash == dest_hash):
            raise Exception("Checksum không khớp")

        # 3. Delete
        if should_delete:
            status_callback("deleting", "Đang xóa...")
            os.remove(source_path)
        
        status_callback("success", "Hoàn thành")
        return True

    except Exception as e:
        error_message = "Lỗi: {}".format(e)
        status_callback("error", error_message)
        print("Failed to process {}: {}".format(source_path, e))
        return False
