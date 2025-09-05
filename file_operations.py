import os
import shutil
import time
from datetime import datetime
import psutil
import hashlib

def get_required_space(file_paths):
    """Calculate the total disk space required for a list of files."""
    return sum(os.path.getsize(p) for p in file_paths if os.path.exists(p))

def has_enough_space(destination_path, required_space):
    """Check if the destination has enough free space."""
    free_space = psutil.disk_usage(destination_path).free
    return free_space >= required_space

def find_files_on_drive(drive_path, extensions_str):
    """Scans a drive for files with specified extensions and yields their data."""
    try:
        extensions = {ext.strip().lower() for ext in extensions_str.split(',')}
    except Exception as e:
        print(f"Could not parse extensions string: {extensions_str}. Error: {e}")
        extensions = set()
    
    for root, _, files in os.walk(drive_path):
        for file in files:
            # Ignore macOS metadata files
            if file.startswith('._'):
                continue
            if os.path.splitext(file)[1].lower() in extensions:
                full_path = os.path.join(root, file)
                try:
                    file_stat = os.stat(full_path)
                    yield {
                        'path': full_path,
                        'size': file_stat.st_size,
                        'drive': drive_path
                    }
                except Exception as e:
                    print(f"Could not stat file {full_path}: {e}")
                    continue

def _copy_file_with_progress(source_path, dest_path, status_callback):
    """Copies a file and reports progress, including real-time speed."""
    total_size = os.path.getsize(source_path)
    if total_size == 0: # Handle zero-byte files
        shutil.copy2(source_path, dest_path)
        status_callback("processing", "Sao chép... (100%)", 1.0, 0.0)
        return

    copied_size = 0
    
    # Variables for speed calculation
    start_time = time.time()
    last_update_time = start_time
    bytes_since_last_update = 0

    with open(source_path, 'rb') as fsrc, open(dest_path, 'wb') as fdst:
        while True:
            buf = fsrc.read(1024 * 1024) # Read in 1MB chunks
            if not buf:
                break
            fdst.write(buf)
            
            chunk_size = len(buf)
            copied_size += chunk_size
            bytes_since_last_update += chunk_size
            percentage = copied_size / total_size
            
            current_time = time.time()
            elapsed_since_last_update = current_time - last_update_time

            # Update speed roughly every half a second to avoid flooding the UI thread
            if elapsed_since_last_update > 0.5:
                speed_mbps = (bytes_since_last_update / (1024*1024)) / elapsed_since_last_update
                status_callback("processing", f"Sao chép ({percentage:.0f}%)", percentage, speed_mbps)
                last_update_time = current_time
                bytes_since_last_update = 0
            
    # Final metadata copy
    shutil.copystat(source_path, dest_path)

def _calculate_checksum(file_path):
    """Calculates the SHA256 checksum for a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def _handle_conflict(dest_path):
    """Generate a new file name to avoid conflict."""
    base, ext = os.path.splitext(dest_path)
    counter = 1
    new_dest_path = "{}-{}{}".format(base, counter, ext)
    while os.path.exists(new_dest_path):
        counter += 1
        new_dest_path = "{}-{}{}".format(base, counter, ext)
    return new_dest_path

def copy_and_verify_file(source_path, destination_folder, conflict_policy, status_callback):
    """
    Handles the copy and verification process for a single file.
    Returns a tuple of (success, skipped, final_destination_path).
    """
    file_name = os.path.basename(source_path)
    dest_path = os.path.join(destination_folder, file_name)

    # --- Conflict Resolution ---
    skipped = False
    if os.path.exists(dest_path):
        if conflict_policy == "Bỏ Qua":
            status_callback("success", "Bỏ qua (đã tồn tại)", 1.0, 0.0) # Mark as complete
            return True, True, dest_path # Skipped successfully
        elif conflict_policy == "Đổi Tên":
            dest_path = _handle_conflict(dest_path)
        # If policy is "Ghi Đè", we just proceed

    try:
        source_size = os.path.getsize(source_path)
        
        # 1. Copy with progress
        _copy_file_with_progress(source_path, dest_path, status_callback)
        
        # 2. Lightweight Verify (Size)
        status_callback("processing", "Đang kiểm tra (kích thước)...", None, None)
        dest_size = os.path.getsize(dest_path)
        
        if source_size != dest_size:
            raise Exception(f"Lỗi kích thước file (Gốc: {source_size}, Đích: {dest_size})")

        # 3. Robust Verify (Checksum)
        status_callback("processing", "Đang kiểm tra (checksum)...", None, None)
        source_checksum = _calculate_checksum(source_path)
        dest_checksum = _calculate_checksum(dest_path)

        if source_checksum != dest_checksum:
            raise Exception("Lỗi checksum không khớp, dữ liệu có thể đã bị lỗi.")

        # Deletion logic is now handled separately at the drive level.
        
        status_callback("success", "Hoàn thành & Đã xác thực", 1.0, 0.0) # Mark as complete
        return True, skipped, dest_path # Processed successfully (not skipped)

    except Exception as e:
        error_message = "Lỗi: {}".format(e)
        status_callback("error", error_message, -1.0, None) # Mark as error
        print("Failed to process {}: {}".format(source_path, e))
        return False, skipped, None # Failed (not skipped)

def wipe_drive_data(drive_path, status_callback):
    """
    Deletes all files and then all empty directories on a given drive path,
    skipping known protected system files and directories.
    """
    status_callback("processing", "Chuẩn bị xóa thẻ...", None)
    files_deleted = 0
    dirs_deleted = 0
    errors = []

    # Define protected directories that should not be touched
    protected_dirs = {
        ".Spotlight-V100", ".fseventsd", ".Trashes",
        ".DocumentRevisions-V100", ".TemporaryItems",
        "System Volume Information", "$RECYCLE.BIN"
    }

    # Step 1: Delete all files, walking from the top down.
    status_callback("processing", "Đang xóa các tệp...", None)
    try:
        for root, dirs, files in os.walk(drive_path, topdown=True):
            # Don't recurse into protected directories
            dirs[:] = [d for d in dirs if d not in protected_dirs]

            for name in files:
                if name.startswith('._') or name == '.DS_Store':
                    continue
                file_path = os.path.join(root, name)
                try:
                    os.remove(file_path)
                    files_deleted += 1
                except OSError as e:
                    if e.errno != 2: # errno 2 is "No such file or directory"
                        err_msg = f"Không thể xóa tệp {file_path}: {e}"
                        errors.append(err_msg)
    except Exception as e:
        errors.append(f"Lỗi khi xóa tệp: {e}")

    # Step 2: Delete all empty directories, walking from the bottom up.
    status_callback("processing", "Đang dọn dẹp thư mục...", None)
    try:
        # We walk 'topdown=False' to process from the deepest directories first
        for root, dirs, files in os.walk(drive_path, topdown=False):
            # Don't try to delete the protected directories themselves
            dirs[:] = [d for d in dirs if d not in protected_dirs]
            
            for name in dirs:
                dir_path = os.path.join(root, name)
                try:
                    os.rmdir(dir_path)
                    dirs_deleted += 1
                except OSError:
                    # This might fail if the directory is not empty due to a file
                    # deletion error above, which is acceptable to ignore.
                    pass
    except Exception as e:
         errors.append(f"Lỗi khi xóa thư mục: {e}")

    if errors:
        final_message = f"Hoàn tất xóa với {len(errors)} lỗi."
        status_callback("error", final_message, None)
        print(f"Errors wiping drive {drive_path}:\n" + "\n".join(errors))
        return False, final_message
    else:
        final_message = f"Đã xóa {files_deleted} tệp và {dirs_deleted} thư mục."
        status_callback("success", "Đã xóa sạch dữ liệu trên thẻ.", None)
        return True, final_message
