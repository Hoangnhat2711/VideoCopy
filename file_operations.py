import os
import shutil
import time
from datetime import datetime
import psutil

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
    """Copies a file using shutil.copy2 for reliability and reports progress."""
    total_size = os.path.getsize(source_path)
    if total_size == 0: # Handle zero-byte files
        shutil.copy2(source_path, dest_path)
        status_callback("processing", "Sao chép... (100%)", 1.0)
        return

    # shutil.copy2 is generally faster and more reliable
    # We'll run it in a separate thread to monitor progress, but for simplicity here,
    # we'll use a simplified progress simulation. For a real app, you might
    # use a more complex method or accept that shutil doesn't offer native progress.
    
    # Let's stick with the manual chunk copy for progress reporting, as it's already implemented.
    copied_size = 0
    with open(source_path, 'rb') as fsrc, open(dest_path, 'wb') as fdst:
        while True:
            buf = fsrc.read(1024 * 1024) # Read in 1MB chunks
            if not buf:
                break
            fdst.write(buf)
            copied_size += len(buf)
            percentage = copied_size / total_size
            status_callback("processing", f"Sao chép ({percentage:.0f}%)", percentage)
    
    # After copying, copy metadata
    shutil.copystat(source_path, dest_path)


def _handle_conflict(dest_path):
    """Generate a new file name to avoid conflict."""
    base, ext = os.path.splitext(dest_path)
    counter = 1
    new_dest_path = "{}-{}{}".format(base, counter, ext)
    while os.path.exists(new_dest_path):
        counter += 1
        new_dest_path = "{}-{}{}".format(base, counter, ext)
    return new_dest_path

def copy_verify_delete_file(source_path, destination_folder, should_delete, conflict_policy, status_callback):
    """
    Handles the entire process for a single file, using lightweight verification.
    """
    file_name = os.path.basename(source_path)
    dest_path = os.path.join(destination_folder, file_name)

    # --- Conflict Resolution ---
    if os.path.exists(dest_path):
        if conflict_policy == "Bỏ Qua":
            status_callback("success", "Bỏ qua (đã tồn tại)", 1.0) # Mark as complete
            return True, True # Skipped successfully
        elif conflict_policy == "Đổi Tên":
            dest_path = _handle_conflict(dest_path)
        # If policy is "Ghi Đè", we just proceed

    try:
        source_size = os.path.getsize(source_path)
        
        # 1. Copy with progress
        _copy_file_with_progress(source_path, dest_path, status_callback)
        
        # 2. Lightweight Verify
        status_callback("processing", "Đang kiểm tra...", None)
        dest_size = os.path.getsize(dest_path)
        
        if source_size != dest_size:
            raise Exception(f"Lỗi kích thước file (Gốc: {source_size}, Đích: {dest_size})")

        # 3. Delete
        if should_delete:
            status_callback("processing", "Đang xóa...", None)
            os.remove(source_path)
        
        status_callback("success", "Hoàn thành", 1.0) # Mark as complete
        return True, False # Processed successfully (not skipped)

    except Exception as e:
        error_message = "Lỗi: {}".format(e)
        status_callback("error", error_message, -1.0) # Mark as error
        print("Failed to process {}: {}".format(source_path, e))
        return False, False # Failed (not skipped)
