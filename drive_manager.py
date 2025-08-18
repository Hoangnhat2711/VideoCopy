import psutil
import sys
import subprocess
import os

def get_removable_drives():
    """Returns a list of removable drive partitions, with special handling for macOS."""
    drives = []
    try:
        partitions = psutil.disk_partitions()
        for p in partitions:
            # Standard check for removable flag or media flag (common on Linux)
            is_removable = 'removable' in p.opts or 'media' in p.opts

            # macOS specific check: external drives typically mount under /Volumes
            is_macos_external = (sys.platform == "darwin" and p.mountpoint.startswith('/Volumes/'))

            if is_removable or is_macos_external:
                # Basic check to avoid including the root filesystem if it's not caught by other flags
                if p.mountpoint != '/':
                    drives.append(p)
        
        # Ensure the list is unique by device to avoid duplicates
        unique_drives = {drive.device: drive for drive in drives}.values()
        return list(unique_drives)

    except Exception as e:
        print(f"Could not get drive list: {e}")
        return []

def eject_drive(device_path):
    """
    Ejects a drive using system-specific commands.
    Returns a tuple (success: bool, message: str).
    """
    try:
        if sys.platform == "darwin":  # macOS
            subprocess.run(["diskutil", "eject", device_path], check=True, capture_output=True)
            return True, "Đã tháo an toàn thiết bị."
        elif sys.platform == "win32":  # Windows
            # This is a placeholder as it's more complex on Windows
            return False, "Chức năng tháo an toàn trên Windows hiện chưa được hỗ trợ."
        else:  # Linux
            subprocess.run(["udisksctl", "unmount", "-b", device_path], check=True, capture_output=True)
            subprocess.run(["udisksctl", "power-off", "-b", device_path], check=True, capture_output=True)
            return True, "Đã tháo an toàn thiết bị."
    except subprocess.CalledProcessError as e:
        error_message = e.stderr.decode().strip() if e.stderr else str(e)
        return False, "Lỗi khi tháo thẻ nhớ: {}".format(error_message)
    except Exception as e:
        return False, "Lỗi không xác định: {}".format(e)
