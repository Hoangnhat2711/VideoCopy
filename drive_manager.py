import psutil
import sys
import subprocess
import os

def get_removable_drives():
    """Returns a list of removable drive partitions."""
    try:
        return [p for p in psutil.disk_partitions() if 'removable' in p.opts or 'media' in p.opts]
    except Exception as e:
        print("Could not get drive list: {}".format(e))
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
