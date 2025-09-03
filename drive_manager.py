import psutil
import sys
import subprocess
import os
import json # For parsing lsblk output


def find_and_mount_unmounted_drives():
    """
    On Linux, finds removable partitions that have a filesystem but are not mounted,
    and attempts to mount them using udisksctl.
    This is a no-op on other operating systems.
    """
    if not sys.platform.startswith("linux"):
        return

    try:
        # Use lsblk to get a reliable list of block devices, including removability status.
        result = subprocess.run(
            ["lsblk", "-J", "-o", "NAME,RM,FSTYPE,MOUNTPOINT"],
            capture_output=True, text=True, check=True
        )
        data = json.loads(result.stdout)
        
        partitions_to_mount = []
        # lsblk JSON output is a tree. We need to find removable parent devices (e.g., sdb)
        # and then check their children partitions (e.g., sdb1).
        for device in data.get("blockdevices", []):
            is_disk_removable = device.get("rm", False)
            if is_disk_removable and "children" in device:
                for partition in device["children"]:
                    has_fs = partition.get("fstype") not in [None, "swap", ""]
                    is_mounted = partition.get("mountpoint") not in [None, ""]
                    if has_fs and not is_mounted:
                        # lsblk `name` for partitions is just e.g. `sdb1`. Need to prepend /dev/
                        full_device_path = f"/dev/{partition['name']}"
                        partitions_to_mount.append(full_device_path)

        # Now, attempt to mount the collected partitions
        for device_path in partitions_to_mount:
            try:
                print(f"Attempting to auto-mount {device_path}...")
                # We use udisksctl, the standard way for user apps to mount devices.
                # It handles permissions via polkit, often not requiring a password.
                subprocess.run(
                    ["udisksctl", "mount", "--block-device", device_path, "--no-user-interaction"],
                    check=True, capture_output=True, timeout=10
                )
                print(f"Successfully mounted {device_path}.")
            except subprocess.CalledProcessError as e:
                error_output = e.stderr.decode('utf-8', errors='ignore').strip()
                print(f"Could not auto-mount {device_path}: {error_output}")
            except subprocess.TimeoutExpired:
                print(f"Timeout trying to mount {device_path}.")
            except FileNotFoundError:
                print("Could not auto-mount: 'udisksctl' command not found.")
                break # Stop trying if the command doesn't exist.

    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as e:
        # This can happen if lsblk is not installed or returns an error.
        print(f"Could not check for unmounted drives: {e}")


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

            # Linux specific check: removable drives often mount under /media, /mnt, or /run/media
            is_linux_external = (sys.platform.startswith("linux") and 
                                 (p.mountpoint.startswith('/media/') or 
                                  p.mountpoint.startswith('/mnt/') or
                                  p.mountpoint.startswith('/run/media/')))

            if is_removable or is_macos_external or is_linux_external:
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
            drive_letter = device_path.replace("\\", "")
            ps_command = f"(New-Object -comObject Shell.Application).Namespace(17).ParseName('{drive_letter}').InvokeVerb('Eject')"
            subprocess.run(["powershell", "-WindowStyle", "Hidden", "-Command", ps_command], check=True, capture_output=True)
            return True, "Đã tháo an toàn thiết bị."
        else:  # Linux
            subprocess.run(["udisksctl", "unmount", "-b", device_path], check=True, capture_output=True)
            subprocess.run(["udisksctl", "power-off", "-b", device_path], check=True, capture_output=True)
            return True, "Đã tháo an toàn thiết bị."
    except subprocess.CalledProcessError as e:
        error_message = e.stderr.decode('utf-8', errors='ignore').strip() if e.stderr else str(e)
        if not error_message: # PowerShell sometimes writes errors to stdout
             error_message = e.stdout.decode('utf-8', errors='ignore').strip()
        return False, "Lỗi khi tháo thẻ nhớ: {}".format(error_message)
    except Exception as e:
        return False, "Lỗi không xác định: {}".format(e)
