import json
import os

CONFIG_FILE = "config.json"

# --- "Vibrant Focus" Color Palette ---
COLOR_BG = "#24292e"          # GitHub Dark Dimmed
COLOR_FRAME = "#2f363d"       # Lighter Grey
COLOR_EDITOR_BG = "#2B2B2B"   # A neutral dark gray for text areas
COLOR_ACCENT_SKYBLUE = "#58a6ff" # Sky Blue
COLOR_ACCENT_GREEN = "#3fb950"   # Vibrant Green
COLOR_ACCENT_ORANGE = "#f78166"  # Vibrant Orange
COLOR_TEXT = "#c9d1d9"        # Soft White/Grey
COLOR_TEXT_HEADER = "#ffffff" # Pure White for Headers
COLOR_DISABLED = "#555555"
    
# Status Colors
COLOR_STATUS_SUCCESS = "#3fb950"
COLOR_STATUS_ERROR = "#f85149"
COLOR_STATUS_WARN = "#d29922"

# --- Default Settings ---
DEFAULT_VIDEO_EXTENSIONS = ".mp4, .mov, .mkv, .avi"
DEFAULT_CONFLICT_POLICY = "Đổi Tên" # Options: "Bỏ Qua", "Ghi Đè", "Đổi Tên"

# --- Kafka Settings ---
KAFKA_ENABLED = False
DEFAULT_KAFKA_SERVERS = "localhost:9092"
DEFAULT_KAFKA_TOPIC = "video_copy_events"


def save_config(data):
    """Saves the given data dictionary to the config file."""
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(data, f, indent=4)
    except IOError as e:
        print("Error saving config: {}".format(e))


def load_config():
    """Loads the config file and returns it as a dictionary."""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            print("Error loading config: {}".format(e))
    # Return defaults if file doesn't exist or is empty
    return {
        "destination_path": "",
        "video_extensions": DEFAULT_VIDEO_EXTENSIONS,
        "conflict_policy": DEFAULT_CONFLICT_POLICY,
        "kafka_enabled": KAFKA_ENABLED,
        "kafka_servers": DEFAULT_KAFKA_SERVERS,
        "kafka_topic": DEFAULT_KAFKA_TOPIC
    }
