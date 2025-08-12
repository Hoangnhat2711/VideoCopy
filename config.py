import json
import os

CONFIG_FILE = "config.json"

# --- "Zen Focus" Color Palette ---
COLOR_BG = "#2b2b2b"
COLOR_FRAME = "#3c3f41"
COLOR_ACCENT_TEAL = "#4a7a8c"
COLOR_TEXT = "#a9b7c6"
COLOR_TEXT_HEADER = "#ffffff"
COLOR_STATUS_SUCCESS = "#6a8759"
COLOR_STATUS_ERROR = "#b96565"
COLOR_STATUS_WARN = "#c7926b"


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
    return {}
