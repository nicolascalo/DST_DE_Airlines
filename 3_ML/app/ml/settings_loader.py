import json
import shutil
from pathlib import Path

CONFIG_PATH = Path("./config")
CURRENT_SETTINGS_FILE = CONFIG_PATH / "afklm_ml_training_settings.json"
DEFAULT_SETTINGS_FILE = CONFIG_PATH / "afklm_ml_training_settings_default.json"


def load_training_settings() -> dict:
    """
    Load current training settings from JSON.
    Falls back to defaults if file is missing.
    """
    if CURRENT_SETTINGS_FILE.exists():
        with open(CURRENT_SETTINGS_FILE, "r", encoding="utf-8") as f:
            settings = json.load(f)
    else:
        # fallback to default
        with open(DEFAULT_SETTINGS_FILE, "r", encoding="utf-8") as f:
            settings = json.load(f)
        shutil.copy(DEFAULT_SETTINGS_FILE, CURRENT_SETTINGS_FILE)

    return settings
