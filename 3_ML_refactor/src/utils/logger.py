import logging
from config.config_ml import SETTINGS_ML
import os

def get_logger():
    logger = logging.getLogger('afklm_ml')
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    out_dir = os.path.join(SETTINGS_ML['OUTPUT_DIR'], "training_runs",SETTINGS_ML['RUN_TIMESTAMP'])
    os.makedirs(out_dir, exist_ok=True)
    fh = logging.FileHandler(os.path.join(out_dir, f"{SETTINGS_ML['RUN_TIMESTAMP']}_ML.log"))
    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger
