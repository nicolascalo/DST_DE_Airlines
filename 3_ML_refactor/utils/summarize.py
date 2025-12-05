import pandas as pd
import os
from settings_ml import SETTINGS

def save_global_summary(global_summary, settings):
    df = pd.DataFrame(global_summary)
    out_dir = os.path.join(settings['OUTPUT_DIR'], settings['RUN_TIMESTAMP'])
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(os.path.join(out_dir, f"{settings['RUN_TIMESTAMP']}_global_ml_summary.csv"), index=False)
