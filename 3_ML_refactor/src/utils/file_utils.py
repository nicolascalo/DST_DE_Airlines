import os
from config.config_ml import SETTINGS_ML

def ensure_output_dirs(settings):
    base = settings['OUTPUT_DIR']
    runs = "training_runs"
    ts = settings['RUN_TIMESTAMP']
    paths = [
        base,
        os.path.join(base, runs , ts),
        os.path.join(base, 'best_models'),
        os.path.join(base, runs , ts, 'classification_status', 'confusion_matrix'),
        os.path.join(base, runs , ts, 'classification_status', 'feature_importance'),
        os.path.join(base, runs , ts, 'classification_delay', 'confusion_matrix'),
        os.path.join(base, runs , ts, 'classification_delay', 'feature_importance'),
        os.path.join(base, runs , ts, 'regression', 'feature_importance')
    ]
    for p in paths:
        os.makedirs(p, exist_ok=True)
