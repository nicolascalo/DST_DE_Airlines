import os
from settings_ml import SETTINGS

def ensure_output_dirs(settings):
    base = settings['OUTPUT_DIR']
    ts = settings['RUN_TIMESTAMP']
    paths = [
        base,
        os.path.join(base, ts),
        os.path.join(base, 'best_models'),
        os.path.join(base, ts, 'classification_status', 'confusion_matrix'),
        os.path.join(base, ts, 'classification_status', 'feature_importance'),
        os.path.join(base, ts, 'classification_status', 'tree_plots'),
        os.path.join(base, ts, 'classification_delay', 'confusion_matrix'),
        os.path.join(base, ts, 'classification_delay', 'feature_importance'),
        os.path.join(base, ts, 'classification_delay', 'tree_plots'),
        os.path.join(base, ts, 'dataset_summary'),
        os.path.join(base, ts, 'regression', 'feature_importance'),
        os.path.join(base, ts, 'regression', 'predictions'),
        os.path.join(base, ts, 'regression', 'models'),
        os.path.join(base, ts, 'regression', 'tree_plots')
    ]
    for p in paths:
        os.makedirs(p, exist_ok=True)
