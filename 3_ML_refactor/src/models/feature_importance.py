import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from src.utils.logger import get_logger
import os

logger = get_logger()

def save_feature_importance(pipeline, pipeline_name, problem_type, settings):
    try:
        clf = pipeline.best_estimator_ if hasattr(pipeline, 'best_estimator_') else pipeline
        model = clf.named_steps.get('classifier', clf)

        # feature names safe extraction
        feature_names = []
        if 'preprocessor' in clf.named_steps:
            preproc = clf.named_steps['preprocessor']
            try:
                feature_names = list(preproc.get_feature_names_out())
            except Exception:
                feature_names = [f'f{i}' for i in range(getattr(model, 'n_features_in_', 0))]

        importances = getattr(model, 'feature_importances_', None)
        if importances is None and hasattr(model, 'coef_'):
            importances = np.abs(model.coef_).flatten()
        if importances is None:
            logger.warning('No feature importance for %s', pipeline_name)
            return

        df_imp = pd.DataFrame({'feature': feature_names, 'importance': importances})
        df_imp.sort_values('importance', ascending=False, inplace=True)

        out_dir = os.path.join(settings['OUTPUT_DIR'], 'training_runs', settings['RUN_TIMESTAMP'], problem_type, 'feature_importance')
        os.makedirs(out_dir, exist_ok=True)
        csv_path = os.path.join(out_dir, f"{settings['RUN_TIMESTAMP']}_{pipeline_name}_feature_importance.csv")
        df_imp.to_csv(csv_path, index=False)

        # plot
        df_imp.head(settings['TOP_K_FEATURES']).plot(kind='bar', x='feature', y='importance', legend=False, figsize=(10,6))
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f"{settings['RUN_TIMESTAMP']}_{pipeline_name}_feature_importance.png"))
        plt.close()
    except Exception as e:
        logger.warning('Failed saving feature importance for %s: %s', pipeline_name, e)
