from sklearn.tree import plot_tree
import matplotlib.pyplot as plt
from logger import get_logger
import os
from copy import deepcopy

logger = get_logger()

def save_decision_tree_plot(pipeline, pipeline_name, problem_type, settings):
    try:
        clf = pipeline.best_estimator_ if hasattr(pipeline, 'best_estimator_') else pipeline
        model = clf.named_steps.get('classifier', clf)
        if not hasattr(model, 'tree_'):
            return

        feature_names = None
        if 'preprocessor' in clf.named_steps:
            preproc = clf.named_steps['preprocessor']
            try:
                feature_names = preproc.get_feature_names_out()
            except Exception:
                feature_names = [f'f{i}' for i in range(model.n_features_in_)]

        model_for_plot = deepcopy(model)
        plt.figure(figsize=(20,10))
        plot_tree(model_for_plot, filled=True, feature_names=feature_names, max_depth=3)
        out_dir = os.path.join(settings['OUTPUT_DIR'], settings['RUN_TIMESTAMP'], problem_type, 'tree_plots')
        os.makedirs(out_dir, exist_ok=True)
        plt.savefig(os.path.join(out_dir, f"{settings['RUN_TIMESTAMP']}_{pipeline_name}_tree.png"), dpi=200)
        plt.close()
    except Exception as e:
        logger.warning('Failed tree plot %s: %s', pipeline_name, e)
