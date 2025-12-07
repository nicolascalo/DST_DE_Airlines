from config.config_ml import SETTINGS_ML
from src.utils.logger import get_logger
from src.preprocessing.loader import load_latest_csv, load_best_models
from src.preprocessing.cleaning import clean_dataset
from src.preprocessing.feature_engineering import add_features
from src.preprocessing.target_engineering import engineer_classification_targets
from src.preprocessing.splitting import prepare_all_datasets
from src.models.pipelines import build_pipelines
from src.models.training import run_all_pipelines
from src.utils.summarize import save_global_summary_and_clean_models
from src.utils.file_utils import ensure_output_dirs




def run_training_pipeline():
    logger = get_logger()


    logger.info("Starting ML pipeline")

    ensure_output_dirs(SETTINGS_ML)

    df = load_latest_csv(SETTINGS_ML)
    best_model_scores = load_best_models(SETTINGS_ML)

    df_clean = clean_dataset(df, SETTINGS_ML)
    df_feat = add_features(df_clean, SETTINGS_ML)
    df_target = engineer_classification_targets(df_feat, SETTINGS_ML)

    datasets = prepare_all_datasets(df_target, SETTINGS_ML)
    pipelines = build_pipelines(datasets, SETTINGS_ML)

    global_summary = run_all_pipelines(
        pipelines, datasets, SETTINGS_ML, best_model_scores
    )

    save_global_summary_and_clean_models(
        global_summary, SETTINGS_ML, best_model_scores
    )

    logger.info("Finished ML pipeline")

    return global_summary
