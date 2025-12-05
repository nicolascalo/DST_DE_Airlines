from settings_ml import SETTINGS_ML
from logger import get_logger
from data_prep.loader import load_latest_csv, load_best_models
from data_prep.cleaning import clean_dataset
from data_prep.feature_engineering import add_time_features
from data_prep.splitting import prepare_all_datasets
from models.pipelines import build_pipelines
from models.training import run_all_pipelines
from utils.summarize import save_global_summary
from utils.file_utils import ensure_output_dirs

logger = get_logger()

def main():
    logger.info("Starting modularized ML pipeline")

    # Ensure output directories
    ensure_output_dirs(SETTINGS_ML)

    # Load
    df = load_latest_csv(SETTINGS_ML)
    best_model_scores = load_best_models(SETTINGS_ML)

    # Clean
    df_clean = clean_dataset(df, SETTINGS_ML)

    # Features
    df_feat = add_time_features(df_clean, SETTINGS_ML)

    # Prepare train/test splits for all problems
    datasets = prepare_all_datasets(df_feat, SETTINGS_ML)

    # Build pipelines (returns dicts for regression & classification)
    pipelines = build_pipelines(datasets, SETTINGS_ML)

    # Train & evaluate
    global_summary = run_all_pipelines(pipelines, datasets, SETTINGS_ML)

    # Save summary
    save_global_summary(global_summary, SETTINGS_ML)

    logger.info("Finished modularized ML pipeline")


if __name__ == '__main__':
    main()
