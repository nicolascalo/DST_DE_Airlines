from sklearn.model_selection import train_test_split
from config.config_ml import SETTINGS_ML
from src.utils.logger import get_logger





def _limit_df(df, dataset):



    limit = SETTINGS_ML.get('RECORD_LIMIT')
    
    try:
        if limit and str(limit).strip() != '':
            logger.info(f"{dataset} limiting number of records to {SETTINGS_ML['RECORD_LIMIT']}")
            return df.head(int(limit))
        else:
            logger.info(f"{dataset} Using the full dataset")
    except Exception:
        pass
    return df




    


def prepare_all_datasets(df, SETTINGS_ML):
    logger = get_logger()

    
    logger.info(f"================================== Dataset splitting and feature selection ==================================")


    dict_datasets = {}

    for target in ['TARGET_CLASSIFICATION_STATUS','TARGET_CLASSIFICATION_DELAY','TARGET_REGRESSION']:
        df_ = df.copy()


        if target in ['TARGET_CLASSIFICATION_DELAY','TARGET_REGRESSION']:
            logger.info(f"{dataset} Removing non-LATE flights")
            df_ = df_[df_[SETTINGS_ML['TARGET_CLASSIFICATION_STATUS']] == 'LATE']

        # Keep relevant columns
        dataset = target.replace("TARGET_","")
        col_ini = set(df_.columns)

        cols_to_keep =  SETTINGS_ML['columnKeywordsToKeep_classification_status'] + [SETTINGS_ML[target]] 
        cols_to_drop =        SETTINGS_ML.get('columnsToDrop_classification_status', [])


        df_ = _limit_df(df_, dataset)[cols_to_keep].drop(columns=cols_to_drop, errors='ignore')


        X = df_.drop(columns=SETTINGS_ML[target], errors='ignore')

        col_target = SETTINGS_ML[target]
        col_features = set(X.columns) - set(col_target)
        col_dropped = col_ini - col_features - set(col_target)

        logger.info(f"{dataset} columns dropped: {col_dropped}")
        logger.info(f"{dataset} features: {col_features}")
        logger.info(f"{dataset} target: {col_target}")

        y = df_[SETTINGS_ML[target]]

        if dataset == "REGRESSION":
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=SETTINGS_ML['TEST_SIZE'], random_state=SETTINGS_ML['RANDOM_STATE'])
        else:
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=SETTINGS_ML['TEST_SIZE'], stratify=y, random_state=SETTINGS_ML['RANDOM_STATE'])


            logger.info(f"{dataset} dataset class repartition: {y.value_counts(normalize = True)}")

        logger.info(f"{dataset} train/test split: X_train={X_train.shape}, X_test={X_test.shape}, "
                f"y_train={y_train.shape}, y_test={y_test.shape}")
        

        dict_datasets.update({
            dataset.lower(): {'X_train': X_train, 'X_test': X_test, 'y_train': y_train, 'y_test': y_test}
        })
    return dict_datasets



