from sklearn.model_selection import train_test_split
from settings_ml import SETTINGS
from logger import get_logger

logger = get_logger()

def _limit_df(df):
    limit = SETTINGS.get('RECORD_LIMIT')
    try:
        if limit and str(limit).strip() != '':
            return df.head(int(limit))
    except Exception:
        pass
    return df

def prepare_all_datasets(df, settings):
    # Prepare status dataset
    df_status = _limit_df(df)[settings['columnKeywordsToKeep_classification_status'] + [settings['TARGET_CLASSIFICATION_STATUS']]]
    X_status = df_status.drop(columns=[settings['TARGET_CLASSIFICATION_STATUS']]).drop(columns=settings.get('columnsToDrop_classification_status', []), errors='ignore')
    y_status = df_status[settings['TARGET_CLASSIFICATION_STATUS']]

    X_train_s, X_test_s, y_train_s, y_test_s = train_test_split(X_status, y_status, test_size=settings['TEST_SIZE'], stratify=y_status, random_state=settings['RANDOM_STATE'])

    # Prepare delay classification (only LATE)
    df_delay = df[df[settings['TARGET_CLASSIFICATION_STATUS']] == 'LATE']
    df_delay_class = _limit_df(df_delay)[settings['columnKeywordsToKeep_classification_status'] + [settings['TARGET_CLASSIFICATION_DELAY']]]
    X_delay = df_delay_class.drop(columns=[settings['TARGET_CLASSIFICATION_DELAY']])
    y_delay = df_delay_class[settings['TARGET_CLASSIFICATION_DELAY']]
    X_train_d, X_test_d, y_train_d, y_test_d = train_test_split(X_delay, y_delay, test_size=settings['TEST_SIZE'], random_state=settings['RANDOM_STATE'])

    # Prepare regression (only LATE)
    df_reg = df[df[settings['TARGET_CLASSIFICATION_STATUS']] == 'LATE']
    df_reg_pre = _limit_df(df_reg)[settings['columnKeywordsToKeep_classification_status'] + [settings['TARGET_REGRESSION']]]
    X_reg = df_reg_pre.drop(columns=[settings['TARGET_REGRESSION']])
    y_reg = df_reg_pre[settings['TARGET_REGRESSION']]
    X_train_r, X_test_r, y_train_r, y_test_r = train_test_split(X_reg, y_reg, test_size=settings['TEST_SIZE'], random_state=settings['RANDOM_STATE'])

    return {
        'status': {'X_train': X_train_s, 'X_test': X_test_s, 'y_train': y_train_s, 'y_test': y_test_s},
        'delay': {'X_train': X_train_d, 'X_test': X_test_d, 'y_train': y_train_d, 'y_test': y_test_d},
        'regression': {'X_train': X_train_r, 'X_test': X_test_r, 'y_train': y_train_r, 'y_test': y_test_r}
    }
