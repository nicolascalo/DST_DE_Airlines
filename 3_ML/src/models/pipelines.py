from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.multiclass import OneVsOneClassifier, OneVsRestClassifier
from xgboost import XGBRegressor
from src.models.grid_params import get_grid_params
from src.utils.preprocessing_utils import get_numeric_categorical
from src.utils.logger import get_logger


def build_preprocessor(X, target_col):
    numeric, categorical = get_numeric_categorical(X, target_col)
    num_pipe = Pipeline([('imputer', SimpleImputer(strategy='median')), ('scaler', StandardScaler())])
    cat_pipe = Pipeline([('imputer', SimpleImputer(strategy='most_frequent')), ('onehot', OneHotEncoder(handle_unknown='ignore', drop='first', sparse_output=True))])
    preproc = ColumnTransformer([('num', num_pipe, numeric), ('cat', cat_pipe, categorical)])
    return preproc

def build_pipelines(datasets, settings):
    logger = get_logger()

    logger.info(f"================================== Pipelines building ==================================")


    # regression
    preproc_reg = build_preprocessor(datasets['regression']['X_train'], settings['TARGET_REGRESSION'])
    regression_pipelines = {
        'LinearRegression': [Pipeline([('preprocessor', preproc_reg), ('classifier', LinearRegression())]), get_grid_params('LinearRegression', 'regression')],
        'DecisionTreeRegressor': [Pipeline([('preprocessor', preproc_reg), ('classifier', DecisionTreeRegressor())]), get_grid_params('DecisionTreeRegressor', 'regression')],
        'RandomForestRegressor': [Pipeline([('preprocessor', preproc_reg), ('classifier', RandomForestRegressor(n_jobs=settings['PARALLEL_JOBS']))]), get_grid_params('RandomForestRegressor', 'regression')],
        'XGBRegressor': [Pipeline([('preprocessor', preproc_reg), ('classifier', XGBRegressor(n_jobs=settings['PARALLEL_JOBS']))]), get_grid_params('XGBRegressor', 'regression')]




    }

    # classification (status)
    preproc_cls = build_preprocessor(datasets['classification_status']['X_train'], settings['TARGET_CLASSIFICATION_STATUS'])
    classification_status_pipelines = {
        'DecisionTree': [Pipeline([('preprocessor', preproc_cls), ('classifier', DecisionTreeClassifier())]), get_grid_params('DecisionTree', 'classification_status')],
        'RandomForest': [Pipeline([('preprocessor', preproc_cls), ('classifier', RandomForestClassifier(n_jobs=settings['PARALLEL_JOBS']))]), get_grid_params('RandomForest', 'classification_status')],
        'Logistic_OVO': [Pipeline([('preprocessor', preproc_cls), ('classifier', OneVsOneClassifier(LogisticRegression(max_iter=1000), n_jobs=settings['PARALLEL_JOBS']))]), get_grid_params('Logistic_OVO', 'classification_status')],
        'Logistic_OVR': [Pipeline([('preprocessor', preproc_cls), ('classifier', OneVsRestClassifier(LogisticRegression(max_iter=1000), n_jobs=settings['PARALLEL_JOBS']))]), get_grid_params('Logistic_OVR', 'classification_status')]
    }

    # classification (delay)
    preproc_cls = build_preprocessor(datasets['classification_delay']['X_train'], settings['TARGET_CLASSIFICATION_STATUS'])
    classification_delay_pipelines = {
        'DecisionTree': [Pipeline([('preprocessor', preproc_cls), ('classifier', DecisionTreeClassifier())]), get_grid_params('DecisionTree', 'classification_status')],
        'RandomForest': [Pipeline([('preprocessor', preproc_cls), ('classifier', RandomForestClassifier(n_jobs=settings['PARALLEL_JOBS']))]), get_grid_params('RandomForest', 'classification_status')],
        'Logistic_OVO': [Pipeline([('preprocessor', preproc_cls), ('classifier', OneVsOneClassifier(LogisticRegression(max_iter=1000), n_jobs=settings['PARALLEL_JOBS']))]), get_grid_params('Logistic_OVO', 'classification_status')],
        'Logistic_OVR': [Pipeline([('preprocessor', preproc_cls), ('classifier', OneVsRestClassifier(LogisticRegression(max_iter=1000), n_jobs=settings['PARALLEL_JOBS']))]), get_grid_params('Logistic_OVR', 'classification_status')]
    }

    # Keep only models requested
    regression_pipelines = {k:v for k,v in regression_pipelines.items() if k in settings['MODEL_LIST_TO_TEST']}
    classification_status_pipelines = {k:v for k,v in classification_status_pipelines.items() if k in settings['MODEL_LIST_TO_TEST']}
    classification_delay_pipelines = {k:v for k,v in classification_delay_pipelines.items() if k in settings['MODEL_LIST_TO_TEST']}

    return {'regression': regression_pipelines, 'classification_status': classification_status_pipelines, 'classification_delay': classification_delay_pipelines}
