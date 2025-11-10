#!/usr/bin/env python3
import os, re, datetime, logging, pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.multiclass import OneVsOneClassifier, OneVsRestClassifier
from sklearn.metrics import classification_report, ConfusionMatrixDisplay, mean_absolute_error, mean_squared_error, r2_score
from sklearn.feature_selection import SelectKBest, f_classif
from xgboost import XGBRegressor

# ----------------------
# CONFIG
# ----------------------
RUN_MODE = 'both'          # 'simple','grid','both'
FILTER_EU = True
TOP30_EU = True
USE_FEATURE_SELECTION = True
TOP_K_FEATURES = 20
TOP_FEATURES_N = 10
GRID_LEVEL = 'moderate'    # 'quick','moderate','heavy'
GRID_PARALLEL_JOBS = 10
TEST_SIZE = 0.2
RANDOM_STATE = 42

# ----------------------
# OUTPUT DIRS
# ----------------------
OUTPUT_DIR = 'outputs'
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/classification/confusion_matrix', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/classification/feature_importance', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/classification/tree_plots', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/regression/predictions', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/regression/tree_plots', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/dataset_summary', exist_ok=True)

# ----------------------
# LOGGING
# ----------------------
logger = logging.getLogger(__name__)
handler = logging.FileHandler(f'{OUTPUT_DIR}/ML.log')
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.info("Starting ML script")

# ----------------------
# DATA LOAD
# ----------------------
cwd = os.getcwd()
csv_files = sorted([f for f in os.listdir(cwd) if 'afklm_flight_from_mongo_filtered' in f])
csv_to_import = csv_files[-1]
df = pd.read_csv(csv_to_import)
logger.info(f"Loaded dataset: {csv_to_import}, shape={df.shape}")

# ----------------------
# EU AIRPORTS
# ----------------------
top30euAirports = ['CDG','AMS','ORY','FCO','LHR','CPH','MAD','ARN','OSL','LIN','NCE','BCN','LYS','BGO','LIS','DUB','HEL','TLS','OTP','FRA','MRS','ATH','PMI','MUC','TRD','MAN','BER','AGP','OPO']
euAirports = [
    'ALC','AMS','ATH','BCN','BER','BGO','BRU','CDG','CPH','DUB','DUS','EDI','FCO','FRA','GDN','GVA','HEL','IST','KBP','LIS',
    'LHR','LIN','MAD','MAN','MUC','NCE','OPO','OSL','OTP','ORY','PMI','PRG','RIX','STR','TLS','TRD','VIE','WAW','ZRH'
]

# ----------------------
# DATA CLEANING
# ----------------------
summary_steps = []
def log_step(name, df):
    summary_steps.append({'step': name, 'shape': df.shape, 'null_values': df.isnull().sum().sum()})

log_step('raw_data', df)
df["company_flight"] = df["id"].apply(lambda x: re.sub("^.*?\+","", x))
keywordsToDrop_all = ['id','airline_name','flightLegs_aircraft_ownerAirlineCode','actual','PositionTerminal','latestPublished']
df_cleaned = df.dropna(subset=['flightStatusPublic']).drop(columns=df.filter(regex='|'.join(keywordsToDrop_all)), errors='ignore')
log_step('drop_na_and_unused_cols', df_cleaned)

df_cleaned["scheduled"] = df_cleaned["flightLegs_arrivalInformation_times_scheduled"].apply(
    lambda x: datetime.datetime.fromisoformat(x).date() > datetime.datetime.now().date()
)
df_cleaned["flightLegs_scheduledFlightDuration"] = df_cleaned.apply(
    lambda row: (datetime.datetime.fromisoformat(row.flightLegs_arrivalInformation_times_scheduled)
                 - datetime.datetime.fromisoformat(row.flightLegs_departureInformation_times_scheduled)).seconds / 60,
    axis=1
)

df_past = df_cleaned[~df_cleaned["scheduled"]].copy()
df_past["flightLegs_irregularity_delayDuration_total"] = df_past["flightLegs_irregularity_delayDuration_total"].fillna(0)

def categorize(row):
    if row['flightLegs_legStatusPublic']=='CANCELLED': return 'cancelled'
    return 'late' if row['flightLegs_irregularity_delayDuration_total']>0 else 'on_time'
df_past["flightLegs_Category"] = df_past.apply(categorize, axis=1)
log_step('categorize_flights', df_past)

if FILTER_EU:
    df_past = df_past[df_past['flightLegs_arrivalInformation_airport_code'].isin(euAirports) &
                      df_past['flightLegs_departureInformation_airport_code'].isin(euAirports)]
    if TOP30_EU:
        df_past = df_past[df_past['flightLegs_arrivalInformation_airport_code'].isin(top30euAirports) |
                          df_past['flightLegs_departureInformation_airport_code'].isin(top30euAirports)]
log_step('eu_filtering', df_past)
pd.DataFrame(summary_steps).to_csv(f'{OUTPUT_DIR}/dataset_summary/dataset_cleaning_summary.csv', index=False)

# ----------------------
# FEATURE PROCESSING
# ----------------------
def get_features(df, target_col):
    numeric_features = df.select_dtypes(include=['number']).columns.tolist()
    if target_col in numeric_features: numeric_features.remove(target_col)
    categorical_features = df.select_dtypes(include=['object','category']).columns.tolist()
    if target_col in categorical_features: categorical_features.remove(target_col)
    return numeric_features, categorical_features

def build_preprocessor(X, target_col):
    numeric_features, categorical_features = get_features(X, target_col)
    numerical_transformer = Pipeline([('imputer', SimpleImputer(strategy='median')),('scaler', StandardScaler())])
    categorical_transformer = Pipeline([('imputer', SimpleImputer(strategy='most_frequent')),
                                       ('onehot', OneHotEncoder(handle_unknown='ignore', drop='first', sparse_output=True))])
    preprocessor = ColumnTransformer(transformers=[('num', numerical_transformer, numeric_features),
                                                   ('cat', categorical_transformer, categorical_features)])
    if USE_FEATURE_SELECTION:
        selector = SelectKBest(score_func=f_classif, k=min(TOP_K_FEATURES, X.shape[1]))
        return Pipeline([('preprocessor', preprocessor), ('feature_selection', selector)])
    return preprocessor

# ----------------------
# GRID PARAMETERS
# ----------------------
def get_grid_params(name, problem_type):
    if problem_type=='classification':
        if name=='DecisionTree':
            if GRID_LEVEL=='quick': return {'classifier__max_depth':[2,3],'classifier__criterion':['gini','entropy']}
            elif GRID_LEVEL=='moderate': return {'classifier__max_depth':[2,3,4,5],'classifier__criterion':['gini','entropy'],'classifier__min_samples_split':[2,5]}
            else: return {'classifier__max_depth':[2,3,4,5,6,7],'classifier__criterion':['gini','entropy'],'classifier__min_samples_split':[2,5,10],'classifier__min_samples_leaf':[1,2,4]}
        if name=='RandomForest':
            if GRID_LEVEL=='quick': return {'classifier__n_estimators':[50,100]}
            elif GRID_LEVEL=='moderate': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[None,10,20]}
            else: return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[None,10,20,30],'classifier__min_samples_split':[2,5,10]}
    else:  # regression
        if name=='DecisionTreeRegressor':
            if GRID_LEVEL=='quick': return {'classifier__max_depth':[3,5,7]}
            elif GRID_LEVEL=='moderate': return {'classifier__max_depth':[2,3,4,5,6,7,8]}
            else: return {'classifier__max_depth':[2,3,4,5,6,7,8,9,10],'classifier__min_samples_split':[2,5,10],'classifier__min_samples_leaf':[1,2,4]}
        if name=='RandomForestRegressor':
            if GRID_LEVEL=='quick': return {'classifier__n_estimators':[50,100]}
            elif GRID_LEVEL=='moderate': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[None,10,20]}
            else: return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[None,10,20,30],'classifier__min_samples_split':[2,5,10]}
        if name=='XGBRegressor':
            if GRID_LEVEL=='quick': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[3,5]}
            elif GRID_LEVEL=='moderate': return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[3,5,7],'classifier__learning_rate':[0.01,0.1]}
            else: return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[3,5,7,10],'classifier__learning_rate':[0.001,0.01,0.1],'classifier__subsample':[0.5,0.7,1]}
        if name=='LinearRegression':
            return {}  # no parameters for linear regression
    return {}

# ----------------------
# FEATURE IMPORTANCE
# ----------------------
top_features_summary = []

def save_feature_importance(pipeline, pipeline_name, problem_type):
    try:
        clf = pipeline.best_estimator_ if hasattr(pipeline,'best_estimator_') else pipeline
        step_name = 'classifier' if 'classifier' in clf.named_steps else list(clf.named_steps.keys())[-1]
        model = clf.named_steps.get(step_name, clf)
        feature_names = []
        if 'preprocessor' in clf.named_steps:
            preproc = clf.named_steps['preprocessor']
            try:
                num_features = preproc.transformers_[0][2]
                cat_features = []
                if hasattr(preproc.transformers_[1][1], 'named_steps') and 'onehot' in preproc.transformers_[1][1].named_steps:
                    cat_features = preproc.transformers_[1][1].named_steps['onehot'].get_feature_names_out().tolist()
                feature_names = list(num_features)+cat_features
            except:
                feature_names = [f"f{i}" for i in range(preproc.transform(X_train_cls if problem_type=='classification' else X_train_reg).shape[1])]
        else:
            feature_names = [f"f{i}" for i in range(X_train_cls.shape[1])]
        importances = getattr(model,'feature_importances_',None)
        if importances is None and hasattr(model,'coef_'):
            importances = np.abs(model.coef_).flatten()
        if importances is None: return
        df_imp = pd.DataFrame({'feature':feature_names,'importance':importances})
        df_imp.sort_values('importance',ascending=False,inplace=True)
        df_imp.to_csv(f"{OUTPUT_DIR}/{problem_type}/feature_importance/{pipeline_name}_feature_importance.csv", index=False)
        df_imp.head(TOP_FEATURES_N).plot(kind='bar', x='feature', y='importance', legend=False, figsize=(10,6))
        plt.title(f"Top features for {pipeline_name}")
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/{problem_type}/feature_importance/{pipeline_name}_feature_importance.png")
        plt.close()
        # top features summary
        top_df = df_imp.head(TOP_FEATURES_N).copy()
        top_df['pipeline']=pipeline_name
        top_df['problem_type']=problem_type
        top_features_summary.append(top_df)
    except Exception as e:
        logger.warning(f"Failed feature importance for {pipeline_name}: {e}")

# ----------------------
# PIPELINE TEST FUNCTIONS
# ----------------------
def test_pipeline_classification(name, pipeline_tuple, mode='simple'):
    pipe, params = pipeline_tuple
    if mode=='grid' and params:
        gs = GridSearchCV(pipe, params, cv=5, scoring='accuracy', n_jobs=GRID_PARALLEL_JOBS)
        gs.fit(X_train_cls, y_train_cls)
        save_feature_importance(gs, name, 'classification')
        return gs
    else:
        pipe.fit(X_train_cls, y_train_cls)
        save_feature_importance(pipe, name, 'classification')
        return pipe

def output_metrics_classification(name, pipe):
    y_pred = pipe.predict(X_test_cls)
    report = classification_report(y_test_cls, y_pred, output_dict=True)
    pd.DataFrame(report).transpose().to_csv(f"{OUTPUT_DIR}/classification/{name}_classification_report.csv")
    f,ax=plt.subplots(figsize=(10,10))
    ConfusionMatrixDisplay.from_predictions(y_test_cls, y_pred, ax=ax)
    plt.savefig(f"{OUTPUT_DIR}/classification/confusion_matrix/{name}_confusion_matrix.png")
    plt.close()

def test_pipeline_regression(name, pipeline_tuple, mode='simple'):
    pipe, params = pipeline_tuple
    if mode=='grid' and params:
        gs = GridSearchCV(pipe, params, cv=5, scoring='r2', n_jobs=GRID_PARALLEL_JOBS)
        gs.fit(X_train_reg, y_train_reg)
        save_feature_importance(gs, name, 'regression')
        return gs
    else:
        pipe.fit(X_train_reg, y_train_reg)
        save_feature_importance(pipe, name, 'regression')
        return pipe

def output_metrics_regression(name, pipe):
    y_pred = pipe.predict(X_test_reg)
    metrics = {'mae': mean_absolute_error(y_test_reg,y_pred),
               'mse': mean_squared_error(y_test_reg,y_pred),
               'rmse': np.sqrt(mean_squared_error(y_test_reg,y_pred)),
               'r2': r2_score(y_test_reg,y_pred)}
    pd.DataFrame([metrics]).to_csv(f"{OUTPUT_DIR}/regression/{name}_regression_report.csv", index=False)
    plt.figure(figsize=(10,6))
    plt.scatter(y_test_reg, y_pred, alpha=0.5)
    plt.plot([y_test_reg.min(), y_test_reg.max()], [y_test_reg.min(), y_test_reg.max()], 'k--')
    plt.xlabel("Actual"); plt.ylabel("Predicted"); plt.title(f"{name} Predictions vs Actual")
    plt.savefig(f"{OUTPUT_DIR}/regression/predictions/{name}_yTest_predicted.png")
    plt.close()

# ----------------------
# PREPARE DATA & PIPELINES
# ----------------------
# Classification
X_cls = df_past.drop(columns=['flightLegs_Category'])
y_cls = df_past['flightLegs_Category']
X_train_cls, X_test_cls, y_train_cls, y_test_cls = train_test_split(X_cls, y_cls, test_size=TEST_SIZE, stratify=y_cls, random_state=RANDOM_STATE)
preprocessor_cls = build_preprocessor(X_cls,'flightLegs_Category')

classification_pipelines = {
    'DecisionTree':[Pipeline([('preprocessor', preprocessor_cls),('classifier',DecisionTreeClassifier())]), get_grid_params('DecisionTree','classification')],
    'RandomForest':[Pipeline([('preprocessor', preprocessor_cls),('classifier',RandomForestClassifier(n_jobs=GRID_PARALLEL_JOBS))]), get_grid_params('RandomForest','classification')],
    'Logistic_OVO':[Pipeline([('preprocessor', preprocessor_cls),('classifier',OneVsOneClassifier(LogisticRegression(max_iter=1000))) ]), {}],
    'Logistic_OVR':[Pipeline([('preprocessor', preprocessor_cls),('classifier',OneVsRestClassifier(LogisticRegression(max_iter=1000))) ]), {}]
}

# Regression
df_delay = df_past[df_past['flightLegs_Category']=='late'].dropna(subset=['flightLegs_irregularity_delayDuration_total'])
X_reg = df_delay.drop(columns=['flightLegs_irregularity_delayDuration_total'])
y_reg = df_delay['flightLegs_irregularity_delayDuration_total']
X_train_reg, X_test_reg, y_train_reg, y_test_reg = train_test_split(X_reg, y_reg, test_size=TEST_SIZE, random_state=RANDOM_STATE)
preprocessor_reg = build_preprocessor(X_reg,'flightLegs_irregularity_delayDuration_total')

regression_pipelines = {
    'LinearRegression':[Pipeline([('preprocessor', preprocessor_reg),('classifier',LinearRegression())]), get_grid_params('LinearRegression','regression')],
    'DecisionTreeRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',DecisionTreeRegressor())]), get_grid_params('DecisionTreeRegressor','regression')],
    'RandomForestRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',RandomForestRegressor(n_jobs=GRID_PARALLEL_JOBS))]), get_grid_params('RandomForestRegressor','regression')],
    'XGBRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',XGBRegressor(n_jobs=GRID_PARALLEL_JOBS))]), get_grid_params('XGBRegressor','regression')]
}

# ----------------------
# EXECUTION
# ----------------------
for name, pipeline_tuple in classification_pipelines.items():
    if RUN_MODE in ['simple','both']:
        pipe = test_pipeline_classification(name,pipeline_tuple,mode='simple')
        output_metrics_classification(name,pipe)
    if RUN_MODE in ['grid','both']:
        pipe = test_pipeline_classification(name,pipeline_tuple,mode='grid')
        output_metrics_classification(name,pipe)

for name, pipeline_tuple in regression_pipelines.items():
    if RUN_MODE in ['simple','both']:
        pipe = test_pipeline_regression(name,pipeline_tuple,mode='simple')
        output_metrics_regression(name,pipe)
    if RUN_MODE in ['grid','both']:
        pipe = test_pipeline_regression(name,pipeline_tuple,mode='grid')
        output_metrics_regression(name,pipe)

# ----------------------
# SAVE TOP FEATURES SUMMARY
# ----------------------
if top_features_summary:
    pd.concat(top_features_summary).to_csv(f"{OUTPUT_DIR}/top_features_summary.csv", index=False)
    logger.info(f"Saved top {TOP_FEATURES_N} features summary for all pipelines")

logger.info("End of ML script")
