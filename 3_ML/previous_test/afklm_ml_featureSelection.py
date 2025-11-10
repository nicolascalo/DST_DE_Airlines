import os, re, datetime, logging, pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from concurrent.futures import ProcessPoolExecutor, as_completed
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, plot_tree
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.multiclass import OneVsOneClassifier, OneVsRestClassifier
from sklearn.metrics import classification_report, ConfusionMatrixDisplay, mean_absolute_error, mean_squared_error, r2_score
from sklearn.feature_selection import SelectKBest, f_classif
from xgboost import XGBRegressor

# ----------------------
# CONFIG
# ----------------------
RUN_MODE = 'both'          # 'simple', 'grid', 'both'
FILTER_EU = True           # Whether to filter EU airports
TOP30_EU = True            # Restrict to top30 EU airports
USE_FEATURE_SELECTION = True
TOP_K_FEATURES = 20
GRID_PARALLEL_JOBS = 10
GRID_LEVEL = 'quick'    # 'quick', 'moderate', 'heavy'

TEST_SIZE = 0.2
RANDOM_STATE = 42


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

# EU filtering
top30euAirports = ['CDG','AMS','ORY','FCO','LHR','CPH','MAD','ARN','OSL','LIN','NCE','BCN','LYS','BGO','LIS','DUB','HEL','TLS','OTP','FRA','MRS','ATH','PMI','MUC','TRD','MAN','BER','AGP','OPO']
euAirports = [...]  # full EU airport list

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
    categorical_transformer = Pipeline([('imputer', SimpleImputer(strategy='most_frequent')),('onehot', OneHotEncoder(handle_unknown='ignore', drop='first', sparse_output=True))])
    return ColumnTransformer(transformers=[('num', numerical_transformer, numeric_features),
                                           ('cat', categorical_transformer, categorical_features)])

def build_preprocessor_with_selection(X, target_col):
    preprocessor = build_preprocessor(X, target_col)
    if USE_FEATURE_SELECTION:
        selector = SelectKBest(score_func=f_classif, k=min(TOP_K_FEATURES, X.shape[1]))
        return Pipeline([('preprocessor', preprocessor), ('feature_selection', selector)])
    return preprocessor

# ----------------------
# FEATURE IMPORTANCE
# ----------------------
def save_feature_importance(pipeline, pipeline_name, output_dir=OUTPUT_DIR):
    try:
        clf = pipeline
        if hasattr(pipeline, "best_estimator_"): clf = pipeline.best_estimator_
        classifier = clf.named_steps["classifier"] if "classifier" in clf.named_steps else clf

        feature_names = []
        if "preprocessor" in clf.named_steps:
            preproc = clf.named_steps["preprocessor"]
            try:
                num_features = preproc.transformers_[0][2]
                cat_features = []
                if hasattr(preproc.transformers_[1][1], 'named_steps') and 'onehot' in preproc.transformers_[1][1].named_steps:
                    cat_features = preproc.transformers_[1][1].named_steps['onehot'].get_feature_names_out().tolist()
                feature_names = list(num_features) + cat_features
            except:
                feature_names = [f"f{i}" for i in range(clf.named_steps["preprocessor"].transform(X_train_cls).shape[1])]
        else:
            feature_names = [f"f{i}" for i in range(X_train_cls.shape[1])]

        importances = classifier.feature_importances_ if hasattr(classifier, "feature_importances_") else np.abs(classifier.coef_).flatten()
        df_imp = pd.DataFrame({'feature': feature_names, 'importance': importances})
        df_imp.sort_values(by='importance', ascending=False, inplace=True)
        df_imp.to_csv(f"{output_dir}/classification/feature_importance/{pipeline_name}_feature_importance.csv", index=False)

        plt.figure(figsize=(10,6))
        df_imp.head(20).plot(kind='bar', x='feature', y='importance', legend=False)
        plt.title(f"Top features for {pipeline_name}")
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(f"{output_dir}/classification/feature_importance/{pipeline_name}_feature_importance.png")
        plt.close()
    except Exception as e:
        logger.warning(f"Failed to extract feature importance for {pipeline_name}: {e}")

# ----------------------
# PIPELINE TESTING
# ----------------------
classification_summary = []
regression_summary = []

def test_pipeline(name, pipe, X_train, y_train, X_test, y_test, mode='simple'):
    logger.info(f"Running {name} mode={mode}")
    if mode=='grid' and pipe[1]:
        pipe_obj = GridSearchCV(pipe[0], pipe[1], cv=5, scoring='accuracy' if y_train.dtype=='O' else 'r2', verbose=2, n_jobs=GRID_PARALLEL_JOBS)
    else:
        pipe_obj = pipe[0]
    pipe_obj.fit(X_train, y_train)
    problem_type = 'classification' if y_train.dtype=='O' else 'regression'
    with open(f"{OUTPUT_DIR}/{problem_type}/{name}.pkl", 'wb') as f:
        pickle.dump(pipe_obj, f)
    return pipe_obj

def output_classification_metrics(name, pipe, X_test, y_test):
    y_pred = pipe.predict(X_test)
    report = classification_report(y_test, y_pred, output_dict=True)
    pd.DataFrame(report).transpose().to_csv(f"{OUTPUT_DIR}/classification/{name}_classification_report.csv")
    f,ax=plt.subplots(figsize=(10,10))
    ConfusionMatrixDisplay.from_predictions(y_test, y_pred, ax=ax)
    plt.savefig(f"{OUTPUT_DIR}/classification/confusion_matrix/{name}_confusion_matrix.png")
    save_feature_importance(pipe, name)

def output_regression_metrics(name, pipe, X_test, y_test):
    y_pred = pipe.predict(X_test)
    metrics = {'mae': mean_absolute_error(y_test, y_pred),
               'mse': mean_squared_error(y_test, y_pred),
               'rmse': np.sqrt(mean_squared_error(y_test, y_pred)),
               'r2': r2_score(y_test, y_pred)}
    pd.DataFrame([metrics]).to_csv(f"{OUTPUT_DIR}/regression/{name}_regression_report.csv", index=False)
    plt.figure(figsize=(10,6))
    plt.scatter(y_test, y_pred, alpha=0.5)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--')
    plt.xlabel("Actual"); plt.ylabel("Predicted"); plt.title(f"{name} Predictions vs Actual")
    plt.savefig(f"{OUTPUT_DIR}/regression/predictions/{name}_yTest_predicted.png")

# ----------------------
# PARALLEL EXECUTION
# ----------------------
def run_pipeline_parallel(name, pipe, X_train, y_train, X_test, y_test, problem_type, mode):
    result = {'pipeline': name, 'mode': mode}
    try:
        p = test_pipeline(name, pipe, X_train, y_train, X_test, y_test, mode=mode)
        if problem_type=='classification':
            output_classification_metrics(name, p, X_test, y_test)
            score = p.best_score_ if mode=='grid' else p.score(X_test, y_test)
            result['score'] = score
            if mode=='grid' and hasattr(p,'best_params_'): result['best_params'] = p.best_params_
            classification_summary.append(result)
        else:
            output_regression_metrics(name, p, X_test, y_test)
            r2 = r2_score(y_test, p.predict(X_test))
            result['score'] = r2
            if mode=='grid' and hasattr(p,'best_params_'): result['best_params'] = p.best_params_
            regression_summary.append(result)
        return f"{name} finished"
    except Exception as e:
        logger.error(f"{name} failed: {e}")
        result['score'] = np.nan
        return f"{name} failed"

def run_all_pipelines_parallel(pipelines, X_train, y_train, X_test, y_test, problem_type):
    futures = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        for name, pipe in pipelines.items():
            for mode in ['simple','grid']:
                if RUN_MODE in [mode,'both']:
                    futures.append(executor.submit(run_pipeline_parallel, f"{name}_{mode}", pipe,
                                                   X_train, y_train, X_test, y_test, problem_type, mode))
        for future in as_completed(futures):
            logger.info(future.result())

# ----------------------
# RUN CLASSIFICATION AND REGRESSION
# ----------------------
# Prepare classification pipelines
X_cls = df_past.drop(columns=['flightLegs_Category'])
y_cls = df_past['flightLegs_Category']
X_train_cls, X_test_cls, y_train_cls, y_test_cls = train_test_split(X_cls, y_cls, test_size=TEST_SIZE, stratify=y_cls, random_state=RANDOM_STATE)
preprocessor_cls = build_preprocessor_with_selection(X_cls, 'flightLegs_Category')

classification_pipelines = {
    'DecisionTree':[Pipeline([('preprocessor', preprocessor_cls),('classifier',DecisionTreeClassifier())]), {}],
    'RandomForest':[Pipeline([('preprocessor', preprocessor_cls),('classifier',RandomForestClassifier(n_jobs=GRID_PARALLEL_JOBS))]), {}],
    'Logistic_OVO':[Pipeline([('preprocessor', preprocessor_cls),('classifier',OneVsOneClassifier(LogisticRegression(max_iter=1000))) ]), {}],
    'Logistic_OVR':[Pipeline([('preprocessor', preprocessor_cls),('classifier',OneVsRestClassifier(LogisticRegression(max_iter=1000))) ]), {}]
}

logger.info("Running classification pipelines in parallel...")
run_all_pipelines_parallel(classification_pipelines, X_train_cls, y_train_cls, X_test_cls, y_test_cls, 'classification')

# Prepare regression pipelines
df_delay = df_past[df_past['flightLegs_Category']=='late'].dropna(subset=['flightLegs_irregularity_delayDuration_total'])
X_reg = df_delay.drop(columns=['flightLegs_irregularity_delayDuration_total'])
y_reg = df_delay['flightLegs_irregularity_delayDuration_total']
X_train_reg, X_test_reg, y_train_reg, y_test_reg = train_test_split(X_reg, y_reg, test_size=TEST_SIZE, random_state=RANDOM_STATE)
preprocessor_reg = build_preprocessor(X_reg, 'flightLegs_irregularity_delayDuration_total')

regression_pipelines = {
    'DecisionTreeRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',DecisionTreeRegressor())]), {}],
    'RandomForestRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',RandomForestRegressor(n_jobs=GRID_PARALLEL_JOBS))]), {}],
    'LinearRegression':[Pipeline([('preprocessor', preprocessor_reg),('classifier',LinearRegression(n_jobs=GRID_PARALLEL_JOBS))]), {}],
    'XGBRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',XGBRegressor(n_jobs=GRID_PARALLEL_JOBS))]), {}]
}

logger.info("Running regression pipelines in parallel...")
run_all_pipelines_parallel(regression_pipelines, X_train_reg, y_train_reg, X_test_reg, y_test_reg, 'regression')

# ----------------------
# SAVE FINAL SUMMARY CSVs
# ----------------------
pd.DataFrame(classification_summary).sort_values(by='score', ascending=False)\
    .to_csv(f'{OUTPUT_DIR}/classification/classification_summary.csv', index=False)
pd.DataFrame(regression_summary).sort_values(by='score', ascending=False)\
    .to_csv(f'{OUTPUT_DIR}/regression/regression_summary.csv', index=False)

logger.info("End of ML script")
