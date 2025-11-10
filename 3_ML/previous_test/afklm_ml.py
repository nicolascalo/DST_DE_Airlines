import os, re, datetime, logging, pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, plot_tree
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.multiclass import OneVsOneClassifier, OneVsRestClassifier
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay, mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

# ----------------------
# CONFIG
# ----------------------
RUN_MODE = 'both'  # options: 'simple', 'grid', 'both'
FILTER_EU = True   # whether to filter to EU airports
TOP30_EU = True    # whether to further restrict to top 30 EU airports
GRID_PARALLEL_JOBS = 10

OUTPUT_DIR = 'outputs'
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/classification', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/classification/confusion_matrix', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/classification/feature_importance', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/regression', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/regression/predictions', exist_ok=True)
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
file_list = os.listdir(cwd)
csv_to_import = sorted([f for f in file_list if 'afklm_flight_from_mongo_filtered' in f])[-1]
df = pd.read_csv(csv_to_import)

# ----------------------
# DATA CLEANING
# ----------------------
summary_steps = []

def log_step(name, df):
    summary_steps.append({
        'step': name,
        'shape': df.shape,
        'null_values': df.isnull().sum().sum()
    })

log_step('raw_data', df)

# Extract company
df["company_flight"] = df["id"].apply(lambda x: re.sub("^.*?\+","", x))

# Drop unnecessary columns
keywordsToDrop_all = ['id','airline_name','flightLegs_aircraft_ownerAirlineCode','actual','PositionTerminal','latestPublished']
df_cleaned = df.dropna(subset=['flightStatusPublic']).drop(columns=df.filter(regex='|'.join(keywordsToDrop_all)), errors='ignore')
log_step('drop_na_and_unused_cols', df_cleaned)

# Scheduled flag and flight duration
df_cleaned["scheduled"] = df_cleaned["flightLegs_arrivalInformation_times_scheduled"].apply(
    lambda x: datetime.datetime.fromisoformat(x).date() > datetime.datetime.now().date()
)
df_cleaned["flightLegs_scheduledFlightDuration"] = df_cleaned.apply(
    lambda row: (datetime.datetime.fromisoformat(row.flightLegs_arrivalInformation_times_scheduled)
                 - datetime.datetime.fromisoformat(row.flightLegs_departureInformation_times_scheduled)).seconds / 60,
    axis=1
)

# Past flights
df_past = df_cleaned[~df_cleaned["scheduled"]].copy()
df_past["flightLegs_irregularity_delayDuration_total"] = df_past["flightLegs_irregularity_delayDuration_total"].fillna(0)

# Flight category
def categorize(row):
    if row['flightLegs_legStatusPublic']=='CANCELLED': return 'cancelled'
    return 'late' if row['flightLegs_irregularity_delayDuration_total']>0 else 'on_time'

df_past["flightLegs_Category"] = df_past.apply(categorize, axis=1)
log_step('categorize_flights', df_past)

# Optional EU filtering
top30euAirports = ['CDG','AMS','ORY','FCO','LHR','CPH','MAD','ARN','OSL','LIN','NCE','BCN','LYS','BGO','LIS','DUB','HEL','TLS','OTP','FRA','MRS','ATH','PMI','MUC','TRD','MAN','BER','AGP','OPO']
euAirports = [...] # full EU list as before

if FILTER_EU:
    df_past = df_past[df_past['flightLegs_arrivalInformation_airport_code'].isin(euAirports) &
                      df_past['flightLegs_departureInformation_airport_code'].isin(euAirports)]
    if TOP30_EU:
        df_past = df_past[df_past['flightLegs_arrivalInformation_airport_code'].isin(top30euAirports) |
                          df_past['flightLegs_departureInformation_airport_code'].isin(top30euAirports)]
log_step('eu_filtering', df_past)

# Save dataset cleaning summary
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

# ----------------------
# UTILITY FUNCTIONS
# ----------------------
def test_pipeline(name, pipe, X_train, y_train, X_test, y_test, mode='simple'):
    logger.info(f"Running {name} mode={mode}")
    if mode=='grid' and pipe[1]:
        pipe = GridSearchCV(pipe[0], pipe[1], cv=5, scoring='accuracy' if y_train.dtype=='O' else 'r2', verbose=2, n_jobs=GRID_PARALLEL_JOBS)
    else:
        pipe = pipe[0]
    pipe.fit(X_train, y_train)
    with open(f"{OUTPUT_DIR}/{'classification' if y_train.dtype=='O' else 'regression'}/{name}.pkl", 'wb') as f:
        pickle.dump(pipe, f)
    return pipe

def output_classification_metrics(name, pipe, X_test, y_test):
    y_pred = pipe.predict(X_test)
    report = classification_report(y_test, y_pred, output_dict=True)
    pd.DataFrame(report).transpose().to_csv(f"{OUTPUT_DIR}/classification/{name}_classification_report.csv")
    f,ax=plt.subplots(figsize=(10,10))
    ConfusionMatrixDisplay.from_predictions(y_test, y_pred, ax=ax)
    plt.savefig(f"{OUTPUT_DIR}/classification/confusion_matrix/{name}_confusion_matrix.png")

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
# PIPELINES
# ----------------------
X_cls = df_past.drop(columns=['flightLegs_Category'])
y_cls = df_past['flightLegs_Category']
X_train_cls, X_test_cls, y_train_cls, y_test_cls = train_test_split(X_cls, y_cls, test_size=0.2, stratify=y_cls, random_state=42)
preprocessor_cls = build_preprocessor(X_cls, 'flightLegs_Category')

classification_pipelines = {
    'DecisionTree':[Pipeline([('preprocessor', preprocessor_cls),('classifier',DecisionTreeClassifier(max_depth=4))]), {}],
    'RandomForest':[Pipeline([('preprocessor', preprocessor_cls),('classifier',RandomForestClassifier(n_jobs=GRID_PARALLEL_JOBS))]),
                    {'classifier__n_estimators':[50,100],'classifier__max_depth':[None,10,20],'classifier__min_samples_split':[2,5]}]
}

df_delay = df_past[df_past['flightLegs_Category']=='late'].dropna(subset=['flightLegs_irregularity_delayDuration_total'])
X_reg = df_delay.drop(columns=['flightLegs_irregularity_delayDuration_total'])
y_reg = df_delay['flightLegs_irregularity_delayDuration_total']
X_train_reg, X_test_reg, y_train_reg, y_test_reg = train_test_split(X_reg, y_reg, test_size=0.2, random_state=42)
preprocessor_reg = build_preprocessor(X_reg, 'flightLegs_irregularity_delayDuration_total')

regression_pipelines = {
    'DecisionTreeRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',DecisionTreeRegressor(max_depth=8))]), {}],
    'RandomForestRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',RandomForestRegressor(n_jobs=GRID_PARALLEL_JOBS))]),
                              {'classifier__n_estimators':[50,100],'classifier__max_depth':[None,10,20],'classifier__min_samples_split':[2,5]}],
    'XGBRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',XGBRegressor(n_estimators=100,n_jobs=GRID_PARALLEL_JOBS))]),
                     {'classifier__max_depth':[3,5,7],'classifier__learning_rate':[0.1,0.01],'classifier__subsample':[0.5,0.7,1]}]
}

# ----------------------
# RUN PIPELINES WITH SUMMARY
# ----------------------
summary_classification, summary_regression = [], []

def add_summary(name, pipe, X_test, y_test, problem_type, mode):
    best_params = pipe.best_params_ if hasattr(pipe,'best_params_') else {}
    if problem_type=='classification':
        score = pipe.score(X_test, y_test)
        summary_classification.append({'pipeline':name,'mode':mode,'best_params':best_params,'accuracy':score})
    else:
        y_pred = pipe.predict(X_test)
        summary_regression.append({'pipeline':name,'mode':mode,'best_params':best_params,
                                   'mae': mean_absolute_error(y_test, y_pred),
                                   'mse': mean_squared_error(y_test, y_pred),
                                   'rmse': np.sqrt(mean_squared_error(y_test, y_pred)),
                                   'r2': r2_score(y_test, y_pred)})

def run_all_with_summary(pipelines, X_train, y_train, X_test, y_test, problem_type):
    for name, pipe in pipelines.items():
        for mode in ['simple','grid']:
            if RUN_MODE in [mode,'both']:
                p = test_pipeline(f"{name}_{mode}", pipe, X_train, y_train, X_test, y_test, mode=mode)
                if problem_type=='classification':
                    output_classification_metrics(f"{name}_{mode}", p, X_test, y_test)
                else:
                    output_regression_metrics(f"{name}_{mode}", p, X_test, y_test)
                add_summary(f"{name}_{mode}", p, X_test, y_test, problem_type, mode)

logger.info("Running classification pipelines...")
run_all_with_summary(classification_pipelines, X_train_cls, y_train_cls, X_test_cls, y_test_cls, 'classification')

logger.info("Running regression pipelines...")
run_all_with_summary(regression_pipelines, X_train_reg, y_train_reg, X_test_reg, y_test_reg, 'regression')

# Save summary
pd.DataFrame(summary_classification).to_csv(f"{OUTPUT_DIR}/classification/classification_summary.csv", index=False)
pd.DataFrame(summary_regression).to_csv(f"{OUTPUT_DIR}/regression/regression_summary.csv", index=False)

logger.info("End of ML script")
