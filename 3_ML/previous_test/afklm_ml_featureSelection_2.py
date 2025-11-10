#!/usr/bin/env python3
import os, re, datetime, logging, pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from concurrent.futures import ProcessPoolExecutor, as_completed
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, plot_tree
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.multiclass import OneVsOneClassifier, OneVsRestClassifier
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.metrics import classification_report, ConfusionMatrixDisplay, mean_absolute_error, mean_squared_error, r2_score
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
GRID_LEVEL = 'moderate'    # 'quick', 'moderate', 'heavy'

TEST_SIZE = 0.2
RANDOM_STATE = 42

OUTPUT_DIR = 'outputs'
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/classification/confusion_matrix', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/classification/feature_importance', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/classification/tree_plots', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/regression/predictions', exist_ok=True)
os.makedirs(f'{OUTPUT_DIR}/regression/feature_importance', exist_ok=True)
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
logger.info(f"Starting ML script with RUN_MODE={RUN_MODE}, FILTER_EU={FILTER_EU}, TOP30_EU={TOP30_EU}, GRID_LEVEL={GRID_LEVEL}")

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

euAirports = ['BHX','BOH','BRS','EXT','HUY','LBA','LPL','LGW','LHR','LCY','SEN','STN','LTN','MAN','MME','NCL','NQY','NWI','EMA','SOU','BFS','BHD','LDY','ABZ','EDI','GLA','PIK','INV','CWL','ANR','BRU','CRL','LGG','OST','AJA','BIA','BVA','EGC','BZR','BIQ','BOD','BES','CCF','XCR','CMF','DNR','FSC','GNB','LRH','LIL','LIG','LYS','MRS','BSL','NTE','NCE','FNI','CDG','ORY','PUF','PGF','PIS','RDZ','EBU','SXB','TLN','TLS','TUF','GIB','ORK','DUB','KIR','NOC','SNN','IOM','JER','LUX','AMS','EIN','GRQ','MST','RTM','GRZ','KLU','INN','LNZ','SZG','VIE','BRQ','JCL','KLV','OSR','PED','PRG','FKB','BER','BRE','CGN','DTM','DUS','FRA','HHN','FDH','HAM','HAJ','LEJ','LBC','FMM','MUC','NUE','STR','NRN','BUD','DEB','SOB','BZG','GDN','KTW','KRK','LUZ','LCJ','SZY','POZ','RZE','SZZ','WAW','WMI','RDO','WRO','BTS','KSC','PZY','TAT','ILZ','BSL','BRN','GVA','LUG','ACH','ZRH','BWK','DBV','LSZ','OSI','PUY','RJK','SPU','ZAD','ZAG','ATH','EFL','CHQ','JKH','CFU','HER','KLX','AOK','KVA','KGS','JMK','MJT','PVK','RHO','SMI','JTR','JSI','SKU','SKG','VOL','ZTH','AHO','AOI','BRI','BGY','BLQ','VBS','BDS','CAG','CTA','CUF','FLR','GOA','SUF','LIN','MXP','NAP','OLB','PMO','PMF','PEG','PSR','PSA','RMI','FCO','CIA','QSR','TPS','TRS','TRN','VCE','VRN','MLA','BYJ','FAO','FNC','LIS','PDL','OPO','PXO','TER','LJU','MBX','POW','LCG','ALC','LEI','OVD','BCN','BIO','CDT','FUE','GRO','LPA','GRX','HSK','IBZ','XRY','SPC','ACE','ILD','MAD','AGP','MAH','RMU','PMI','PNA','REU','SDR','SCQ','SVQ','TFN','TFS','VLC','VLL','VGO','VIT','ZAZ','TIA','GNA','GME','MSQ','BNX','OMO','SJJ','TZL','BOJ','PDV','SOF','VAR','PRN','RMO','ARW','BCM','BAY','GHV','OTP','BBU','CLJ','CND','CRA','IAS','OMR','SUJ','SBZ','SCV','TGM','TSR','TGD','TIV','OHD','SKP','ABA','DYR','AAQ','ARH','ASF','BAX','EGO','BQS','BTK','BZK','CSY','CEK','CEE','HTA','ESL','GRV','IKT','KGD','KZN','KHV','KXK','KRR','KJA','URS','GDX','MQF','MCX','MRV','DME','ZIA','SVO','VKO','MMK','NAL','NBC','NJC','GOJ','NOZ','OVB','OMS','REN','OSW','PEE','PES','PVS','PKC','PKV','ROV','LED','KUF','GSV','AER','STW','SGC','SCW','TOF','TJM','UUD','ULV','UFA','VVO','OGZ','VOG','VOZ','YKS','IAR','SVX','UUS','BEG','KVO','INI','CWC','IFO','HRK','KWG','KBP','IEV','LWO','NLV','ODS','PLV','SIP','UDJ','OZH','AAL','AAR','BLL','CPH','EPU','TLL','TAY','FAE','MHQ','HEL','KTT','KUO','KAO','LPP','OUL','RVN','SVL','TMP','TKU','VAA','AEY','EGS','KEF','RKV','RIX','VNT','KUN','PLQ','SQQ','VNO','AES','BGO','BOO','HAU','KRS','KSU','OSL','TRF','SVG','TOS','TRD','GOT','LLA','MMX','NRK','OSD','ARN','BMA','NYO','VST','SDL','UME','VXO','VBY']

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
    categorical_transformer = Pipeline([('imputer', SimpleImputer(strategy='most_frequent')),('onehot', OneHotEncoder(handle_unknown='ignore', drop='first', sparse_output=True))])
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
    # Quick / moderate / heavy grids
    if problem_type == 'classification':
        if name == 'DecisionTree':
            if GRID_LEVEL == 'quick': return {'classifier__max_depth':[2,3]}
            if GRID_LEVEL == 'moderate': return {'classifier__max_depth':[2,3,4,5],'classifier__min_samples_split':[2,5]}
            return {'classifier__max_depth':[2,3,4,5,6,7],'classifier__min_samples_split':[2,5,10],'classifier__min_samples_leaf':[1,2,4]}
        if name == 'RandomForest':
            if GRID_LEVEL == 'quick': return {'classifier__n_estimators':[50,100]}
            if GRID_LEVEL == 'moderate': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[None,10,20]}
            return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[None,10,20,30],'classifier__min_samples_split':[2,5,10]}
    if problem_type == 'regression':
        if name == 'DecisionTreeRegressor':
            if GRID_LEVEL == 'quick': return {'classifier__max_depth':[3,5,7]}
            if GRID_LEVEL == 'moderate': return {'classifier__max_depth':[2,3,4,5,6,7,8]}
            return {'classifier__max_depth':[2,3,4,5,6,7,8,9,10],'classifier__min_samples_split':[2,5,10],'classifier__min_samples_leaf':[1,2,4]}
        if name == 'RandomForestRegressor':
            if GRID_LEVEL == 'quick': return {'classifier__n_estimators':[50,100]}
            if GRID_LEVEL == 'moderate': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[None,10,20]}
            return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[None,10,20,30],'classifier__min_samples_split':[2,5,10]}
        if name == 'XGBRegressor':
            if GRID_LEVEL == 'quick': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[3,5]}
            if GRID_LEVEL == 'moderate': return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[3,5,7],'classifier__learning_rate':[0.01,0.1]}
            return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[3,5,7,10],'classifier__learning_rate':[0.001,0.01,0.1],'classifier__subsample':[0.5,0.7,1]}
        if name == 'LinearRegression':
            return {}  # No hyperparameters to tune
    return {}

# ----------------------
# PIPELINE TESTING AND METRICS
# ----------------------
classification_summary = []
regression_summary = []
feature_summary = []

def test_pipeline_classification(name, pipeline_tuple, mode='simple'):
    pipe, grid_params = pipeline_tuple
    if mode=='grid' and grid_params:
        gs = GridSearchCV(pipe, grid_params, cv=5, scoring='accuracy', verbose=2, n_jobs=GRID_PARALLEL_JOBS)
        gs.fit(X_train_cls, y_train_cls)
        return gs
    else:
        pipe.fit(X_train_cls, y_train_cls)
        return pipe

def test_pipeline_regression(name, pipeline_tuple, mode='simple'):
    pipe, grid_params = pipeline_tuple
    if mode=='grid' and grid_params:
        gs = GridSearchCV(pipe, grid_params, cv=5, scoring='r2', verbose=2, n_jobs=GRID_PARALLEL_JOBS)
        gs.fit(X_train_reg, y_train_reg)
        return gs
    else:
        pipe.fit(X_train_reg, y_train_reg)
        return pipe

def output_metrics_classification(name, pipe):
    y_pred = pipe.predict(X_test_cls)
    report = classification_report(y_test_cls, y_pred, output_dict=True)
    pd.DataFrame(report).transpose().to_csv(f"{OUTPUT_DIR}/classification/{name}_classification_report.csv")
    f, ax = plt.subplots(figsize=(10,10))
    ConfusionMatrixDisplay.from_predictions(y_test_cls, y_pred, ax=ax)
    plt.savefig(f"{OUTPUT_DIR}/classification/confusion_matrix/{name}_confusion_matrix.png")
    save_feature_importance(pipe, X_train_cls, name, 'classification')

def output_metrics_regression(name, pipe):
    y_pred = pipe.predict(X_test_reg)
    metrics = {'mae': mean_absolute_error(y_test_reg, y_pred),
               'mse': mean_squared_error(y_test_reg, y_pred),
               'rmse': np.sqrt(mean_squared_error(y_test_reg, y_pred)),
               'r2': r2_score(y_test_reg, y_pred)}
    pd.DataFrame([metrics]).to_csv(f"{OUTPUT_DIR}/regression/{name}_regression_report.csv", index=False)
    plt.figure(figsize=(10,6))
    plt.scatter(y_test_reg, y_pred, alpha=0.5)
    plt.plot([y_test_reg.min(), y_test_reg.max()], [y_test_reg.min(), y_test_reg.max()], 'k--')
    plt.xlabel("Actual"); plt.ylabel("Predicted"); plt.title(f"{name} Predictions vs Actual")
    plt.savefig(f"{OUTPUT_DIR}/regression/predictions/{name}_yTest_predicted.png")
    save_feature_importance(pipe, X_train_reg, name, 'regression')

def save_feature_importance(pipe, X, name, problem_type):
    try:
        clf = pipe.best_estimator_ if hasattr(pipe, 'best_estimator_') else pipe
        if 'preprocessor' in clf.named_steps:
            preproc = clf.named_steps['preprocessor']
            num_features = preproc.transformers_[0][2]
            cat_features = []
            if hasattr(preproc.transformers_[1][1], 'named_steps') and 'onehot' in preproc.transformers_[1][1].named_steps:
                cat_features = preproc.transformers_[1][1].named_steps['onehot'].get_feature_names_out().tolist()
            features = list(num_features) + cat_features
            if USE_FEATURE_SELECTION and 'feature_selection' in clf.named_steps:
                mask = clf.named_steps['feature_selection'].get_support()
                features = [f for f, m in zip(features, mask) if m]
        else:
            features = [f"f{i}" for i in range(X.shape[1])]
        classifier = clf.named_steps['classifier']
        importances = classifier.feature_importances_ if hasattr(classifier,'feature_importances_') else np.abs(classifier.coef_).flatten()
        df_imp = pd.DataFrame({'pipeline': name, 'problem_type': problem_type,'feature':features,'importance':importances})
        df_imp.to_csv(f"{OUTPUT_DIR}/{problem_type}/feature_importance/{name}_feature_importance.csv", index=False)
        feature_summary.append(df_imp)
    except Exception as e:
        logger.warning(f"Failed to save feature importance for {name}: {e}")

def save_combined_feature_summary():
    if feature_summary:
        pd.concat(feature_summary).to_csv(f"{OUTPUT_DIR}/dataset_summary/combined_feature_summary.csv", index=False)

# ----------------------
# RUN PIPELINES
# ----------------------
# Classification
X_cls = df_past.drop(columns=['flightLegs_Category'])
y_cls = df_past['flightLegs_Category']
X_train_cls, X_test_cls, y_train_cls, y_test_cls = train_test_split(X_cls, y_cls, test_size=TEST_SIZE, stratify=y_cls, random_state=RANDOM_STATE)
preprocessor_cls = build_preprocessor(X_cls, 'flightLegs_Category')

classification_pipelines = {
    'DecisionTree':[Pipeline([('preprocessor', preprocessor_cls),('classifier',DecisionTreeClassifier())]), get_grid_params('DecisionTree','classification')],
    'RandomForest':[Pipeline([('preprocessor', preprocessor_cls),('classifier',RandomForestClassifier(n_jobs=GRID_PARALLEL_JOBS))]), get_grid_params('RandomForest','classification')],
    'Logistic_OVO':[Pipeline([('preprocessor', preprocessor_cls),('classifier',OneVsOneClassifier(LogisticRegression(max_iter=1000))) ]), {}],
    'Logistic_OVR':[Pipeline([('preprocessor', preprocessor_cls),('classifier',OneVsRestClassifier(LogisticRegression(max_iter=1000))) ]), {}]
}

for name, pipeline_tuple in classification_pipelines.items():
    if RUN_MODE in ['simple','both']:
        pipe = test_pipeline_classification(name, pipeline_tuple, mode='simple')
        output_metrics_classification(name, pipe)
    if RUN_MODE in ['grid','both']:
        pipe = test_pipeline_classification(name, pipeline_tuple, mode='grid')
        output_metrics_classification(name, pipe)

# Regression
df_delay = df_past[df_past['flightLegs_Category']=='late'].dropna(subset=['flightLegs_irregularity_delayDuration_total'])
X_reg = df_delay.drop(columns=['flightLegs_irregularity_delayDuration_total'])
y_reg = df_delay['flightLegs_irregularity_delayDuration_total']
X_train_reg, X_test_reg, y_train_reg, y_test_reg = train_test_split(X_reg, y_reg, test_size=TEST_SIZE, random_state=RANDOM_STATE)
preprocessor_reg = build_preprocessor(X_reg, 'flightLegs_irregularity_delayDuration_total')

regression_pipelines = {
    'DecisionTreeRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',DecisionTreeRegressor())]), get_grid_params('DecisionTreeRegressor','regression')],
    'RandomForestRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',RandomForestRegressor(n_jobs=GRID_PARALLEL_JOBS))]), get_grid_params('RandomForestRegressor','regression')],
    'XGBRegressor':[Pipeline([('preprocessor', preprocessor_reg),('classifier',XGBRegressor(n_jobs=GRID_PARALLEL_JOBS))]), get_grid_params('XGBRegressor','regression')],
    'LinearRegression':[Pipeline([('preprocessor', preprocessor_reg),('classifier',LinearRegression())]), get_grid_params('LinearRegression','regression')]
}

for name, pipeline_tuple in regression_pipelines.items():
    if RUN_MODE in ['simple','both']:
        pipe = test_pipeline_regression(name, pipeline_tuple, mode='simple')
        output_metrics_regression(name, pipe)
    if RUN_MODE in ['grid','both']:
        pipe = test_pipeline_regression(name, pipeline_tuple, mode='grid')
        output_metrics_regression(name, pipe)

# Save combined feature summary
save_combined_feature_summary()
logger.info("Saved combined feature summary")
logger.info("End of ML script")
