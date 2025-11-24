#!/usr/bin/env python3
import os, re, datetime, logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder, MinMaxScaler
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.multiclass import OneVsOneClassifier, OneVsRestClassifier
from sklearn.metrics import classification_report, ConfusionMatrixDisplay, mean_absolute_error, mean_squared_error, r2_score, accuracy_score
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.tree import plot_tree
from xgboost import XGBRegressor
import pickle
import json
import re
from copy import deepcopy


# ----------------------------------------------------------------------------------------------------------------------------------------------------------
# PARAMETERS AND CONSTANTS
# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# ----------------------
# CONFIG
# ----------------------

cwd = os.getcwd()
if cwd.endswith("DST_DE_Airlines"):
    os.chdir("3_ML")
elif cwd.endswith("1_data_collection"):
    os.chdir("../3_ML")
elif cwd.endswith("1_data_collection"):
    os.chdir("../3_ML")
elif cwd.endswith("afklm_api_collection"):
    os.chdir("../../3_ML")
else:
    try:
        script_path = os.path.dirname(os.path.realpath(__file__))
        os.chdir(script_path)
    except:
        pass
cwd = os.getcwd()

print(cwd)



try:
    with open('./config/afklm_ml_training_settings.json') as json_file:
        ml_training_settings = json.load(json_file)
    params_from_json = True
    
except:
    ml_training_settings = {
        "DATA_FILE_ROOT_NAME" : "afklm_flight_from_mongo_filtered",
"RUN_MODE" : "simple",     
"FILTER_AIRPORTS_OPTIONAL" : "True",
"FILTER_AIRPORTS_MANDATORY" : "True",
"TOP_K_FEATURES" : 20,
"GRID_LEVEL" : "quick",  
"PARALLEL_JOBS" : 6,
"TEST_SIZE" : 0.2,
"RANDOM_STATE" : 42,
"RECORD_LIMIT" : "", 
"MODEL_TO_KEEP" : "LATEST",
"TARGET_REGRESSION" : "flightLegs_irregularity_delayDuration_total",
"TARGET_CLASSIFICATION_STATUS" : "flightlegs_publishedstatus",
"TARGET_CLASSIFICATION_DELAY" : "flightlegs_irregularity_delayduration_total_bracket",
"MODEL_LIST_TO_TEST" : [
    "LinearRegression",
    "RandomForest",

    "DecisionTreeRegressor",
    "RandomForestRegressor",
    "XGBRegressor",
    "DecisionTree",
    "Logistic_OVO",
    "Logistic_OVR"
    ], 
"CV_NB" : 5,
"columnKeywordsToDrop_all" : ["id",
                            "airline_name",
                            "flightlegs_aircraft_ownerairlineCode",
                            "actual",
                            "posterm",
                            "latestpublished",
                            "airline_code",
                            "iata",
                            "icao",
                            "company_flight",
                            "city_country_areaCode",
                            "airport_location",
                            "airport_city_country_name"],
"columnKeywordsToKeep_classification_status" : [
      "flightlegs_aircraft_ownerairlinecode", 
"flightlegs_aircraft_typecode",
 "flightlegs_arrinfo_airport_city_country_areacode", 
 "flightlegs_arrinfo_airport_code", 
 "flightlegs_depinfo_airport_city_country_areacode",
  "flightlegs_depinfo_airport_code",
   "flightlegs_scheduledflightduration",
      "flightlegs_season", 
      "flightlegs_arrinfo_times_scheduled_dayPeriod",
       "flightlegs_depinfo_times_scheduled_dayPeriod"],
"columnKeywordsToDrop_classification_status" : ["delay",
                                       "country_code",
                                       "flightNumber",
                                       "flightlegs_legstatuspublic",
                                       "airline_name",
                                       "flightlegs_serviceType",
                                       "status",
                                       "status",
                                       "estimated"],

"columnKeywordsToDrop_classification_delay" : ["country_code",
                                   "flightNumber",
                                   "flightlegs_legdelaypublic",
                                   "airline_name",
                                   "flightlegs_serviceType",
                                   "estimated",
                                   "irregularity_delayInformation",     
                                   "flightlegs_category",
                                   "flightstatuspublic",
                                   "status",
                                   "status",
                                   "flightlegs_irregularity_delayReason"],
"columnsToDrop_classification_status" : ["flightlegs_arrinfo_times_scheduled",
                                "flightlegs_departureInformation_times_scheduled"],
"columnsToDrop_classification_delay" : ["flightlegs_arrinfo_times_scheduled",
                            "flightlegs_departureInformation_times_scheduled",
                            "flightlegs_irregularity_delayduration"],
"DATA_DIR" : "data",
"OUTPUT_DIR": "outputs"
}


script_start_time =  re.sub("\\..*","",datetime.datetime.now().isoformat().replace(":","-").replace('-',"_"))



# --------------------------------------------------------------
# OUTPUT DIRS
# --------------------------------------------------------------


output_folder = f'{ml_training_settings['OUTPUT_DIR']}/{script_start_time}'
best_model_folder = f'{ml_training_settings['OUTPUT_DIR']}/best_models'
os.makedirs(best_model_folder, exist_ok=True)
os.makedirs(output_folder, exist_ok=True)
os.makedirs(f'{output_folder}/classification_status/confusion_matrix', exist_ok=True)
os.makedirs(f'{output_folder}/classification_status/feature_importance', exist_ok=True)
os.makedirs(f'{output_folder}/classification_status/tree_plots', exist_ok=True)
os.makedirs(f'{output_folder}/classification_delay/confusion_matrix', exist_ok=True)
os.makedirs(f'{output_folder}/classification_delay/feature_importance', exist_ok=True)
os.makedirs(f'{output_folder}/classification_delay/tree_plots', exist_ok=True)
os.makedirs(f'{output_folder}/dataset_summary', exist_ok=True)
os.makedirs(f'{output_folder}/regression/feature_importance', exist_ok=True)
os.makedirs(f'{output_folder}/regression/predictions', exist_ok=True)
os.makedirs(f'{output_folder}/regression/models', exist_ok=True)
os.makedirs(f'{output_folder}/regression/tree_plots', exist_ok=True)




# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


logger = logging.getLogger(__name__)
handler = logging.FileHandler(f'{output_folder}/{script_start_time}_ML.log')
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.info("==== Starting ML script====")
logger.info(f"Nb of cores: {ml_training_settings['PARALLEL_JOBS']}\n")

if params_from_json:
    logger.info(f"ML pipeline settings loaded from afklm_ml_training_settings.json")
else:
    logger.warning(f"afklm_ml_training_settings.json not found. ML pipeline settings loaded from default parameters")




# --------------------------------------------------------------
# EU AIRPORTS
# --------------------------------------------------------------


try:
    with open('./config/airport_list.json') as json_file:
        airport_list = json.load(json_file)
        airports_mandatory = airport_list['mandatory']
        airports_optional = airport_list['optional']
    logger.info(f"Airport list loaded from airport_list.json")
except:
    logger.warning(f"Airport list loaded from default parameters")

    airports_mandatory = ['CDG','AMS','ORY','FCO','LHR','CPH','MAD','ARN','OSL','LIN','NCE','BCN','LYS','BGO','LIS','DUB','HEL','TLS','OTP','FRA','MRS','ATH','PMI','MUC','TRD','MAN','BER','AGP','OPO']

    airports_optional = ['BHX','BOH','BRS','EXT','HUY','LBA','LPL','LGW','LHR','LCY','SEN','STN','LTN','MAN','MME','NCL','NQY','NWI','EMA','SOU','BFS','BHD','LDY','ABZ','EDI','GLA','PIK','INV','CWL','ANR','BRU','CRL','LGG','OST','AJA','BIA','BVA','EGC','BZR','BIQ','BOD','BES','CCF','XCR','CMF','DNR','FSC','GNB','LRH','LIL','LIG','LYS','MRS','BSL','NTE','NCE','FNI','CDG','ORY','PUF','PGF','PIS','RDZ','EBU','SXB','TLN','TLS','TUF','GIB','ORK','DUB','KIR','NOC','SNN','IOM','JER','LUX','AMS','EIN','GRQ','MST','RTM','GRZ','KLU','INN','LNZ','SZG','VIE','BRQ','JCL','KLV','OSR','PED','PRG','FKB','BER','BRE','CGN','DTM','DUS','FRA','HHN','FDH','HAM','HAJ','LEJ','LBC','FMM','MUC','NUE','STR','NRN','BUD','DEB','SOB','BZG','GDN','KTW','KRK','LUZ','LCJ','SZY','POZ','RZE','SZZ','WAW','WMI','RDO','WRO','BTS','KSC','PZY','TAT','ILZ','BSL','BRN','GVA','LUG','ACH','ZRH','BWK','DBV','LSZ','OSI','PUY','RJK','SPU','ZAD','ZAG','ATH','EFL','CHQ','JKH','CFU','HER','KLX','AOK','KVA','KGS','JMK','MJT','PVK','RHO','SMI','JTR','JSI','SKU','SKG','VOL','ZTH','AHO','AOI','BRI','BGY','BLQ','VBS','BDS','CAG','CTA','CUF','FLR','GOA','SUF','LIN','MXP','NAP','OLB','PMO','PMF','PEG','PSR','PSA','RMI','FCO','CIA','QSR','TPS','TRS','TRN','VCE','VRN','MLA','BYJ','FAO','FNC','LIS','PDL','OPO','PXO','TER','LJU','MBX','POW','LCG','ALC','LEI','OVD','BCN','BIO','CDT','FUE','GRO','LPA','GRX','HSK','IBZ','XRY','SPC','ACE','ILD','MAD','AGP','MAH','RMU','PMI','PNA','REU','SDR','SCQ','SVQ','TFN','TFS','VLC','VLL','VGO','VIT','ZAZ','TIA','GNA','GME','MSQ','BNX','OMO','SJJ','TZL','BOJ','PDV','SOF','VAR','PRN','RMO','ARW','BCM','BAY','GHV','OTP','BBU','CLJ','CND','CRA','IAS','OMR','SUJ','SBZ','SCV','TGM','TSR','TGD','TIV','OHD','SKP','ABA','DYR','AAQ','ARH','ASF','BAX','EGO','BQS','BTK','BZK','CSY','CEK','CEE','HTA','ESL','GRV','IKT','KGD','KZN','KHV','KXK','KRR','KJA','URS','GDX','MQF','MCX','MRV','DME','ZIA','SVO','VKO','MMK','NAL','NBC','NJC','GOJ','NOZ','OVB','OMS','REN','OSW','PEE','PES','PVS','PKC','PKV','ROV','LED','KUF','GSV','AER','STW','SGC','SCW','TOF','TJM','UUD','ULV','UFA','VVO','OGZ','VOG','VOZ','YKS','IAR','SVX','UUS','BEG','KVO','INI','CWC','IFO','HRK','KWG','KBP','IEV','LWO','NLV','ODS','PLV','SIP','UDJ','OZH','AAL','AAR','BLL','CPH','EPU','TLL','TAY','FAE','MHQ','HEL','KTT','KUO','KAO','LPP','OUL','RVN','SVL','TMP','TKU','VAA','AEY','EGS','KEF','RKV','RIX','VNT','KUN','PLQ','SQQ','VNO','AES','BGO','BOO','HAU','KRS','KSU','OSL','TRF','SVG','TOS','TRD','GOT','LLA','MMX','NRK','OSD','ARN','BMA','NYO','VST','SDL','UME','VXO','VBY']




# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# FONCTIONS
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



# ----------------------
# FEATURE PROCESSING
# ----------------------


def get_dayPeriod(x):
    if (x >= 6) & (x < 12):
        return "morning"
    elif (x >= 12) & (x < 18):
        return "afternoon"
    elif (x >= 18) & (x < 24):
        return "evening"
    else:
        return 'night'


def get_features(df, target_col):
    logger.info(f"Extracting features from dataset, target column: {target_col}")
    numeric_features = df.select_dtypes(include=['number']).columns.tolist()
    if target_col in numeric_features: numeric_features.remove(target_col)
    categorical_features = df.select_dtypes(include=['object','category']).columns.tolist()
    if target_col in categorical_features: categorical_features.remove(target_col)
    logger.info(f"Identified {len(numeric_features)} numeric and {len(categorical_features)} categorical features")
    logger.info(f"Numeric features: {numeric_features}")
    logger.info(f"Categorical features: {categorical_features}\n") 

    return numeric_features, categorical_features

def build_preprocessor(X, target_col):
    numeric_features, categorical_features = get_features(X, target_col)
    numerical_transformer = Pipeline([('imputer', SimpleImputer(strategy='median')),('scaler', StandardScaler())])
    categorical_transformer = Pipeline([('imputer', SimpleImputer(strategy='most_frequent')),
                                       ('onehot', OneHotEncoder(handle_unknown='ignore', drop='first', sparse_output=True))])
    preprocessor = ColumnTransformer(transformers=[('num', numerical_transformer, numeric_features),
                                                   ('cat', categorical_transformer, categorical_features)])
    return preprocessor




def categorize_status(row):
    if (row[ml_training_settings['TARGET_CLASSIFICATION_STATUS']]=='CANCELLED') |(row[ml_training_settings['TARGET_REGRESSION']]>360) : 
        return 'CANCELLED'
    return 'LATE' if row[ml_training_settings['TARGET_REGRESSION']] > 0 else 'ONTIME'



# ----------------------
# GRID PARAMETERS
# ----------------------

def get_grid_params(name, problem_type):
    if problem_type=='classification_status':
        if name=='DecisionTree':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__max_depth':[2,3],'classifier__criterion':['gini','entropy']}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__max_depth':[2,3,4,5],'classifier__criterion':['gini','entropy'],'classifier__min_samples_split':[2,5]}
            else: return {'classifier__max_depth':[2,3,4,5,6,7],'classifier__criterion':['gini','entropy'],'classifier__min_samples_split':[2,5,10],'classifier__min_samples_leaf':[1,2,4]}
        if name=='RandomForest':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__n_estimators':[50,100]}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[None,10,20]}
            else: return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[None,10,20,30],'classifier__min_samples_split':[2,5,10]}
        if name=='Logistic_OVR':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l2']}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l2'],'classifier__estimator__solver': ['liblinear', 'lbfgs']}
            else: return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l1','l2'],'classifier__estimator__solver': ['liblinear', 'lbfgs'],'classifier__estimator__class_weight': [None, 'balanced']}
        if name=='Logistic_OVO':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l2']}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l2'],'classifier__estimator__solver': ['liblinear', 'lbfgs']}
            else: return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l1','l2'],'classifier__estimator__solver': ['liblinear', 'lbfgs'],'classifier__estimator__class_weight': [None, 'balanced']}
            
    elif problem_type=='classification_delay':
        if name=='DecisionTree':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__max_depth':[2,3],'classifier__criterion':['gini','entropy']}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__max_depth':[2,3,4,5],'classifier__criterion':['gini','entropy'],'classifier__min_samples_split':[2,5]}
            else: return {'classifier__max_depth':[2,3,4,5,6,7],'classifier__criterion':['gini','entropy'],'classifier__min_samples_split':[2,5,10],'classifier__min_samples_leaf':[1,2,4]}
        if name=='RandomForest':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__n_estimators':[50,100]}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[None,10,20]}
            else: return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[None,10,20,30],'classifier__min_samples_split':[2,5,10]}
        if name=='Logistic_OVR':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l2']}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l2'],'classifier__estimator__solver': ['liblinear', 'lbfgs']}
            else: return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l1','l2'],'classifier__estimator__solver': ['liblinear', 'lbfgs'],'classifier__estimator__class_weight': [None, 'balanced']}
        if name=='Logistic_OVO':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l2']}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l2'],'classifier__estimator__solver': ['liblinear', 'lbfgs']}
            else: return {'classifier__estimator__C': [0.01, 0.1, 1],'classifier__estimator__penalty': ['l1','l2'],'classifier__estimator__solver': ['liblinear', 'lbfgs'],'classifier__estimator__class_weight': [None, 'balanced']}
    else:  # regression
        if name=='DecisionTreeRegressor':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__max_depth':[3,5,7]}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__max_depth':[2,3,4,5,6,7,8]}
            else: return {'classifier__max_depth':[2,3,4,5,6,7,8,9,10],'classifier__min_samples_split':[2,5,10],'classifier__min_samples_leaf':[1,2,4]}
        if name=='RandomForestRegressor':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__n_estimators':[50,100]}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[None,10,20]}
            else: return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[None,10,20,30],'classifier__min_samples_split':[2,5,10]}
        if name=='XGBRegressor':
            if ml_training_settings['GRID_LEVEL']=='quick': return {'classifier__n_estimators':[50,100],'classifier__max_depth':[3,5]}
            elif ml_training_settings['GRID_LEVEL']=='moderate': return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[3,5,7],'classifier__learning_rate':[0.01,0.1]}
            else: return {'classifier__n_estimators':[50,100,200],'classifier__max_depth':[3,5,7,10],'classifier__learning_rate':[0.001,0.01,0.1],'classifier__subsample':[0.5,0.7,1]}
        if name=='LinearRegression':
            return {}  # no parameters for linear regression
            
    return {}


# ----------------------
# FEATURE IMPORTANCE LOGGING
# ----------------------

top_features_summary = []

def save_feature_importance(pipeline, pipeline_name, problem_type):
    logger.info(f"Saving feature importance for {pipeline_name} ({problem_type})")
    try:
        clf = pipeline.best_estimator_ if hasattr(pipeline,'best_estimator_') else pipeline
        step_name = 'classifier' if 'classifier' in clf.named_steps else list(clf.named_steps.keys())[-1]
        model = clf.named_steps.get(step_name, clf)
        feature_names = []
        
        if 'preprocessor' in clf.named_steps:
            preproc = clf.named_steps['preprocessor']
            try:
                feature_names = preproc.get_feature_names_out()
                feature_names = [f.replace("num__", "").replace("cat__", "") for f in feature_names]  # optional cleanup
            except Exception:
                feature_names = [f"f{i}" for i in range(model.n_features_in_)]
        else:
            feature_names = [f"f{i}" for i in range(model.n_features_in_)]
            
            
            
        importances = getattr(model,'feature_importances_',None)
        if importances is None and hasattr(model,'coef_'):
            importances = np.abs(model.coef_).flatten()
        if importances is None: 
            logger.warning(f"No feature importances available for {pipeline_name}")
            return
        df_imp = pd.DataFrame({'feature':feature_names,'importance':importances})
        df_imp.sort_values('importance',ascending=False,inplace=True)
        df_imp.to_csv(f"{output_folder}/{problem_type}/feature_importance/{script_start_time}_{pipeline_name}_feature_importance.csv", index=False)
        logger.info(f"Feature importance CSV saved for {pipeline_name}")
        df_imp.head(ml_training_settings['TOP_K_FEATURES']).plot(kind='bar', x='feature', y='importance', legend=False, figsize=(10,6))
        plt.title(f"Top features for {pipeline_name}")
        plt.xticks(rotation=90)
        plt.tight_layout()
        plt.savefig(f"{output_folder}/{problem_type}/feature_importance/{script_start_time}_{pipeline_name}_feature_importance.png")
        plt.close()
        logger.info(f"Feature importance plot saved for {pipeline_name}")
        top_df = df_imp.head(ml_training_settings['TOP_K_FEATURES']).copy()
        top_df['pipeline'] = pipeline_name
        top_df['problem_type'] = problem_type
        top_features_summary.append(top_df)
    except Exception as e:
        logger.warning(f"Failed feature importance for {pipeline_name}: {e}")
        
        
# ----------------------
# PIPELINE TEST FUNCTIONS WITH LOGGING
# ----------------------

global_summary = []
classification_status_summary = []
classification_delay_summary = []
regression_summary = []

def test_pipeline_classification(name, pipeline_tuple, mode='simple', target = "status"):
    if target == "status":
        X_train = X_train_cls_status
        y_train = y_train_cls_status
        X_test = X_test_cls_status
        y_test = y_test_cls_status
        problem = 'classification_status'
    else:
        X_train = X_train_cls_delay
        y_train = y_train_cls_delay
        X_test = X_test_cls_delay
        y_test = y_test_cls_delay
        problem = 'classification_delay'
        
    
    pipe, params = pipeline_tuple
    logger.info(f"Starting {problem} pipeline: {name} | mode: {mode}")
    start_time = datetime.datetime.now()
    if mode=='grid' and params:
        logger.info(f"Performing GridSearchCV (cv = {ml_training_settings['CV_NB']}, scoring='accuracy') for {name} with parameters: {params}")
        gs = GridSearchCV(pipe, params, cv=ml_training_settings['CV_NB'], scoring='accuracy', n_jobs=ml_training_settings['PARALLEL_JOBS'])
        gs.fit(X_train, y_train)
        end_time = datetime.datetime.now()
        time_elapsed = (end_time- start_time).seconds        
        logger.info(f"GridSearchCV completed for {name} | best score: {gs.best_score_:.3f} | Total duration: {time_elapsed} seconds")
        save_feature_importance(gs, name, problem)
        save_decision_tree_plot(gs, name, problem)
        if (gs.best_score_> best_model_classification_status_score) | (ml_training_settings['MODEL_TO_KEEP'] == "LATEST"):
            with open(f'{best_model_folder}/{problem}_{script_start_time}_{name}_{mode}.pkl','wb') as f:
                pickle.dump(pipe,f)
        return gs, time_elapsed

    else:
        pipe.fit(X_train, y_train)
        end_time = datetime.datetime.now()
        time_elapsed = (end_time- start_time).seconds        

        y_pred = pipe.predict(X_test)
    
        score = accuracy_score(y_test.values, y_pred)
        if (score > best_model_classification_status_score) | (ml_training_settings['MODEL_TO_KEEP'] == "LATEST"):
            with open(f'{best_model_folder}/{problem}_{script_start_time}_{name}_{mode}.pkl','wb') as f:
                pickle.dump(pipe,f)

        logger.info(f"Fitted simple pipeline for {problem} {name} | Score: {score:.3f} | Total duration: {time_elapsed} seconds")
        save_feature_importance(pipe, name, problem)
        save_decision_tree_plot(pipe, name, problem)
        return pipe, time_elapsed



def output_metrics_classification(name, pipe, mode='simple', target = None, processing_time=None):
    
    if target == "status":
        X_train = X_train_cls_status
        y_train = y_train_cls_status
        X_test = X_test_cls_status
        y_test = y_test_cls_status
        problem = 'classification_status'
        target_variable = ml_training_settings['TARGET_CLASSIFICATION_STATUS']
    else:
        X_train = X_train_cls_delay
        y_train = y_train_cls_delay
        X_test = X_test_cls_delay
        y_test = y_test_cls_delay
        problem = 'classification_delay'
        target_variable = ml_training_settings['TARGET_CLASSIFICATION_DELAY']
        
    
    logger.info(f"Generating {problem} metrics for {name} | mode={mode}")
    y_pred = pipe.predict(X_test)
    report = classification_report(y_test, y_pred, output_dict=True)
    
    # Save detailed report
    pd.DataFrame(report).transpose().to_csv(f"{output_folder}/{problem}/{script_start_time}_{name}_{mode}_classification_report.csv")
    
    # Confusion matrix
    f, ax = plt.subplots(figsize=(10,10))
    ConfusionMatrixDisplay.from_predictions(y_test, y_pred, ax=ax)
    plt.savefig(f"{output_folder}/{problem}/confusion_matrix/{script_start_time}_{name}_{mode}_confusion_matrix.png")
    plt.close()
    
    # Extract hyperparameters

    if hasattr(pipe, 'best_estimator_'):
        model = pipe.best_estimator_.named_steps.get('classifier', pipe.best_estimator_)
    else:
        model = pipe.named_steps.get('classifier', pipe)
    hyperparams = json.dumps(sklearn_params_to_dict(model.get_params()))
    
  # Extract features used
    numeric_features, categorical_features = extract_feature_lists(pipe)

    # Add features to summary
    global_summary.append({
        'pipeline': name,
        'mode': mode,
        'problem_type': problem,
        'dataset_size_training': len(X_train),
        'dataset_size_testing': len(X_test),
        'accuracy': report['accuracy'],
        'macro_avg_precision': report['macro avg']['precision'],
        'macro_avg_recall': report['macro avg']['recall'],
        'macro_avg_f1': report['macro avg']['f1-score'],
        'hyperparameters': hyperparams,
        'processing_time': processing_time,
        'numeric_features': numeric_features,
        'categorical_features': categorical_features,
        'target_variable': target_variable
    })

def test_pipeline_regression(name, pipeline_tuple, mode='simple'):
    pipe, params = pipeline_tuple
    logger.info(f"Starting regression pipeline: {name} | mode: {mode}")
    start_time = datetime.datetime.now()
    if mode=='grid' and params:
        logger.info(f"Performing GridSearchCV (cv = {ml_training_settings['CV_NB']}, scoring='r2') for {name} with parameters: {params}")
        gs = GridSearchCV(pipe, params, cv=ml_training_settings['CV_NB'], scoring='r2', n_jobs=ml_training_settings['PARALLEL_JOBS'], verbose=2)
        gs.fit(X_train_reg, y_train_reg)
        end_time = datetime.datetime.now()
        time_elapsed = (end_time- start_time).seconds        
        logger.info(f"GridSearchCV completed for {name} | best score: {gs.best_score_:.3f} | Total duration: {time_elapsed} seconds")
        save_feature_importance(gs, name, 'regression')
        save_decision_tree_plot(gs, name, 'regression')

        if (gs.best_score_> best_model_regression_score) | (ml_training_settings['MODEL_TO_KEEP'] == "LATEST"):
            with open(f'{best_model_folder}/regression_{script_start_time}_{name}_{mode}.pkl','wb') as f:
                pickle.dump(pipe,f)
        return gs, time_elapsed
    else:
        pipe.fit(X_train_reg, y_train_reg)
        y_pred = pipe.predict(X_test_reg)
            
        score = r2_score(y_test_reg, y_pred)

        end_time = datetime.datetime.now()
        time_elapsed = (end_time- start_time).seconds     


        if (score >=best_model_regression_score) | (ml_training_settings['MODEL_TO_KEEP'] == "LATEST"):
            with open(f'{best_model_folder}/regression_{script_start_time}_{name}_{mode}.pkl','wb') as f:
                pickle.dump(pipe,f)

        logger.info(f"Fitted simple pipeline for {name} | r2: {score:.3f} | Total duration: {time_elapsed} seconds")
        save_feature_importance(pipe, name, 'regression')
        save_decision_tree_plot(pipe, name, 'regression')
        return pipe, time_elapsed

def output_metrics_regression(name, pipe, mode='simple', processing_time=None):
    logger.info(f"Generating regression metrics for {name} | mode={mode}")
    y_pred = pipe.predict(X_test_reg)
    metrics = {
        'mae': mean_absolute_error(y_test_reg, y_pred),
        'mse': mean_squared_error(y_test_reg, y_pred),
        'rmse': np.sqrt(mean_squared_error(y_test_reg, y_pred)),
        'r2': r2_score(y_test_reg, y_pred)
    }
    
    # Save regression report
    pd.DataFrame([metrics]).to_csv(f"{output_folder}/regression/{script_start_time}_{name}_{mode}_regression_report.csv", index=False)
    
    # Scatter plot
    plt.figure(figsize=(10,6))
    plt.scatter(y_test_reg, y_pred, alpha=0.5)
    plt.plot([y_test_reg.min(), y_test_reg.max()], [y_test_reg.min(), y_test_reg.max()], 'k--')
    plt.xlabel("Actual"); plt.ylabel("Predicted"); plt.title(f"{name} Predictions vs Actual ({mode})")
    plt.savefig(f"{output_folder}/regression/predictions/{script_start_time}_{name}_{mode}_yTest_predicted.png")
    plt.close()
    
    # Extract hyperparameters
    if hasattr(pipe, 'best_estimator_'):
        model = pipe.best_estimator_.named_steps.get('classifier', pipe.best_estimator_)
    else:
        model = pipe.named_steps.get('classifier', pipe)
    hyperparams = json.dumps(sklearn_params_to_dict(model.get_params()))   
 




   # Extract features used
    numeric_features, categorical_features = extract_feature_lists(pipe)

    # Add features to summary
    global_summary.append({
        'pipeline': name,
        'mode': mode,
        'problem_type': 'regression',
        'dataset_size_training': len(X_train_reg),
        'dataset_size_testing': len(X_test_reg),
        **metrics,
        'hyperparameters': hyperparams,
        'processing_time': processing_time,
        'numeric_features': numeric_features,
        'categorical_features': categorical_features,
        'target_variable': ml_training_settings['TARGET_REGRESSION']
    })
    
def extract_feature_lists(pipe):
    """Extract numeric and categorical feature names actually used by the preprocessor."""
    if hasattr(pipe, "best_estimator_"):
        clf = pipe.best_estimator_
    else:
        clf = pipe

    if 'preprocessor' not in clf.named_steps:
        return [], []

    preproc = clf.named_steps['preprocessor']

    numeric_features = []
    categorical_features = []

    for name, transformer, cols in preproc.transformers_:
        if name == 'num':
            numeric_features = cols
        elif name == 'cat':
            categorical_features = cols

    return list(numeric_features), list(categorical_features)


def save_decision_tree_plot(pipeline, pipeline_name, problem_type, feature_names=None):
    """Save a DecisionTree plot showing thresholds in original (non-standardized) feature units,
    even when using a ColumnTransformer with multiple numeric/categorical branches.
    """
    try:
        # --- Extract trained estimator ---
        clf = pipeline.best_estimator_ if hasattr(pipeline, 'best_estimator_') else pipeline
        model = clf.named_steps.get('classifier', clf)

        if not isinstance(model, (DecisionTreeClassifier, DecisionTreeRegressor)):
            logger.info(f"Skipping tree plot for {pipeline_name} (not a DecisionTree model)")
            return

        # --- Feature names ---
        if feature_names is None and 'preprocessor' in clf.named_steps:
            preproc = clf.named_steps['preprocessor']
            try:
                feature_names = preproc.get_feature_names_out()
                feature_names = [f.replace("num__", "").replace("cat__", "") for f in feature_names]
            except Exception:
                feature_names = [f"f{i}" for i in range(model.n_features_in_)]
        if feature_names is None:
            feature_names = [f"f{i}" for i in range(model.n_features_in_)]

        # --- Identify scalers & feature subsets ---
        scaler_map = {}  # {column_index: scaler}
        original_feature_indices = np.arange(model.n_features_in_)

        if 'preprocessor' in clf.named_steps:
            preproc = clf.named_steps['preprocessor']
            if hasattr(preproc, 'transformers_'):
                start_idx = 0
                for name, trans, cols in preproc.transformers_:
                    if name == 'remainder' and trans == 'drop':
                        continue
                    # get number of features this transformer outputs
                    if hasattr(trans, 'get_feature_names_out'):
                        n_out = len(trans.get_feature_names_out())
                    elif hasattr(cols, '__len__'):
                        n_out = len(cols)
                    else:
                        n_out = 1

                    # search for scaler in this branch
                    scaler = None
                    if isinstance(trans, (StandardScaler, MinMaxScaler)):
                        scaler = trans
                    elif hasattr(trans, 'named_steps'):
                        for step in trans.named_steps.values():
                            if isinstance(step, (StandardScaler, MinMaxScaler)):
                                scaler = step
                                break

                    if scaler is not None:
                        # Assign this scaler to corresponding feature indices
                        for j in range(n_out):
                            scaler_map[start_idx + j] = scaler
                    start_idx += n_out

        # --- Copy model and adjust thresholds ---
        model_for_plot = deepcopy(model)
        tree = model_for_plot.tree_
        n_features = model_for_plot.n_features_in_
        sample_scaled = np.zeros((1, n_features), dtype=float)

        for node in range(tree.node_count):
            f = tree.feature[node]
            thr = tree.threshold[node]
            if f >= 0 and thr != -2.0:
                scaler = scaler_map.get(f, None)
                if scaler is not None:
                    sample_scaled[:] = 0.0
                    sample_scaled[0, f] = thr
                    orig_thr = scaler.inverse_transform(sample_scaled)[0, f]
                    tree.threshold[node] = orig_thr

        logger.info(f"Adjusted thresholds to original units for {len(scaler_map)} scaled features")

        # --- Plot and save ---
        plt.figure(figsize=(25, 15))
        plot_tree(
            model_for_plot,
            filled=True,
            feature_names=feature_names,
            class_names=getattr(model, 'classes_', None),
            max_depth=3,
            fontsize=10
        )
        plt.title(f"Decision Tree - {pipeline_name}")
        plt.tight_layout()

        out_dir = os.path.join(output_folder, problem_type, "tree_plots")
        os.makedirs(out_dir, exist_ok=True)
        plt.savefig(os.path.join(out_dir, f"{script_start_time}_{pipeline_name}_tree.png"), dpi=200)
        plt.close()
        logger.info(f"Decision tree plot saved for {pipeline_name}")

    except Exception as e:
        logger.warning(f"Failed to save decision tree plot for {pipeline_name}: {e}")


# --------------------------------------------------------------------------------------------------------------------------------------------------------
# PIPELINE EXECUTION
# --------------------------------------------------------------------------------------------------------------------------------------------------------


# --------------------------------------
# DATA LOAD
# --------------------------------------

csv_files = sorted([f for f in list(os.listdir(ml_training_settings['DATA_DIR'])) if ml_training_settings['DATA_FILE_ROOT_NAME'] in f])
csv_to_import = csv_files[-1]
df = pd.read_csv(f'{ml_training_settings['DATA_DIR']}/{csv_to_import}', low_memory=False)
logger.info(f"Loaded dataset: {csv_to_import}, shape={df.shape}")

try:
    best_models = pd.read_csv(f"{best_model_folder}/best_models.csv")
    best_model_classification_status_score = best_models[best_models['problem_type'] == 'classification_status']['accuracy'].item()
    best_model_classification_delay_score = best_models[best_models['problem_type'] == 'classification_delay']['accuracy'].item()
    best_model_regression_score = best_models[best_models['problem_type'] == 'regression']['accuracy'].item()
except:
    best_model_classification_status_score = -100
    best_model_classification_delay_score = -100
    best_model_regression_score = -100



# --------------------------------------
# DATA CLEANING
# --------------------------------------

summary_steps = []

def log_step(name, df):
    logger.info(f"Step: {name} | shape: {df.shape} | null values: {df.isnull().sum().sum()}")
    summary_steps.append({'step': name, 'shape': df.shape, 'null_values': df.isnull().sum().sum()})

log_step('raw_data', df)

logger.info("Extracting 'company_flight' from 'id' column")
df["company_flight"] = df["id"].apply(lambda x: re.sub("^.*?\\+","", x))

logger.info(f"Dropping columns: {ml_training_settings['columnKeywordsToDrop_all']} and rows with missing {ml_training_settings['TARGET_CLASSIFICATION_DELAY']}")

df_cleaned = df.dropna(subset=ml_training_settings['TARGET_CLASSIFICATION_STATUS']).drop(columns=df.filter(regex='|'.join(ml_training_settings['columnKeywordsToDrop_all'])), errors='ignore')

log_step('drop_na_and_unused_cols', df_cleaned)



logger.info("Computing 'flightlegs_scheduledFlightDuration' in minutes")
df_cleaned["flightlegs_scheduledFlightDuration"] = df_cleaned.apply(
    lambda row: (datetime.datetime.fromisoformat(row.flightlegs_arrinfo_times_scheduled)
                 - datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled)).seconds / 60,
    axis=1
)


df_cleaned = df_cleaned[df_cleaned[ml_training_settings['TARGET_CLASSIFICATION_STATUS']].isin(['ARRIVED','CANCELLED','DELAYED_DEPARTURE','ONTIME'])]

logger.info(f"Filtered past flights only, resulting shape: {df_cleaned.shape}")




df_cleaned[ml_training_settings['TARGET_REGRESSION']] = df_cleaned[ml_training_settings['TARGET_REGRESSION']].fillna(0)
logger.info(f"Filled missing {ml_training_settings['TARGET_REGRESSION']} with 0")

logger.info("Categorizing flights into 'CANCELLED', 'LATE', 'ONTIME'")


df_cleaned[ml_training_settings['TARGET_CLASSIFICATION_STATUS']] = df_cleaned.apply(categorize_status, axis=1)
log_step('categorize_flights', df_cleaned)

logger.info("Categorizing flights into 'cancelled', 'late', 'on_time'")


def categorize_delay(row):   
    delay_value = row[ml_training_settings['TARGET_REGRESSION']] 
    if delay_value < 5:
        return "]000;005]"
    if delay_value < 15:
        return "]005;015]"
    elif delay_value < 30:
        return "]015;030]"
    elif delay_value < 60:
        return "]030;060]"
    elif delay_value < 120:
        return "]060;120]"
    elif delay_value < 240:
        return "]120;240]"
    else:
        return "]240;360]"
    
    
    
df_cleaned[ml_training_settings['TARGET_CLASSIFICATION_DELAY']] = df_cleaned.apply(categorize_delay, axis=1)
log_step('categorize_delay_brackets', df_cleaned)


if ml_training_settings['FILTER_AIRPORTS_OPTIONAL']:
    logger.info("Filtering flights in optional list")
    df_cleaned = df_cleaned[df_cleaned['flightlegs_arrinfo_airport_code'].isin(airports_optional) &
                      df_cleaned['flightlegs_depinfo_airport_code'].isin(airports_optional)]
    if ml_training_settings['FILTER_AIRPORTS_MANDATORY']:
        logger.info("AFiltering flights in mandatory list")
        df_cleaned = df_cleaned[df_cleaned['flightlegs_arrinfo_airport_code'].isin(airports_mandatory) |
                          df_cleaned['flightlegs_depinfo_airport_code'].isin(airports_mandatory)]
log_step('airport_filtering', df_cleaned)



# ----------------------
# FEATURE ENGINEERING
# ----------------------

logger.info("Adding seasonality, isWeekend and dayPeriod")


season_dictionary = {1:'winter',2:'winter',3:'spring',4:'spring',5:'spring',6:'summer',7:'summer',8:'summer',9:'fall',10:'fall',11:'fall',12:'winter'}



df_cleaned['flightlegs_season'] = df_cleaned.apply(
    lambda row: season_dictionary[(datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled).month)]
                ,
    axis=1
)

df_cleaned['flightlegs_arrinfo_times_scheduled_isWeekend'] = df_cleaned.apply(
    lambda row: True if datetime.datetime.fromisoformat(row.flightlegs_arrinfo_times_scheduled).isoweekday() in [6,7] else False
                ,
    axis=1
)

df_cleaned['flightlegs_arrinfo_times_scheduled_dayPeriod'] = df_cleaned.apply(
    lambda row: get_dayPeriod(datetime.datetime.fromisoformat(row.flightlegs_arrinfo_times_scheduled).hour + datetime.datetime.fromisoformat(row.flightlegs_arrinfo_times_scheduled).minute/60)
                ,
    axis=1
)


df_cleaned['flightlegs_depinfo_times_scheduled_isWeekend'] = df_cleaned.apply(
    lambda row: True if datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled).isoweekday() in [6,7] else False
                ,
    axis=1
)

df_cleaned['flightlegs_depinfo_times_scheduled_dayPeriod'] = df_cleaned.apply(
    lambda row: get_dayPeriod(datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled).hour + datetime.datetime.fromisoformat(row.flightlegs_depinfo_times_scheduled).minute/60)
                ,
    axis=1
)




# ----------------------
# DATA SPLIT LOGGING
# ----------------------

# Classification status split

logger.info("==== Preparing classification_status dataset ====")


try:
    df_status = df_cleaned.head(int(ml_training_settings['RECORD_LIMIT']))
    logger.info(f"limiting number of records to {ml_training_settings['RECORD_LIMIT']}")

except:
    df_status = df_cleaned
    logger.info(f"Using the full dataset")



df_status = df_status[ml_training_settings['columnKeywordsToKeep_classification_status'] + [ml_training_settings['TARGET_CLASSIFICATION_STATUS']]]


X_cls_status = df_status.drop(columns=[ml_training_settings['TARGET_CLASSIFICATION_STATUS']]).drop(list(df.filter(regex='|'.join(ml_training_settings['columnKeywordsToDrop_classification_status']))), axis=1, errors='ignore').drop(ml_training_settings['columnsToDrop_classification_status'], axis=1, errors='ignore')
y_cls_status = df_status[ml_training_settings['TARGET_CLASSIFICATION_STATUS']]


logger.info(f"Classification status dataset shape: X={X_cls_status.shape}, y={y_cls_status.shape}")
logger.info(f"Classification status dataset class repartition: {y_cls_status.value_counts(normalize = True)}")

X_train_cls_status, X_test_cls_status, y_train_cls_status, y_test_cls_status = train_test_split(
    X_cls_status, y_cls_status, test_size=ml_training_settings['TEST_SIZE'], stratify=y_cls_status, random_state=ml_training_settings['RANDOM_STATE']
)
logger.info(f"Classification status train/test split: X_train={X_train_cls_status.shape}, X_test={X_test_cls_status.shape}, "
            f"y_train={y_train_cls_status.shape}, y_test={y_test_cls_status.shape}")

preprocessor_cls_status = build_preprocessor(X_cls_status, ml_training_settings['TARGET_CLASSIFICATION_STATUS'])

def sklearn_params_to_dict(params):
    """
    Recursively convert sklearn parameters to JSON-serializable dict.
    """
    serializable = {}
    for k, v in params.items():
        try:
            serializable[k] = v.get_params() if hasattr(v, 'get_params') else v
        except:
            serializable[k] = str(v)  # fallback
    return serializable





# Classification delay split



logger.info("==== Preparing classification_delay dataset ====")


df_delay_class = df_cleaned[ml_training_settings['columnKeywordsToKeep_classification_status'] +[ ml_training_settings['TARGET_CLASSIFICATION_STATUS']] + [ml_training_settings['TARGET_CLASSIFICATION_DELAY']]]

df_delay_class = df_delay_class[df_cleaned[ml_training_settings['TARGET_CLASSIFICATION_STATUS']]=='LATE'].dropna(subset=[ml_training_settings['TARGET_CLASSIFICATION_DELAY']]).drop(
    list(df.filter(regex='|'.join(ml_training_settings['columnKeywordsToDrop_classification_delay']))), axis=1, errors='ignore'
).drop(ml_training_settings['columnsToDrop_classification_delay'], axis=1, errors='ignore')

log_step('lateFlights_filtering', df_delay_class)


try:
    df_delay_class = df_delay_class.head(int(ml_training_settings['RECORD_LIMIT']))
    logger.info(f"limiting number of records to {ml_training_settings['RECORD_LIMIT']}")

except:

    logger.info(f"Using the full dataset")



X_cls_delay = df_delay_class.drop(columns=[ml_training_settings['TARGET_CLASSIFICATION_DELAY']])
y_cls_delay = df_delay_class[ml_training_settings['TARGET_CLASSIFICATION_DELAY']]
logger.info(f"Classification delay dataset shape: X={X_cls_delay.shape}, y={y_cls_delay.shape}")

X_train_cls_delay, X_test_cls_delay, y_train_cls_delay, y_test_cls_delay = train_test_split(
    X_cls_delay, y_cls_delay, test_size=ml_training_settings['TEST_SIZE'], random_state=ml_training_settings['RANDOM_STATE']
)
logger.info(f"Classification delay train/test split: X_train={X_train_cls_delay.shape}, X_test={X_test_cls_delay.shape}, "
            f"y_train={y_train_cls_delay.shape}, y_test={y_test_cls_delay.shape}")

preprocessor_cls_delay = build_preprocessor(X_cls_delay, ml_training_settings['TARGET_CLASSIFICATION_DELAY'])



# Regression split



logger.info("==== Preparing regression dataset ====")

df_delay_reg = df_cleaned[ml_training_settings['columnKeywordsToKeep_classification_status'] +[ ml_training_settings['TARGET_CLASSIFICATION_STATUS']] + [ml_training_settings['TARGET_REGRESSION']]]

df_delay_reg = df_delay_reg[df_delay_reg[ml_training_settings['TARGET_CLASSIFICATION_STATUS']]=='LATE'].dropna(subset=[ml_training_settings['TARGET_REGRESSION']]).drop(
    list(df.filter(regex='|'.join(ml_training_settings['columnKeywordsToDrop_regression']))), axis=1, errors='ignore'
).drop(ml_training_settings['columnsToDrop_regression'], axis=1, errors='ignore')

log_step('lateFlights_filtering', df_delay_reg)

pd.DataFrame(summary_steps).to_csv(f'{output_folder}/dataset_summary/{script_start_time}_dataset_cleaning_summary.csv', index=False)
logger.info(f"Saved dataset cleaning summary to '{output_folder}/dataset_summary/dataset_cleaning_summary.csv'\n")


try:
    df_delay_reg = df_delay_reg.head(int(ml_training_settings['RECORD_LIMIT']))
    logger.info(f"limiting number of records to {ml_training_settings['RECORD_LIMIT']}")

except:

    logger.info(f"Using the full dataset")



X_reg = df_delay_reg.drop(columns=[ml_training_settings['TARGET_REGRESSION']])
y_reg = df_delay_reg[ml_training_settings['TARGET_REGRESSION']]
logger.info(f"Regression dataset shape: X={X_reg.shape}, y={y_reg.shape}")

X_train_reg, X_test_reg, y_train_reg, y_test_reg = train_test_split(
    X_reg, y_reg, test_size=ml_training_settings['TEST_SIZE'], random_state=ml_training_settings['RANDOM_STATE']
)
logger.info(f"Regression train/test split: X_train={X_train_reg.shape}, X_test={X_test_reg.shape}, "
            f"y_train={y_train_reg.shape}, y_test={y_test_reg.shape}")

preprocessor_reg = build_preprocessor(X_reg, ml_training_settings['TARGET_REGRESSION'])






# ----------------------
# REGRESSION PIPELINES
# ----------------------

if ml_training_settings['RUN_MODE'] == 'simple':
    regression_pipelines = {
        'LinearRegression':[Pipeline([
            ('preprocessor', preprocessor_reg),
            ('classifier', LinearRegression())
        ]), get_grid_params('LinearRegression','regression')],

        'DecisionTreeRegressor':[Pipeline([
            ('preprocessor', preprocessor_reg),
            ('classifier', DecisionTreeRegressor())
        ]), get_grid_params('DecisionTreeRegressor','regression')],

        'RandomForestRegressor':[Pipeline([
            ('preprocessor', preprocessor_reg),
            ('classifier', RandomForestRegressor(n_jobs=ml_training_settings['PARALLEL_JOBS']))
        ]), get_grid_params('RandomForestRegressor','regression')],

        'XGBRegressor':[Pipeline([
            ('preprocessor', preprocessor_reg),
            ('classifier', XGBRegressor(n_jobs=ml_training_settings['PARALLEL_JOBS']))
        ]), get_grid_params('XGBRegressor','regression')]
    }
else:
    regression_pipelines = {
        'LinearRegression':[Pipeline([
            ('preprocessor', preprocessor_reg),
            ('classifier', LinearRegression())
        ]), get_grid_params('LinearRegression','regression')],

        'DecisionTreeRegressor':[Pipeline([
            ('preprocessor', preprocessor_reg),
            ('classifier', DecisionTreeRegressor())
        ]), get_grid_params('DecisionTreeRegressor','regression')],

        'RandomForestRegressor':[Pipeline([
            ('preprocessor', preprocessor_reg),
            ('classifier', RandomForestRegressor())
        ]), get_grid_params('RandomForestRegressor','regression')],

        'XGBRegressor':[Pipeline([
            ('preprocessor', preprocessor_reg),
            ('classifier', XGBRegressor())
        ]), get_grid_params('XGBRegressor','regression')]
    }




regression_pipelines = {key: value for key, value in regression_pipelines.items() if key in ml_training_settings['MODEL_LIST_TO_TEST']}





# ----------------------
# CLASSIFICATION PIPELINES
# ----------------------

if ml_training_settings['RUN_MODE'] == 'simple':
    classification_pipelines = {
        'Logistic_OVO':[Pipeline([
            ('preprocessor', preprocessor_cls_status),
            ('classifier', OneVsOneClassifier(LogisticRegression(max_iter=1000), n_jobs=ml_training_settings['PARALLEL_JOBS']))
        ]), get_grid_params('Logistic_OVO','classification_status')],

        'DecisionTree':[Pipeline([
            ('preprocessor', preprocessor_cls_status),
            ('classifier', DecisionTreeClassifier())
        ]), get_grid_params('DecisionTree','classification_status')],

        'RandomForest':[Pipeline([
            ('preprocessor', preprocessor_cls_status),
            ('classifier', RandomForestClassifier(n_jobs=ml_training_settings['PARALLEL_JOBS']))
        ]), get_grid_params('RandomForest','classification_status')],

        'Logistic_OVR':[Pipeline([
            ('preprocessor', preprocessor_cls_status),
            ('classifier', OneVsRestClassifier(LogisticRegression(max_iter=1000), n_jobs=ml_training_settings['PARALLEL_JOBS']))
        ]), get_grid_params('Logistic_OVR','classification_status')]
    }
else:
    classification_pipelines = {
        'Logistic_OVO':[Pipeline([
            ('preprocessor', preprocessor_cls_status),
            ('classifier', OneVsOneClassifier(LogisticRegression(max_iter=1000)))
        ]), get_grid_params('Logistic_OVO','classification_status')],

        'DecisionTree':[Pipeline([
            ('preprocessor', preprocessor_cls_status),
            ('classifier', DecisionTreeClassifier())
        ]), get_grid_params('DecisionTree','classification_status')],

        'RandomForest':[Pipeline([
            ('preprocessor', preprocessor_cls_status),
            ('classifier', RandomForestClassifier())
        ]), get_grid_params('RandomForest','classification_status')],

        'Logistic_OVR':[Pipeline([
            ('preprocessor', preprocessor_cls_status),
            ('classifier', OneVsRestClassifier(LogisticRegression(max_iter=1000)))
        ]), get_grid_params('Logistic_OVR','classification_status')]
    }


classification_pipelines = {key: value for key, value in classification_pipelines.items() if key in ml_training_settings['MODEL_LIST_TO_TEST']}


# ----------------------
# EXECUTION WITH LOGGING
# ----------------------

# Classification status pipelines
for name, pipeline_tuple in classification_pipelines.items():
    logger.info(f"==== Starting classification_status pipeline: {name} ====\n")
    if ml_training_settings['RUN_MODE'] in ['simple','both']:
        logger.info(f"Running simple mode for {name}")
        try:
            pipe, time_elapsed = test_pipeline_classification(name, pipeline_tuple, mode='simple', target = "status")
            output_metrics_classification(name, pipe, mode='simple', target = "status", processing_time=time_elapsed)
            logger.info(f"Simple mode completed for {name}\n")
        except Exception as e:
            logger.error(f"Error in simple mode pipeline {name}: {e}\n")
    if ml_training_settings['RUN_MODE'] in ['grid','both']:
        logger.info(f"Running grid mode for {name}")
        try:
            pipe, time_elapsed = test_pipeline_classification(name, pipeline_tuple, mode='grid', target = "status")
            output_metrics_classification(name, pipe, mode='grid', target = "status", processing_time=time_elapsed)
            logger.info(f"Grid mode completed for {name}\n")
        except Exception as e:
            logger.error(f"Error in grid mode pipeline {name}: {e}\n")
    logger.info(f"==== Finished classification_status pipeline: {name} ====\n\n")


# Classification delay pipelines
for name, pipeline_tuple in classification_pipelines.items():
    logger.info(f"==== Starting classification_delay pipeline: {name} ====\n")
    if ml_training_settings['RUN_MODE'] in ['simple','both']:
        logger.info(f"Running simple mode for {name}")
        try:
            pipe, time_elapsed = test_pipeline_classification(name, pipeline_tuple, mode='simple', target = "delay")
            output_metrics_classification(name, pipe, mode='simple', target = "delay", processing_time=time_elapsed)
            logger.info(f"Simple mode completed for {name}\n")
        except Exception as e:
            logger.error(f"Error in simple mode pipeline {name}: {e}\n")
    if ml_training_settings['RUN_MODE'] in ['grid','both']:
        logger.info(f"Running grid mode for {name}")
        try:
            pipe, time_elapsed = test_pipeline_classification(name, pipeline_tuple, mode='grid', target = "delay")
            output_metrics_classification(name, pipe, mode='grid', target = "delay", processing_time=time_elapsed)
            logger.info(f"Grid mode completed for {name}\n")
        except Exception as e:
            logger.error(f"Error in grid mode pipeline {name}: {e}\n")
    logger.info(f"==== Finished classification_delay pipeline: {name} ====\n\n")

# Regression pipelines
for name, pipeline_tuple in regression_pipelines.items():
    logger.info(f"==== Starting regression pipeline: {name} ====\n")
    if ml_training_settings['RUN_MODE'] in ['simple','both']:
        logger.info(f"Running simple mode for {name}")
        try:
            pipe, time_elapsed = test_pipeline_regression(name, pipeline_tuple, mode='simple')
            output_metrics_regression(name, pipe, mode='simple', processing_time=time_elapsed)
            logger.info(f"Simple mode completed for {name}\n")
        except Exception as e:
            logger.error(f"Error in simple mode regression pipeline {name}: {e}\n")
    if ml_training_settings['RUN_MODE'] in ['grid','both']:
        logger.info(f"Running grid mode for {name}")
        try:
            pipe, time_elapsed = test_pipeline_regression(name, pipeline_tuple, mode='grid')
            output_metrics_regression(name, pipe, mode='grid', processing_time=time_elapsed)
            logger.info(f"Grid mode completed for {name}\n")
        except Exception as e:
            logger.error(f"Error in grid mode regression pipeline {name}: {e}\n")
    logger.info(f"==== Finished regression pipeline: {name} ====\n\n")





# ----------------------
# SAVE TOP FEATURES SUMMARY
# ----------------------

if top_features_summary:
    pd.concat(top_features_summary).to_csv(f"{output_folder}/{script_start_time}_top_features_summary.csv", index=False)
    logger.info(f"Saved top {ml_training_settings['TOP_K_FEATURES']} features summary for all pipelines")


# Global summary CSVs
if classification_status_summary:
    pd.DataFrame(classification_status_summary).to_csv(f"{output_folder}/classification_status/{script_start_time}_global_classification_status_summary.csv", index=False)
    logger.info("Saved global classification_status summary for all pipelines")

if classification_delay_summary:
    pd.DataFrame(classification_delay_summary).to_csv(f"{output_folder}/classification_delay/{script_start_time}_global_classification_delay_summary.csv", index=False)
    logger.info("Saved global classification_delay summary for all pipelines")
    
if regression_summary:
    pd.DataFrame(regression_summary).to_csv(f"{output_folder}/regression/{script_start_time}_global_regression_summary.csv", index=False)
    logger.info("Saved global regression summary for all pipelines")
    

if global_summary:
    pd.DataFrame(global_summary).to_csv(f"{output_folder}/{script_start_time}_global_ml_summary.csv", index=False)
    logger.info("Saved combined global ML summary for all pipelines and modes")

    # Convert to DataFrame
    df_global = pd.DataFrame(global_summary)

    # Expand hyperparameters JSON into separate columns
    def expand_hyperparams(df, column='hyperparameters'):
        if column in df.columns:
            hp_expanded = df[column].apply(lambda x: json.loads(x) if pd.notnull(x) else {})
            hp_df = pd.json_normalize(hp_expanded)
            df = pd.concat([df.drop(columns=[column]), hp_df], axis=1)
        return df

    df_global_expanded = expand_hyperparams(df_global)

    df_global_expanded['best_pipeline'] = False

    # Best classification_status pipeline (highest accuracy)
    df_cls_status = df_global_expanded[df_global_expanded['problem_type']=='classification_status']
    if not df_cls_status.empty:
        idx_best_cls_status = df_cls_status['accuracy'].idxmax()
        df_global_expanded.loc[idx_best_cls_status, 'best_pipeline'] = True

    # Best classification_delay pipeline (highest r2)
    df_cls_delay = df_global_expanded[df_global_expanded['problem_type']=='classification_delay']
    if not df_cls_delay.empty:
        idx_best_cls_delay = df_cls_delay['accuracy'].idxmax()
        df_global_expanded.loc[idx_best_cls_delay, 'best_pipeline'] = True

    # Best regression pipeline (highest r2)
    df_reg = df_global_expanded[df_global_expanded['problem_type']=='regression']
    if not df_reg.empty:
        idx_best_reg = df_reg['r2'].idxmax()
        df_global_expanded.loc[idx_best_reg, 'best_pipeline'] = True

    df_global_expanded['timestamp'] = script_start_time

    cols = df_global_expanded.columns.tolist()
    if 'best_pipeline' in cols:
        cols.remove('best_pipeline')
        cols = ['pipeline', 'best_pipeline'] + cols[1:]  # keep 'pipeline' first, 'best_pipeline' second

    if 'timestamp' in cols:
        cols.remove('timestamp')
        cols = ['pipeline', 'timestamp'] + cols[1:] 

    df_global_expanded = df_global_expanded[cols]


    df_global_expanded.to_csv(f"{output_folder}/{script_start_time}_global_ml_summary_expanded.csv", index=False)
    logger.info("Saved enhanced global ML summary with expanded hyperparameters")

    try:
        historical_global = pd.read_csv(f"{ml_training_settings['OUTPUT_DIR']}/historical_global_ml_summary_expanded.csv")
        historical_global = pd.concat([historical_global,df_global_expanded])
        historical_global.drop_duplicates().to_csv(f"{ml_training_settings['OUTPUT_DIR']}/historical_global_ml_summary_expanded.csv", index=False)
        logger.info("Appended current enhanced global ML summary with expanded hyperparameters to historical summaries")
    except:
        historical_global = df_global_expanded
        df_global_expanded.to_csv(f"{ml_training_settings['OUTPUT_DIR']}/historical_global_ml_summary_expanded.csv", index=False)

        logger.info("Created enhanced global ML summary with expanded hyperparameters to historical summaries")


    if ml_training_settings['MODEL_TO_KEEP'] == "LATEST":


        best_model_summary_classification_status = df_global_expanded[(df_global_expanded['problem_type'] == "classification_status")]
        best_model_summary_classification_status = best_model_summary_classification_status[(best_model_summary_classification_status['accuracy'] == best_model_summary_classification_status['accuracy'].max())]
        
        best_model_summary_classification_status_filename = "_".join([best_model_summary_classification_status['problem_type'].values[0] ,best_model_summary_classification_status['timestamp'].values[0] ,best_model_summary_classification_status['pipeline'].values[0] ,best_model_summary_classification_status['mode'].values[0] ])

        best_model_summary_classification_delay = df_global_expanded[(df_global_expanded['problem_type'] == "classification_delay")]
        best_model_summary_classification_delay = best_model_summary_classification_delay[(best_model_summary_classification_delay['accuracy'] == best_model_summary_classification_delay['accuracy'].max())]
        
        best_model_summary_classification_delay_filename = "_".join([best_model_summary_classification_delay['problem_type'].values[0] ,best_model_summary_classification_delay['timestamp'].values[0] ,best_model_summary_classification_delay['pipeline'].values[0] ,best_model_summary_classification_delay['mode'].values[0] ])


        best_model_summary_regression = df_global_expanded[df_global_expanded['r2'] == df_global_expanded['r2'].max()]
        best_model_summary_regression_filename = "_".join([best_model_summary_regression['problem_type'].values[0] ,best_model_summary_regression['timestamp'].values[0] ,best_model_summary_regression['pipeline'].values[0] ,best_model_summary_regression['mode'].values[0] ])



        pd.concat([best_model_summary_classification_status,best_model_summary_classification_delay,best_model_summary_regression]).to_csv(f"{best_model_folder}/best_models.csv", index=False)

        model_files_to_delete = sorted([f for f in list(os.listdir(best_model_folder)) if ('.pkl' in f) & (best_model_summary_classification_delay_filename not in f) & (best_model_summary_regression_filename not in f) & (best_model_summary_classification_status_filename not in f)])


        for file in model_files_to_delete:
            os.remove("/".join([best_model_folder, file]))


    else :

        best_model_summary_classification_status = historical_global[(historical_global['problem_type'] == "classification_status")]
        best_model_summary_classification_status = best_model_summary_classification_status[(best_model_summary_classification_status['accuracy'] == best_model_summary_classification_status['accuracy'].max())]
        
        best_model_summary_classification_status_filename = "_".join([best_model_summary_classification_status['problem_type'].values[0] ,best_model_summary_classification_status['timestamp'].values[0] ,best_model_summary_classification_status['pipeline'].values[0] ,best_model_summary_classification_status['mode'].values[0] ])

        best_model_summary_classification_delay = historical_global[(historical_global['problem_type'] == "classification_delay")]
        best_model_summary_classification_delay = best_model_summary_classification_delay[(best_model_summary_classification_delay['accuracy'] == best_model_summary_classification_delay['accuracy'].max())]
        
        best_model_summary_regression = historical_global[historical_global['r2'] == historical_global['r2'].max()]
        best_model_summary_regression_filename = "_".join([best_model_summary_regression['problem_type'].values[0] ,best_model_summary_regression['timestamp'].values[0] ,best_model_summary_regression['pipeline'].values[0] ,best_model_summary_regression['mode'].values[0] ])


        best_model_summary_classification_delay_filename = "_".join([best_model_summary_classification_delay['problem_type'].values[0] ,best_model_summary_classification_delay['timestamp'].values[0] ,best_model_summary_classification_delay['pipeline'].values[0] ,best_model_summary_classification_delay['mode'].values[0] ])

        pd.concat([best_model_summary_classification_status,best_model_summary_classification_delay,best_model_summary_regression]).to_csv(f"{best_model_folder}/best_models.csv", index=False)

        model_files_to_delete = sorted([f for f in list(os.listdir(best_model_folder)) if ('.pkl' in f) & (best_model_summary_classification_delay_filename not in f)& (best_model_summary_regression_filename not in f) & (best_model_summary_classification_status_filename not in f)])

        for file in model_files_to_delete:
            os.remove("/".join([best_model_folder, file]))

logger.info("End of ML script\n")
