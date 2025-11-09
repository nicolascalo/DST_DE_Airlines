import pandas as pd
import matplotlib.pyplot as plt
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.multiclass import OneVsOneClassifier
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay, mean_squared_error, r2_score, mean_absolute_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn import linear_model
from xgboost import XGBRegressor
from sklearn.multiclass import OneVsRestClassifier

import seaborn as sns
import datetime
import os
import re
import numpy as np
import logging

from multiprocessing import Pool
from multiprocessing import cpu_count
from functools import partial

import pickle


logger = logging.getLogger(__name__)
handler = logging.FileHandler('ML.log')
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel('INFO')

logger.info("Starting ML script")

top30euAirports = ['CDG','AMS','ORY','FCO','LHR','CPH','MAD','ARN','OSL','LIN','NCE','BCN','LYS','BGO','LIS','DUB','HEL','TLS','OTP','FRA','MRS','ATH','PMI','MUC','TRD','MAN','BER','AGP','OPO']

euAirports = ['BHX','BOH','BRS','EXT','HUY','LBA','LPL','LGW','LHR','LCY','SEN','STN','LTN','MAN','MME','NCL','NQY','NWI','EMA','SOU','BFS','BHD','LDY','ABZ','EDI','GLA','PIK','INV','CWL','ANR','BRU','CRL','LGG','OST','AJA','BIA','BVA','EGC','BZR','BIQ','BOD','BES','CCF','XCR','CMF','DNR','FSC','GNB','LRH','LIL','LIG','LYS','MRS','BSL','NTE','NCE','FNI','CDG','ORY','PUF','PGF','PIS','RDZ','EBU','SXB','TLN','TLS','TUF','GIB','ORK','DUB','KIR','NOC','SNN','IOM','JER','LUX','AMS','EIN','GRQ','MST','RTM','GRZ','KLU','INN','LNZ','SZG','VIE','BRQ','JCL','KLV','OSR','PED','PRG','FKB','BER','BRE','CGN','DTM','DUS','FRA','HHN','FDH','HAM','HAJ','LEJ','LBC','FMM','MUC','NUE','STR','NRN','BUD','DEB','SOB','BZG','GDN','KTW','KRK','LUZ','LCJ','SZY','POZ','RZE','SZZ','WAW','WMI','RDO','WRO','BTS','KSC','PZY','TAT','ILZ','BSL','BRN','GVA','LUG','ACH','ZRH','BWK','DBV','LSZ','OSI','PUY','RJK','SPU','ZAD','ZAG','ATH','EFL','CHQ','JKH','CFU','HER','KLX','AOK','KVA','KGS','JMK','MJT','PVK','RHO','SMI','JTR','JSI','SKU','SKG','VOL','ZTH','AHO','AOI','BRI','BGY','BLQ','VBS','BDS','CAG','CTA','CUF','FLR','GOA','SUF','LIN','MXP','NAP','OLB','PMO','PMF','PEG','PSR','PSA','RMI','FCO','CIA','QSR','TPS','TRS','TRN','VCE','VRN','MLA','BYJ','FAO','FNC','LIS','PDL','OPO','PXO','TER','LJU','MBX','POW','LCG','ALC','LEI','OVD','BCN','BIO','CDT','FUE','GRO','LPA','GRX','HSK','IBZ','XRY','SPC','ACE','ILD','MAD','AGP','MAH','RMU','PMI','PNA','REU','SDR','SCQ','SVQ','TFN','TFS','VLC','VLL','VGO','VIT','ZAZ','TIA','GNA','GME','MSQ','BNX','OMO','SJJ','TZL','BOJ','PDV','SOF','VAR','PRN','RMO','ARW','BCM','BAY','GHV','OTP','BBU','CLJ','CND','CRA','IAS','OMR','SUJ','SBZ','SCV','TGM','TSR','TGD','TIV','OHD','SKP','ABA','DYR','AAQ','ARH','ASF','BAX','EGO','BQS','BTK','BZK','CSY','CEK','CEE','HTA','ESL','GRV','IKT','KGD','KZN','KHV','KXK','KRR','KJA','URS','GDX','MQF','MCX','MRV','DME','ZIA','SVO','VKO','MMK','NAL','NBC','NJC','GOJ','NOZ','OVB','OMS','REN','OSW','PEE','PES','PVS','PKC','PKV','ROV','LED','KUF','GSV','AER','STW','SGC','SCW','TOF','TJM','UUD','ULV','UFA','VVO','OGZ','VOG','VOZ','YKS','IAR','SVX','UUS','BEG','KVO','INI','CWC','IFO','HRK','KWG','KBP','IEV','LWO','NLV','ODS','PLV','SIP','UDJ','OZH','AAL','AAR','BLL','CPH','EPU','TLL','TAY','FAE','MHQ','HEL','KTT','KUO','KAO','LPP','OUL','RVN','SVL','TMP','TKU','VAA','AEY','EGS','KEF','RKV','RIX','VNT','KUN','PLQ','SQQ','VNO','AES','BGO','BOO','HAU','KRS','KSU','OSL','TRF','SVG','TOS','TRD','GOT','LLA','MMX','NRK','OSD','ARN','BMA','NYO','VST','SDL','UME','VXO','VBY']


cwd = os.getcwd()
if cwd.endswith("DST_DE_Airlines"):
    os.chdir("3_ML")
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

file_list = os.listdir(cwd)
csv_to_import = [val for val in file_list if 'afklm_flight_from_mongo_filtered' in val]

csv_to_import.sort()
csv_to_import = csv_to_import[-1]



df = pd.read_csv(csv_to_import)

df.columns
df.info()
df.isnull().sum().sort_values()



df["company_flight"] = df.apply(
    lambda row: re.sub("^.*?\\+","", row["id"]),

    axis=1)


keywordsToDrop_all = ['id','airline_name','flightLegs_aircraft_ownerAirlineCode','actual','PositionTerminal','latestPublished']
df_cleaned = df.dropna(subset=['flightStatusPublic']).drop(list(df.filter(regex = '|'.join(keywordsToDrop_all))), axis = 1)





df_cleaned["scheduled"] = df_cleaned.apply(
    lambda row: 
    
    True if (datetime.datetime.now().date() - datetime.datetime.fromisoformat(row["flightLegs_arrivalInformation_times_scheduled"]).date()).days < 0
    else False,
    axis=1,
)

df_cleaned["flightLegs_scheduledFlightDuration"] = df_cleaned.apply(
lambda row: ((datetime.datetime.fromisoformat(row.flightLegs_arrivalInformation_times_scheduled) - datetime.datetime.fromisoformat(row.flightLegs_departureInformation_times_scheduled)).seconds)/60,
    axis=1
)



df_cleaned_past = df_cleaned[df_cleaned["scheduled"] == False].copy(deep=True).drop('scheduled',axis=1)


df_cleaned_past["flightLegs_irregularity_delayDuration_total"] = df_cleaned_past["flightLegs_irregularity_delayDuration_total"].fillna(0)


df_cleaned_past["flightLegs_Category"] = df_cleaned.apply(
    lambda row: 
    
    'late' if row["flightLegs_irregularity_delayDuration_total"] > 0
    else 'on_time',
    axis=1,
)


df_cleaned_past["flightLegs_Category"] = df_cleaned_past.apply(
    lambda row: 
    'cancelled' if  row["flightLegs_legStatusPublic"] == 'CANCELLED'
    else row["flightLegs_Category"],
    axis=1,
)

df_cleaned_past["flightLegs_Category"].value_counts()



df_cleaned_past.columns
df_cleaned_past.info()
df_cleaned_past.isnull().sum()
df_cleaned_past.isnull().sum().sort_values()





keywordsToDrop_status = ['delay','country_code','flightNumber','flightLegs_legStatusPublic','airline_name','flightLegs_serviceType','status','Status','estimated']
df_status = df_cleaned_past.drop(list(df.filter(regex = '|'.join(keywordsToDrop_status))), axis = 1,errors='ignore')


df_status.info()
df_status.isnull().sum()
df_status.isnull().sum().sort_values()

df_status = df_status.dropna()

df_status.info()
df_status.isnull().sum()
df_status.isnull().sum().sort_values()


df_status_euFromTo_single_airport_top30eu = df_status[(df_status['flightLegs_arrivalInformation_airport_code'].isin(top30euAirports))|(df_status['flightLegs_departureInformation_airport_code'].isin(top30euAirports))]

df_status_euFromTo_single_airport_top30eu_euonly = df_status[(df_status['flightLegs_arrivalInformation_airport_code'].isin(euAirports)) & (df_status['flightLegs_departureInformation_airport_code'].isin(euAirports))]





X = df_status_euFromTo_single_airport_top30eu_euonly.drop(columns=['flightLegs_Category'], axis=1)
y = df_status_euFromTo_single_airport_top30eu_euonly['flightLegs_Category']



y.value_counts()




X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)


numeric_features = X_train.select_dtypes(include=['number']).columns.tolist()  
categorical_features = X_train.select_dtypes(include=['object', 'category']).columns.tolist()


numerical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore',drop = "first",sparse_output=False))
])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore',drop = "first",sparse_output=True))
])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ]
)







def test_pipeline_classification(pipeline_descriptor:str,pipeline,  test_type:str) -> dict:
    
    logger.info(f"starting pipeline {pipeline_descriptor} {test_type}")
    start_time = datetime.datetime.now()
    
    if test_type == 'GridSearchCV':
        cv = StratifiedKFold(n_splits=5, shuffle=True)
        param_grid = dict(pipeline[1])    
        pipeline_settings = pipeline[0]
        pipeline = GridSearchCV(pipeline_settings, param_grid, cv=cv, scoring='accuracy', verbose=2) 
        test = GridSearchCV(pipeline_settings, param_grid, cv=cv, scoring='accuracy', verbose=2) 
        test.fit(X_train, y_train)
    else:
        pipeline = pipeline[0]
    
    try:
        pipeline.fit(X_train, y_train)
    except ValueError:
        print(e)
        logger.exception(e)
        return {pipeline_descriptor:'FAILED'}
        
    
    end_time = datetime.datetime.now()
    time_elapsed = (end_time- start_time).seconds
    logger.info(f"pipeline duration: {time_elapsed} seconds")

    if test_type == 'GridSearchCV':
        logger.info(f"Best parameters found: { pipeline.best_params_}")
        logger.info(f"Best cross-validation score: { pipeline.best_score_}")
        file_suffix = "_grid"
    
    else:
        pipeline_score = pipeline.score(X_test, y_test)
        logger.info(f"pipeline score: {pipeline_score}")
        file_suffix = ""


    logger.info(f"Exporting model {pipeline_descriptor}")
    with open(f'{pipeline_descriptor}{file_suffix}.pkl','wb') as f:
       pickle.dump(pipeline,f)
    logger.info(f"Export of model {pipeline_descriptor}{file_suffix} over")

    return {pipeline_descriptor:pipeline}




    
def output_metrics_classification(pipeline_descriptor,pipeline):
    
    logger.info(f"Outputing classification metrics for {pipeline_descriptor}")
    
    plt.rcParams.update({
    'font.size': 60,           # Default font size
    'axes.titlesize': 90,      # Title font size
    'axes.labelsize': 60,      # X/Y label font size
    'xtick.labelsize': 60,     # X tick labels
    'ytick.labelsize': 60,     # Y tick labels
    })
    
    try:
        pipeline = pipeline.best_estimator_
        pipeline_descriptor = pipeline_descriptor + "_bestEstimator"
    except:
        pass
    
    y_pred = pipeline.predict(X_test)

    #feature_names = pipeline['preprocessor'].transformers_[1][1]['onehot'].get_feature_names_out(categorical_features)
    
    feature_names = pipeline[:-1].get_feature_names_out()
    
    try:
        
        df = pd.DataFrame()
        df['feature'] = feature_names
        df['feature_importances'] = pipeline['classifier'].feature_importances_
        
        df.to_csv(f'{pipeline_descriptor}_featureImportance.csv')

    except Exception as e: 
        print(e)
        logger.exception(e)
        return {pipeline_descriptor:'FAILED'}

    
    logger.info(f"Saving confusion matrix of {pipeline_descriptor}_grid")
    
    f,ax = plt.subplots(1,1,figsize=(60,60))
    ConfusionMatrixDisplay.from_predictions(y_test, y_pred, ax=ax,
                              display_labels=pipeline.classes_)

    plt.title(pipeline_descriptor)

    plt.savefig(f'{pipeline_descriptor}_confusionMatrix.png')

    report = classification_report(y_test, y_pred, output_dict=True)
    df = pd.DataFrame(report).transpose()
    df.to_csv(f'{pipeline_descriptor}_classification_report.csv')
    
    
    try:
        
        plt.figure(figsize=(150,40))  # set plot size (denoted in inches)
        plot_tree(pipeline['classifier'],
                  filled=True,
                  class_names=pipeline['classifier'].classes_,
                  #class_names= True,
                  feature_names=feature_names,
                  fontsize=30
                  )
        plt.savefig(f'{pipeline_descriptor}_decisionTree.png')
    except Exception as e: 
        print(e)
        logger.exception(e)

        
        

        






    
def output_metrics_regression(pipeline_descriptor,pipeline):
    
    logger.info(f"Outputing regression metrics for {pipeline_descriptor}")
    
    y_pred = pipeline.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    
    
    std_y = np.std(y_test)
    
    
    logger.info(f"Mean absolute error (MAE): {mae}")
    logger.info(f"Mean squared error (MSE): {mse}")
    logger.info(f"Root mean squared error (rMSE): {rmse}")
    logger.info(f"R2-score: {r2}")
    
    pd.DataFrame.from_dict({'pipeline_descriptor':pipeline_descriptor,'mae':[mae],'mse':[mse],'rmse':[rmse],'r2':[r2]}).to_csv(f'{pipeline_descriptor}_regression_report.csv')

    
    
    plt.figure(figsize=(100, 60))


    plt.scatter(y_test, y_pred, alpha=0.5, color="blue",ec='k')
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2,label="perfect model")
    plt.plot([y_test.min(), y_test.max()], [y_test.min() + std_y, y_test.max() + std_y], 'r--', lw=1, label="+/-1 Std Dev")
    plt.plot([y_test.min(), y_test.max()], [y_test.min() - std_y, y_test.max() - std_y], 'r--', lw=1, )

    plt.title(f"{pipeline_descriptor} Predictions vs Actual")
    plt.xlabel("Actual Values")
    plt.ylabel("Predicted Values")
    plt.legend()

    plt.savefig(f'{pipeline_descriptor}_yTestPredictedVSactual.png')
    


pipeline_list_status_simple = {
'pipeline_DecisionTreeClassifier_3' : [Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier',  DecisionTreeClassifier(criterion="entropy", max_depth = 3))
]),
{}
],
'pipeline_DecisionTreeClassifier_4' :[ Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier',  DecisionTreeClassifier(criterion="entropy", max_depth = 4))
]),
{}
],
'pipeline_OneVsOneClassifier_LogisticRegression' : [Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier',  OneVsOneClassifier(LogisticRegression(max_iter=1000, n_jobs=10)))
]),
{}
],


'pipeline_OneVsRestClassifier_LogisticRegression' : [Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier',  OneVsRestClassifier(LogisticRegression( max_iter=1000, n_jobs=10)))
]),
{} 
]
}

test_results_pipeline_list_status_simple = {}

for pipeline_descriptor, pipeline in pipeline_list_status_simple.items():
    print(pipeline_descriptor)
    test_results_pipeline_list_status_simple.update(test_pipeline_classification(pipeline_descriptor,pipeline,'simple'))

for pipeline_descriptor,pipeline in test_results_pipeline_list_status_simple.items():
    output_metrics_classification(pipeline_descriptor,pipeline)



pipeline_list_status_grid = {

    'pipeline_DecisionTreeClassifier': [
        Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', DecisionTreeClassifier())
        ]),
        {
            'classifier__criterion': ['gini', 'entropy', 'log_loss'],
            'classifier__splitter': ['best', 'random'],
            'classifier__max_depth': [2*n for n in range(1,10)],
            'classifier__max_features': ['log2', 'sqrt', None],
            'classifier__min_samples_leaf': [1, 2, 4],
            'classifier__min_samples_split': [2, 5, 10]
        }
    ],

    'pipeline_OneVsOneClassifier_LogisticRegression': [
        Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', OneVsOneClassifier(LogisticRegression(max_iter=1000), n_jobs=10))
        ]),
        {
            'classifier__estimator__C': [0.01, 0.1, 1],
            'classifier__estimator__penalty': ['l2'],  # safe choice
            'classifier__estimator__solver': ['liblinear', 'lbfgs'],
            'classifier__estimator__class_weight': [None, 'balanced']
        }
    ],

    'pipeline_OneVsRestClassifier_LogisticRegression': [
        Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', OneVsRestClassifier(LogisticRegression(max_iter=1000), n_jobs=10))
        ]),
        {
            'classifier__estimator__C': [0.01, 0.1, 1],
            'classifier__estimator__penalty': ['l2'],
            'classifier__estimator__solver': ['liblinear', 'lbfgs'],
            'classifier__estimator__class_weight': [None, 'balanced']
        }
    ],

    'pipeline_RandomForestClassifier': [
        Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', RandomForestClassifier(random_state=42, n_jobs=10))
        ]),
        {
            'classifier__n_estimators': [50, 100],
            'classifier__max_depth': [None, 10, 20],
            'classifier__min_samples_split': [2, 5]
        }
    ]
}


                         
results_pipeline_list_status_grid = {}

for pipeline_descriptor, pipeline in pipeline_list_status_grid.items():
    print(pipeline_descriptor)
    print(pipeline)
    
    pipeline_descriptor = pipeline_descriptor
    pipeline = pipeline
    results_pipeline_list_status_grid.update(test_pipeline_classification(pipeline_descriptor,pipeline,'GridSearchCV'))


for pipeline_descriptor,pipeline in results_pipeline_list_status_grid.items():
    print(pipeline_descriptor)
    print(pipeline)
    
    output_metrics_classification(pipeline_descriptor,pipeline)





df_delay = df_cleaned_past[df_cleaned_past['flightLegs_Category'] == 'late']


keywordsToDrop_delay = ['country_code','flightNumber','flightLegs_legdelayPublic','airline_name','flightLegs_serviceType','estimated','irregularity_delayInformation','flightLegs_Category','flightStatusPublic','status','Status','flightLegs_irregularity_delayReason']

df_delay = df_delay.drop(list(df.filter(regex = '|'.join(keywordsToDrop_delay))), axis = 1,errors='ignore')

df_delay = df_delay.drop('flightLegs_Category', axis = 1)

df_delay.info()
df_delay.isnull().sum()
df_delay.isnull().sum().sort_values()

df_delay = df_delay.dropna()

df_delay.info()
df_delay.isnull().sum()
df_delay.isnull().sum().sort_values()



df_delay_euFromTo_single_airport_top30eu = df_delay[(df_delay['flightLegs_arrivalInformation_airport_code'].isin(top30euAirports))|(df_delay['flightLegs_departureInformation_airport_code'].isin(top30euAirports))]

df_delay_euFromTo_single_airport_top30eu_euonly = df_delay[(df_delay['flightLegs_arrivalInformation_airport_code'].isin(euAirports)) & (df_delay['flightLegs_departureInformation_airport_code'].isin(euAirports))]





X = df_delay_euFromTo_single_airport_top30eu_euonly.drop(columns=['flightLegs_irregularity_delayDuration_total'], axis=1)
y = df_delay_euFromTo_single_airport_top30eu_euonly['flightLegs_irregularity_delayDuration_total']

y.value_counts()




X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


numeric_features = X_train.select_dtypes(include=['number']).columns.tolist()  
categorical_features = X_train.select_dtypes(include=['object', 'category']).columns.tolist()


numerical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore',drop = "first",sparse_output=False))
])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore',drop = "first",sparse_output=True))
])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ]
)


pipeline_list_delay = {
'pipeline_LinearRegression' : [Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier',  linear_model.LinearRegression())
]),
{'copy_X': [True,False],
 'fit_intercept': [True,False], 
 'n_jobs': [1,5,10,15,None],
 'positive': [True,False]}                                     ],



'pipeline_DecisionTreeRegressor' : [Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier',  DecisionTreeRegressor(criterion = 'squared_error',
                               max_depth=8, 
                               random_state=42))
]),
{'classifier__criterion': ['gini', 'entropy'],
     'classifier__splitter': ['best', 'random'],
     'classifier__max_depth': [2*n for n in range(1,10)],
     'classifier__max_features': ['auto', 'sqrt'],
     'classifier__min_samples_leaf': [1, 2, 4],
     'classifier__min_samples_split': [2, 5, 10]}                                               ],


'pipeline_XGBRegressor' : [Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier',  XGBRegressor(n_estimators=100, random_state=42, n_jobs=10))
]),
{
    'classifier__max_depth': [3, 5, 7],
    'classifier__learning_rate': [0.1, 0.01, 0.001],
    'classifier__subsample': [0.5, 0.7, 1]
}                                                ],

}



test_results_pipeline_list_delay_simple = {}

for pipeline_descriptor, pipeline in pipeline_list_delay.items():
    print(pipeline_descriptor)
    test_results_pipeline_list_delay_simple.update(test_pipeline_classification(pipeline_descriptor,pipeline,'simple'))

for pipeline_descriptor,pipeline in test_results_pipeline_list_delay_simple.items():
    output_metrics_regression(pipeline_descriptor,pipeline)





test_results_pipeline_list_delay_grid = {}

for pipeline_descriptor, pipeline in pipeline_list_delay.items():
    print(pipeline_descriptor)
    test_results_pipeline_list_delay_simple.update(test_pipeline_classification(pipeline_descriptor,pipeline,'GridSearchCV'))

for pipeline_descriptor,pipeline in test_results_pipeline_list_delay_grid.items():
    output_metrics_regression(pipeline_descriptor,pipeline)







pipeline_list_status_rfc = {

'pipeline_RandomForestClassifier' : [Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', RandomForestClassifier(random_state=42, n_jobs = 10))
]),
{
    'n_estimators': [50, 100],
    'max_depth': [None, 10, 20],
    'min_samples_split': [2, 5]
}                                                    ]

}


for pipeline_descriptor, pipeline in pipeline_list_status_rfc.items():
    print(pipeline_descriptor)
    test_pipeline_classification(pipeline,pipeline_descriptor)


for pipeline_descriptor, pipeline in pipeline_list_status_rfc.items():
    print(pipeline_descriptor)
    test_pipeline_classification_grid(pipeline,pipeline_descriptor)





pipeline_list_delay_rfc = {
'pipeline_RandomForestRegressor' : [Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', RandomForestRegressor(n_estimators = 100, random_state=42, n_jobs=10))
]),
{
    'n_estimators': [50, 100],
    'max_depth': [None, 10, 20],
    'min_samples_split': [2, 5]
}                                                    ]

}




for pipeline_descriptor, pipeline in pipeline_list_delay_rfc = {
.items():
    print(pipeline_descriptor)
    test_pipeline_classification(pipeline,pipeline_descriptor)



for pipeline_descriptor, pipeline in pipeline_list_delay_rfc = {
.items():
    print(pipeline_descriptor)
    test_pipeline_classification_grid(pipeline,pipeline_descriptor)






logger.info("End of ML script")