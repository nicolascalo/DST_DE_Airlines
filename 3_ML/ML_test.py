import pandas as pd
import matplotlib.pyplot as plt
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay
import seaborn as sns
import datetime


cwd = os.getcwd()
if cwd.endswith("DST_DE_Airlines"):
    os.chdir("3_ML")
elif cwd.endswith("1_data_collection"):
    os.chdir("../3_ML")
elif cwd.endswith("afklm_api_collection"):
    os.chdir("../../3_ML")
else:
    script_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_path)
    


df = pd.read_csv('afklm_flight_from_mongo_filtered_20251104-22-25-13_878683.csv')

df.columns
df.info()
df.isnull().sum().sort_values()


keywordsToDrop_all = ['id','airline_code','flightLegs_aircraft_ownerAirlineCode','actual','PositionTerminal','latestPublished']

df_cleaned = df.dropna(subset=['flightStatusPublic']).drop(list(df.filter(regex = '|'.join(keywordsToDrop_all))), axis = 1)


df_cleaned["flightLegs_scheduledFlightDuration"] = df_cleaned.apply(
lambda row: ((datetime.datetime.fromisoformat(row.flightLegs_arrivalInformation_times_scheduled) - datetime.datetime.fromisoformat(row.flightLegs_departureInformation_times_scheduled)).seconds)/60,
    axis=1
)

df_cleaned.columns
df_cleaned.info()
df_cleaned.isnull().sum()
df_cleaned.isnull().sum().sort_values()


keywordsToDrop_status = ['delayDuration',]


df_status = df_cleaned.dropna(subset=['flightStatusPublic']).drop(list(df.filter(regex = '|'.join(keywordsToDrop_status))), axis = 1)


keywordsToDrop_delay = ['']


df_delay = df_cleaned.dropna(subset=['flightLegs_irregularity_delayDuration_total']).drop(list(df.filter(regex = '|'.join(keywordsToDrop_delay))), axis = 1)
df_delay = df_delay[df_delay['flightLegs_irregularity_delayDuration_total'] > 0]
df_delay.info()
df_delay.isnull().sum()
df_delay = df_delay.dropna()
df_delay.info()
df_delay.isnull().sum()



df.count()

df.isnull().sum()

df = df.dropna()
df.info()

df.columns

X = df.drop(columns=['RainToday'], axis=1)
y = df['RainToday']

print(y.value_counts())


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)


numeric_features = X_train.select_dtypes(include=['number']).columns.tolist()  
categorical_features = X_train.select_dtypes(include=['object', 'category']).columns.tolist()


numerical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ]
)


pipeline = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', RandomForestClassifier(random_state=42))
])


param_grid = {
    'classifier__n_estimators': [50, 100],
    'classifier__max_depth': [None, 10, 20],
    'classifier__min_samples_split': [2, 5]
}

cv = StratifiedKFold(n_splits=5, shuffle=True)

grid_search = GridSearchCV(pipeline, param_grid, cv=cv, scoring='accuracy', verbose=2)  
grid_search.fit(X_train, y_train)

print("\nBest parameters found: ", grid_search.best_params_)
print("Best cross-validation score: {:.2f}".format(grid_search.best_score_))












