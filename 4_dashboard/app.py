from dash import Dash, dcc, html, Input, Output, callback, dash_table
from dash.dash_table import DataTable
import pandas as pd
import requests, json
from sqlalchemy import create_engine
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

ml_api_host = os.getenv('ML_API_HOST')
ml_api_port = os.getenv('ML_API_PORT')
metrics_route = os.getenv('ML_METRICS_ROUTE')
prediction_route = os.getenv('ML_PRED_ROUTE')

# ====================================================
# 1. Chargement des métriques modèle depuis l’API ML
# ====================================================

try:
    response = requests.get(f"http://{ml_api_host}:{ml_api_port}{metrics_route}")
    print(response)
    model_metrics_dict = response.json()
    model_metrics = pd.DataFrame(model_metrics_dict).drop(['mode','best_pipeline',"processing_time","target_variable","numeric_features","categorical_features","hyperparameters","macro_avg_precision","macro_avg_recall","macro_avg_f1","mae","mse","rmse"],axis=1,errors="ignore")


    model_metrics = model_metrics.loc[:, ["pipeline","problem_type","dataset_size_training","dataset_size_testing","accuracy","r2"]] 

    print("Model metrics loaded")

except:
    response = "Issue when fetching the model metrics" 
    model_metrics = pd.DataFrame({"Error":"Issue when fetching the model metrics"}, index=[0])

# ======================================================
# 2. Connexion base PostgreSQL & récupération des vols
# ======================================================

username = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_URI')
port = os.getenv('POSTGRES_PORT')
database_name = os.getenv('POSTGRES_DB')

DATABASE_URL = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(DATABASE_URL)

query = " select v_future_flight.flight_id,  v_future_flight.flightNumber,   v_future_flight.airline_name,    v_future_flight.flightLegs_aircraft_typeCode,  v_future_flight.flightLegs_serviceTypeName , v_geod.flightLegs_depInfo_airport_Continent_Name,  v_geod.flightLegs_depInfo_airport_Subcontinent_Name,    v_geod.flightLegs_depInfo_airport_Country_Name,    v_geod.flightLegs_depInfo_airport_Airport_Name, v_future_flight.flightLegs_depInfo_airport_code, v_future_flight.flightLegs_depInfo_times_scheduled_date, v_future_flight.flightLegs_depInfo_times_scheduled_time, v_future_flight.flightLegs_depInfo_times_scheduled_year, v_future_flight.flightLegs_depInfo_times_scheduled_month, v_future_flight.flightLegs_depInfo_times_scheduled_day , v_future_flight.flightLegs_depInfo_times_scheduled_timezone, v_geoa.flightLegs_arrInfo_airport_Continent_Name,  v_geoa.flightLegs_arrInfo_airport_Subcontinent_Name,    v_geoa.flightLegs_arrInfo_airport_Country_Name,   v_geoa.flightLegs_arrInfo_airport_Airport_Name,      v_future_flight.flightLegs_arrInfo_airport_code, v_future_flight.flightLegs_arrInfo_airport_places_arrivalPositionTerminal, v_future_flight.flightLegs_arrInfo_times_scheduled_date, v_future_flight.flightLegs_arrInfo_times_scheduled_time, v_future_flight.flightLegs_arrInfo_times_scheduled_year, v_future_flight.flightLegs_arrInfo_times_scheduled_month, v_future_flight.flightLegs_arrInfo_times_scheduled_day, v_future_flight.flightLegs_arrInfo_times_scheduled_timezone from v_future_flight  INNER JOIN v_geod v_geod ON v_geod.flightLegs_depInfo_airport_Iata_Code = v_future_flight.flightLegs_depInfo_airport_code    INNER JOIN v_geoa v_geoa ON v_geoa.flightLegs_arrInfo_airport_Iata_Code = v_future_flight.flightLegs_arrInfo_airport_code WHERE flightLegs_depInfo_times_scheduled_date >= CURRENT_DATE;"

#df = pd.read_csv('afklm_flight_from_mongo_filtered_20251113-21-36-51_test.csv', low_memory=False)
try :
    def get_sql_data(query):
        df = pd.read_sql(query, engine)
        return df

    df = get_sql_data(query)
    print("PostreSQL data retrieved")
except Exception as e:
    prediction_status = "Issues with the PostreSQL query"
    raise RuntimeError(f"STATUS MODEL ERROR: {e}")            

# ============================
# 3. Nettoyage des colonnes
# ============================

columns_new = df.columns.copy(deep=True)
columns_new = [w.replace('flightlegs_', '') for w in columns_new]
columns_new = [w.replace('info_times', '') for w in columns_new]
columns_new = [w.replace('info_airport', '') for w in columns_new]
columns_new = [w.replace('scheduled_', '') for w in columns_new]
columns_new = [w.replace('_depposterm', '') for w in columns_new]

df.columns = columns_new

df = df[df['servicetypename'] != 'Service operated by Surface Vehicle']
df = df.drop(['servicetypename'],axis=1,errors='ignore')

#df = df.dropna(axis=1, how='all')

df['id'] = df['flight_id']
df.set_index('id', inplace=True, drop=False)

# ===============================================
# 4. App Dash + Layout : sidebar push + thème
# ===============================================

app = Dash(__name__)

app.layout = html.Div(
    id="app-root",
    className="theme-dark", # thème sombre par défaut
    children=[
        # ----- SIDEBAR (push) -----
        html.Div(
            id="sidebar",
            className="sidebar",
            children=[
                # Bouton avion (toujours présent)
                html.Button(
                    "✈",
                    id="sidebar-toggle",
                    className="sidebar-toggle",
                    n_clicks=0,
                ),

                # Contenu de la barre (masqué quand collapsed)
                html.Div(
                    className="sidebar-inner",
                    children=[
                        html.Div(
                            className="sidebar-header",
                            children=[
                                html.Div(
                                    className="sidebar-titles",
                                    children=[
                                        html.Div(
                                            "Prédiction des retards",
                                            className="sidebar-title",
                                        ),
                                        html.Div(
                                            "AF–KLM",
                                            className="sidebar-subtitle",
                                        ),
                                    ],
                                ),
                            ],
                        ),

                        # Menu principal
                        html.Div("API", className="sidebar-section-title"),
                        html.Ul(
                            className="sidebar-links",
                            children=[
                                html.Li(
                                    html.A(
                                        "MongoDB", 
                                        href="http://localhost:8000",
                                        target="_blank"
                                        )
                                ),
                                html.Li(
                                    html.A(
                                        "Machine Learning", 
                                        href="http://localhost:8001",
                                        target="_blank"
                                        )
                                ),  
                                html.Li(
                                    html.A(
                                        "pgAdmin",
                                        href="http://localhost:5050",
                                        target="_blank"
                                        )
                                ),                                
                            ],
                        ),

                        # Documentation (texte statique)
                        html.Div("Documentation", className="sidebar-section-title"),
                        html.Ul(
                            className="sidebar-links",
                            children=[
                                html.Li(
                                    html.A(
                                        "Récolte des données", 
                                        href="/assets/docs/recolte.pdf", 
                                        target="_blank"
                                    )
                                ),
                                html.Li(
                                    html.A(
                                        "Architecture des données", 
                                        href="/assets/docs/architecture.pdf", 
                                        target="_blank"
                                    )
                                ),
                                html.Li(
                                    html.A(
                                        "Consommation de la donnée", 
                                        href="/assets/docs/consommation.pdf", 
                                        target="_blank"
                                    )
                                ),
                            ],
                        ),

                        # Liens utiles
                        html.Div("Liens utiles", className="sidebar-section-title"),
                        html.Ul(
                            className="sidebar-links",
                            children=[
                                html.Li(
                                    html.A(
                                        "GitHub du projet",
                                        href="https://github.com/nicolascalo/DST_DE_Airlines",
                                        target="_blank",
                                    )
                                ),
                                html.Li(
                                    html.A(
                                        "Datascientest",
                                        href="https://datascientest.com/formation-data-engineer",
                                        target="_blank",
                                    )
                                ),
                            ],
                        ),

                        # Auteurs
                        html.Div("Auteurs", className="sidebar-section-title"),
                        html.Ul(
                            className="sidebar-links",
                            children=[
                                html.Li("Nicolas Calo"),
                                html.Li("Johan Cloos"),
                                html.Li("Rathana Lat"),
                                html.Li("Younes Es-soualhi"),
                                html.Li("Youssef Znati"),
                            ],
                        ),

                        # Sélecteur de thème
                        html.Div(
                            className="sidebar-bottom",
                            children=[
                                html.Div("Thème", className="theme-label"),
                                dcc.RadioItems(
                                    id="theme-selector",
                                    options=[
                                        {"label": "Sombre", "value": "dark"},
                                        {"label": "Clair", "value": "light"},
                                    ],
                                    value="dark",
                                    className="theme-radio",
                                    inputClassName="theme-radio-input",
                                    labelClassName="theme-radio-label",
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),

        # ----- CONTENU PRINCIPAL -----
        html.Div(
            id="main-content",
            className="main-content expanded", # "expanded" = sidebar ouverte
            children=[
                # Bandeau haut
                html.Div(
                    className="main-header",
                    children=[
                        html.Div(
                            children=[
                                html.H1(
                                    "Prédiction de retard des vols AF–KLM",
                                    className="main-title",
                                ),
                                html.P(
                                    "Sélectionnez un vol dans le tableau ci-dessous pour obtenir une prédiction de retard.",
                                    className="main-subtitle",
                                ),
                            ]
                        ),
                    ],
                ),
                html.Hr(className="section-separator"),

                # Cartes avec tables
                html.Div(
                    className="cards-container",
                    children=[
                        html.Div(
                            className="card",
                            children=[
                                html.H2("Flights", className="card-title"),
                                html.P(
                                    "Liste des vols à venir issus de la base PostgreSQL.",
                                    className="card-desc",
                                ),
                                DataTable(
                                    id="datatable-row-ids",
                                    columns=[
                                        {"name": i, "id": i, "deletable": False}
                                        for i in df.columns
                                        if i != "id"
                                    ],
                                    data=df.to_dict("records"),
                                    editable=False,
                                    filter_action="native",
                                    sort_action="native",
                                    sort_mode="multi",
                                    filter_options={"case": "insensitive"},
                                    row_deletable=False,
                                    selected_rows=[],
                                    page_action="native",
                                    page_current=0,
                                    page_size=25,
                                ),
                            ],
                        ),
                        html.Div(
                            className="card",
                            children=[
                                html.H2("Model metrics", className="card-title"),
                                html.P(
                                    "Performances comparées des modèles de Machine Learning entraînés.",
                                    className="card-desc",
                                ),
                                DataTable(
                                    id="datatable-metrics",
                                    columns=[
                                        {"name": i, "id": i, "deletable": False}
                                        for i in model_metrics.columns
                                        if i != "id"
                                    ],
                                    data=model_metrics.to_dict("records"),
                                    editable=False,
                                    sort_action="native",
                                    sort_mode="multi",
                                    filter_options={"case": "insensitive"},
                                    row_deletable=False,
                                    selected_rows=[],
                                    page_action="native",
                                    page_current=0,
                                    page_size=25,
                                ),
                            ],
                        ),
                    ],
                ),

                html.Hr(className="section-separator"),

                # Bloc de prédiction (rempli par le callback existant)
                html.Div(
                    id="datatable-row-ids-container",
                    className="prediction-wrapper",
                ),
            ],
        ),
    ],
)

# =====================================================
# 5. Callbacks + nouveaux callbacks (thème / sidebar)
# =====================================================

@callback(
    Output('datatable-row-ids-container', 'children'),
    Input('datatable-row-ids', 'derived_virtual_row_ids'),
    Input('datatable-row-ids', 'selected_row_ids'),
    Input('datatable-row-ids', 'active_cell'))
def update_graphs(row_ids, selected_row_ids, active_cell):
    # When the table is first rendered, `derived_virtual_data` and
    # `derived_virtual_selected_rows` will be `None`. This is due to an
    # idiosyncrasy in Dash (unsupplied properties are always None and Dash
    # calls the dependent callbacks when the component is first rendered).
    # So, if `rows` is `None`, then the component was just rendered
    # and its value will be the same as the component's dataframe.
    # Instead of setting `None` in here, you could also set
    # `derived_virtual_data=df.to_rows('dict')` when you initialize
    # the component.
    selected_id_set = set(selected_row_ids or [])

    if row_ids is None:
        dff = df
        # pandas Series works enough like a list for this to be OK
        row_ids = df['id']
    else:
        dff = df.loc[row_ids]

    active_row_id = active_cell["row_id"] if active_cell else None

    if active_row_id is None:
        return html.Div(
            className="prediction-block",
            children=html.P(
                "Cliquez sur un vol dans le tableau pour afficher la prédiction de retard."
            ),
        )

    # Requête SQL ciblée sur le vol sélectionné
    query = f"select *  from v_future_flight where flight_id = '{active_row_id}';"
    df_row = get_sql_data(query)

    # Formatage des dates / heures
    df_row['flightlegs_arrinfo_times_scheduled_date'] = df_row['flightlegs_arrinfo_times_scheduled_date'].apply(lambda row: row.strftime('%Y-%m-%d') )
    df_row['flightlegs_depinfo_times_scheduled_date'] = df_row['flightlegs_depinfo_times_scheduled_date'].apply(lambda row: row.strftime('%Y-%m-%d') )
    df_row['flightlegs_arrinfo_times_scheduled_time'] = df_row['flightlegs_arrinfo_times_scheduled_time'].apply( lambda row: row.strftime('%H:%M:%S'))
    df_row['flightlegs_depinfo_times_scheduled_time'] = df_row['flightlegs_depinfo_times_scheduled_time'].apply( lambda row: row.strftime('%H:%M:%S'))

    json_tosend = df_row.to_dict(orient="records")[0]

    try:
        response = requests.post(f"http://{ml_api_host}:{ml_api_port}/get_delay_predictions",
            json=json_tosend  
        )

        response_json = response.json()
        df_response = pd.DataFrame.from_records(response_json, index=[0])
        df_response = df_response.transpose().reset_index()
        df_response.columns = ['prediction_type','prediction_value']
        df_response.dropna(subset=['prediction_value'])
        df_response = df_response[df_response['prediction_value'] != "NA"]

    except Exception as e:
        print("Issue with the request:", e)    
        
        df_response = pd.DataFrame({"Issue":e})
        
    #return html.Div([
    #    html.H1('Delay prediction'),
    #    dash_table.DataTable(df_response.to_dict('records'),
    #style_cell={'textAlign': 'left'})
    #])

    return html.Div(
        className="prediction-block",
        children=[
            html.H2("Delay prediction", className="card-title"),
            dash_table.DataTable(
                df_response.to_dict("records"),
                columns=[{"name": col, "id": col} for col in df_response.columns],
                style_cell={"textAlign": "left"},
            ),
        ],
    )

# --- Thème clair / sombre ---
@callback(
    Output("app-root", "className"),
    Input("theme-selector", "value"),
)
def update_theme(theme_value):
    if theme_value == "light":
        return "theme-light"
    return "theme-dark"

# --- Ouverture / fermeture de la sidebar (push complet) ---
@callback(
    Output("sidebar", "className"),
    Output("main-content", "className"),
    Input("sidebar-toggle", "n_clicks"),
    prevent_initial_call=False,
)
def toggle_sidebar(n_clicks):
    """
    n_clicks pair -> sidebar ouverte (push complet)
    n_clicks impair -> sidebar rétractée (width 0, seul bouton avion visible)
    """
    if not n_clicks:
        return "sidebar", "main-content expanded"

    if n_clicks % 2 == 1:
        return "sidebar collapsed", "main-content collapsed"
    return "sidebar", "main-content expanded"


server = app.server

if __name__ == '__main__':
    app.run(debug=True)

