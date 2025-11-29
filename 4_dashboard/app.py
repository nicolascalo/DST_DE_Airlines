from dash import Dash, dcc, html, Input, Output, callback, dash_table
from dash.dash_table import DataTable
import pandas as pd
import requests, json
from sqlalchemy import create_engine
import datetime


try:
    response = requests.get("http://afklm_ml_api:8001/model_parameters_and_metrics")
    model_metrics_dict = response.json()
    model_metrics = pd.DataFrame(model_metrics_dict).drop(['mode','best_pipeline'],axis=1)
    print("Model metrics loaded")

except:
    response = "Issue when fetching the model metrics" 
    model_metrics = pd.DataFrame({"Error":"Issue when fetching the model metrics"})


username = 'postgres'
password = 'postgres'
host = 'afklm_postgres'
port = '5432'
database_name = 'afklm'

DATABASE_URL = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(DATABASE_URL)

query = "select v_future_flight.flight_id,  v_future_flight.flightNumber,  v_future_flight.airline_code,  v_future_flight.airline_name,  v_future_flight.flightStatusPublic,  v_future_flight.flightLegs_aircraft_typeCode,  v_future_flight.flightLegs_scheduledFlightDuration,  v_future_flight.flightLegs_serviceType,  v_future_flight.flightLegs_aircraft_ownerAirlineCode,  v_future_flight.flightLegs_status,  v_future_flight.flightLegs_serviceTypeName, v_future_flight.flightLegs_publishedStatus, v_future_flight.flightLegs_legStatusPublic, v_future_flight.flightLegs_statusName, v_geod.flightLegs_depInfo_airport_Continent_Name,  v_geod.flightLegs_depInfo_airport_Subcontinent_Name,  v_geod.flightLegs_depInfo_airport_Country_Code,  v_geod.flightLegs_depInfo_airport_Country_Name,  v_geod.flightLegs_depInfo_airport_Location_name,  v_geod.flightLegs_depInfo_airport_Airport_Name,  v_geod.flightLegs_depInfo_airport_Icao_Code,  v_geod.flightLegs_depInfo_airport_Latitude,  v_geod.flightLegs_depInfo_airport_Longitude, v_future_flight.flightLegs_depInfo_airport_code, v_future_flight.flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, v_future_flight.flightLegs_depInfo_airport_places_depPosTerm_gateNumber, v_future_flight.flightLegs_depInfo_times_scheduled_date, v_future_flight.flightLegs_depInfo_times_scheduled_time, v_future_flight.flightLegs_depInfo_times_scheduled_year, v_future_flight.flightLegs_depInfo_times_scheduled_month, v_future_flight.flightLegs_depInfo_times_scheduled_day , v_future_flight.flightLegs_depInfo_times_scheduled_hour, v_future_flight.flightLegs_depInfo_times_scheduled_minute, v_future_flight.flightLegs_depInfo_times_scheduled_timezone, v_future_flight.flightLegs_depInfo_times_number_week, v_geoa.flightLegs_arrInfo_airport_Continent_Name,  v_geoa.flightLegs_arrInfo_airport_Subcontinent_Name,  v_geoa.flightLegs_arrInfo_airport_Country_Code,  v_geoa.flightLegs_arrInfo_airport_Country_Name,  v_geoa.flightLegs_arrInfo_airport_Location_name,  v_geoa.flightLegs_arrInfo_airport_Airport_Name,  v_geoa.flightLegs_arrInfo_airport_Icao_Code,  v_geoa.flightLegs_arrInfo_airport_Latitude,  v_geoa.flightLegs_arrInfo_airport_Longitude, v_future_flight.flightLegs_arrInfo_airport_code, v_future_flight.flightLegs_arrInfo_airport_places_arrivalPositionTerminal, v_future_flight.flightLegs_arrInfo_times_scheduled_date, v_future_flight.flightLegs_arrInfo_times_scheduled_time, v_future_flight.flightLegs_arrInfo_times_scheduled_year, v_future_flight.flightLegs_arrInfo_times_scheduled_month, v_future_flight.flightLegs_arrInfo_times_scheduled_day, v_future_flight.flightLegs_arrInfo_times_scheduled_hour, v_future_flight.flightLegs_arrInfo_times_scheduled_minute, v_future_flight.flightLegs_arrInfo_times_scheduled_timezone, v_future_flight.flightLegs_arrInfo_times_number_week from v_future_flight v_future_flight   INNER JOIN v_geod v_geod ON v_geod.flightLegs_depInfo_airport_Iata_Code = v_future_flight.flightLegs_depInfo_airport_code    INNER JOIN v_geoa v_geoa ON v_geoa.flightLegs_arrInfo_airport_Iata_Code = v_future_flight.flightLegs_arrInfo_airport_code"

query = " select v_future_flight.flight_id,  v_future_flight.flightNumber,  v_future_flight.airline_code,  v_future_flight.airline_name,    v_future_flight.flightLegs_aircraft_typeCode,  v_future_flight.flightLegs_serviceTypeName , v_geod.flightLegs_depInfo_airport_Continent_Name,  v_geod.flightLegs_depInfo_airport_Subcontinent_Name,  v_geod.flightLegs_depInfo_airport_Country_Code,  v_geod.flightLegs_depInfo_airport_Country_Name,    v_geod.flightLegs_depInfo_airport_Airport_Name, v_future_flight.flightLegs_depInfo_airport_code, v_future_flight.flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, v_future_flight.flightLegs_depInfo_airport_places_depPosTerm_gateNumber, v_future_flight.flightLegs_depInfo_times_scheduled_date, v_future_flight.flightLegs_depInfo_times_scheduled_time, v_future_flight.flightLegs_depInfo_times_scheduled_year, v_future_flight.flightLegs_depInfo_times_scheduled_month, v_future_flight.flightLegs_depInfo_times_scheduled_day , v_future_flight.flightLegs_depInfo_times_scheduled_hour, v_future_flight.flightLegs_depInfo_times_scheduled_minute, v_future_flight.flightLegs_depInfo_times_scheduled_timezone, v_future_flight.flightLegs_depInfo_times_number_week, v_geoa.flightLegs_arrInfo_airport_Continent_Name,  v_geoa.flightLegs_arrInfo_airport_Subcontinent_Name,  v_geoa.flightLegs_arrInfo_airport_Country_Code,  v_geoa.flightLegs_arrInfo_airport_Country_Name,   v_geoa.flightLegs_arrInfo_airport_Airport_Name,      v_future_flight.flightLegs_arrInfo_airport_code, v_future_flight.flightLegs_arrInfo_airport_places_arrivalPositionTerminal, v_future_flight.flightLegs_arrInfo_times_scheduled_date, v_future_flight.flightLegs_arrInfo_times_scheduled_time, v_future_flight.flightLegs_arrInfo_times_scheduled_year, v_future_flight.flightLegs_arrInfo_times_scheduled_month, v_future_flight.flightLegs_arrInfo_times_scheduled_day, v_future_flight.flightLegs_arrInfo_times_scheduled_hour, v_future_flight.flightLegs_arrInfo_times_scheduled_minute, v_future_flight.flightLegs_arrInfo_times_scheduled_timezone, v_future_flight.flightLegs_arrInfo_times_number_week from v_future_flight v_future_flight   INNER JOIN v_geod v_geod ON v_geod.flightLegs_depInfo_airport_Iata_Code = v_future_flight.flightLegs_depInfo_airport_code    INNER JOIN v_geoa v_geoa ON v_geoa.flightLegs_arrInfo_airport_Iata_Code = v_future_flight.flightLegs_arrInfo_airport_code ;"


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




app = Dash(__name__)

app.layout = html.Div([
        html.H1('Flights'),
        html.P('Click on a row to get the delay prediction'),
    DataTable(
        id='datatable-row-ids',
        columns=[
            {'name': i, 'id': i, 'deletable': False} for i in df.columns if i != 'id'
        ],
        data=df.to_dict('records'),
        editable=False,
        filter_action="native",
        sort_action="native",
        sort_mode='multi',
        filter_options={'case':'insensitive'}, 
        row_deletable=False,
        selected_rows=[],
        page_action='native',
        page_current=0,
        page_size=25,
    ),
            html.H1('Model metrics'),

    DataTable(
        id='datatable-metrics',
        columns=[
            {'name': i, 'id': i, 'deletable': False} for i in model_metrics.columns if i != 'id'
        ],
        data=model_metrics.to_dict('records'),
        editable=False,
        sort_action="native",
        sort_mode='multi',
        filter_options={'case':'insensitive'}, 
        row_deletable=False,
        selected_rows=[],
        page_action='native',
        page_current=0,
        page_size=25,
    ),
    html.Div(id='datatable-row-ids-container')
])

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

    active_row_id = active_cell['row_id'] if active_cell else None

    colors = ['#FF69B4' if id == active_row_id
              else '#7FDBFF' if id in selected_id_set
              else '#0074D9'
              for id in row_ids]






    query = f"select *  from v_future_flight where flight_id = '{active_row_id}';"

    df_row = get_sql_data(query)

    df_row['flightlegs_arrinfo_times_scheduled_date'] = df_row['flightlegs_arrinfo_times_scheduled_date'].apply(lambda row: row.strftime('%Y-%m-%d') )
    df_row['flightlegs_depinfo_times_scheduled_date'] = df_row['flightlegs_depinfo_times_scheduled_date'].apply(lambda row: row.strftime('%Y-%m-%d') )

    df_row['flightlegs_arrinfo_times_scheduled_time'] = df_row['flightlegs_arrinfo_times_scheduled_time'].apply( lambda row: row.strftime('%H:%M:%S'))

    df_row['flightlegs_depinfo_times_scheduled_time'] = df_row['flightlegs_depinfo_times_scheduled_time'].apply( lambda row: row.strftime('%H:%M:%S'))



    json_tosend = df_row.to_dict(orient="records")[0]



    try:
        response = requests.post("http://afklm_ml_api:8001/get_delay_predictions",
            json=json_tosend  
        )

    except Exception as e:
        print("Issue with the request:", e)    
    
    response_json = response.json()
    df_response = pd.DataFrame.from_records(response_json, index=[0])
    df_response = df_response.transpose().reset_index()
    df_response.columns = ['prediction_type','prediction_value']
    df_response.dropna(subset=['prediction_value'])
    df_response = df_response[df_response['prediction_value'] != "NA"]
    

    return html.Div([
        html.H1('Delay prediction'),
        dash_table.DataTable(df_response.to_dict('records'),
    style_cell={'textAlign': 'left'})




    ])

server = app.server

if __name__ == '__main__':
    app.run(debug=True)
