from dash import Dash, dcc, html, Input, Output, callback
from dash.dash_table import DataTable
import pandas as pd
import requests, json
from sqlalchemy import create_engine




try:
    response = requests.get("http://127.0.0.1:8001/model_parameters_and_metrics")
    model_metrics_dict = response.json()
    print(model_metrics_dict)
    model_metrics = pd.DataFrame(model_metrics_dict).drop(['mode','best_pipeline'],axis=1)


except:
    response = "Issue when fetching the model metrics" 
    model_metrics = pd.DataFrame({"Error":"Issue when fetching the model metrics"})


print(model_metrics)

username = 'postgres'
password = 'postgres'
host = '0.0.0.0'
port = '5432'
database_name = 'afklm'

DATABASE_URL = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(DATABASE_URL)




#df = pd.read_csv('afklm_flight_from_mongo_filtered_20251113-21-36-51_test.csv', low_memory=False)

query = "select distinct(flight_id), \
        flightNumber, \
        airline_code, \
        flightLegs_depInfo_airport_Country_Name, \
        flightLegs_depInfo_airport_Airport_Name, \
        flightLegs_depInfo_airport_code, \
        flightLegs_depInfo_times_scheduled_date, \
        flightLegs_depInfo_times_scheduled_time, \
        flightLegs_arrInfo_airport_Country_Name, \
        flightLegs_arrInfo_airport_Airport_Name, \
        flightLegs_arrInfo_airport_code, \
        flightLegs_arrInfo_times_scheduled_date, \
        flightLegs_arrInfo_times_scheduled_time, \
        flightLegs_aircraft_typeCode, \
        flightLegs_serviceType, \
        flightLegs_aircraft_ownerAirlineCode, \
        flightLegs_depInfo_airport_Continent_Name, \
        flightLegs_depInfo_airport_Subcontinent_Name, \
        flightLegs_depInfo_airport_Country_Code, \
        flightLegs_depInfo_airport_Location_name, \
        flightLegs_depInfo_airport_Latitude, \
        flightLegs_depInfo_airport_Longitude, \
        flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, \
        flightLegs_depInfo_airport_places_depPosTerm_gateNumber, \
        flightLegs_depInfo_times_scheduled_year, \
        flightLegs_depInfo_times_scheduled_month, \
        flightLegs_depInfo_times_scheduled_day , \
        flightLegs_depInfo_times_scheduled_hour, \
        flightLegs_depInfo_times_scheduled_minute, \
        flightLegs_depInfo_times_scheduled_timezone, \
        flightLegs_depInfo_times_number_week, \
        flightLegs_arrInfo_airport_Continent_Name, \
        flightLegs_arrInfo_airport_Subcontinent_Name, \
        flightLegs_arrInfo_airport_Country_Code, \
        flightLegs_arrInfo_airport_Location_name, \
        flightLegs_arrInfo_airport_Latitude, \
        flightLegs_arrInfo_airport_Longitude, \
        flightLegs_arrInfo_airport_places_arrivalPositionTerminal, \
        flightLegs_arrInfo_times_scheduled_year, \
        flightLegs_arrInfo_times_scheduled_month, \
        flightLegs_arrInfo_times_scheduled_day, \
        flightLegs_arrInfo_times_scheduled_hour, \
        flightLegs_arrInfo_times_scheduled_minute, \
        flightLegs_arrInfo_times_scheduled_timezone, \
        flightLegs_arrInfo_times_number_week\
            from v_future_flight  \
                where (flightLegs_depInfo_airport_code IN ('CDG','AMS','ORY','FCO','LHR','CPH','MAD','ARN','OSL','LIN','NCE','BCN','LYS','BGO','LIS','DUB','HEL','TLS','OTP','FRA','MRS','ATH','PMI','MUC','TRD','MAN','BER','AGP','OPO')) AND (flightLegs_arrInfo_airport_code IN ('CDG','AMS','ORY','FCO','LHR','CPH','MAD','ARN','OSL','LIN','NCE','BCN','LYS','BGO','LIS','DUB','HEL','TLS','OTP','FRA','MRS','ATH','PMI','MUC','TRD','MAN','BER','AGP','OPO'));"


#query = "select * FROM v_past_flight  LIMIT 10 ;"


def get_sql_data(query):

    df = pd.read_sql(query, engine)
    return df


df = get_sql_data(query)

columns_new = df.columns.copy(deep=True)
columns_new = [w.replace('flightlegs_', '') for w in columns_new]
columns_new = [w.replace('info_times', '') for w in columns_new]
columns_new = [w.replace('info_airport', '') for w in columns_new]
columns_new = [w.replace('scheduled_', '') for w in columns_new]
columns_new = [w.replace('_depposterm', '') for w in columns_new]

df.columns = columns_new



df['id'] = df['flight_id']
df.set_index('id', inplace=True, drop=False)

app = Dash(__name__)

app.layout = html.Div([
        html.H1('Flights'),
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
    
    print("")
    print("")
    print("----------------------------------------------------------------")

    print(f"{active_row_id}")

    query = f"select *  from v_future_flight where flight_id = '{active_row_id}';"

    df_row = get_sql_data(query)
    json_tosend = df_row.to_dict(orient="records")
    print(json_tosend)



    try:
        response = requests.post("http://127.0.0.1:8001/get_delay_predictions", json=json_tosend[0])
    except:
        response = "Issue with the request" 
    print(response)



    return html.Div([
        html.H1('Delay prediction'),
        html.P(f'{response}'),
        html.P(f'{df_row}')

    ])

server = app.server

if __name__ == '__main__':
    app.run(debug=True)
