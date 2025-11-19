import pandas as pd
import dash, requests, json
from dash import Dash, dcc, html, State
from dash.dependencies import Input, Output
from fastapi import FastAPI, Depends, HTTPException
from typing import List, Optional
from pydantic import BaseModel
from sqlalchemy import create_engine

username = 'daniel'
password = 'datascientest'
host = '54.75.58.98'
port = '5432'
database_name = 'airline'

DATABASE_URL = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"
engine = create_engine(DATABASE_URL)

def get_data():
    query = "select distinct(flight.flight_id), \
        Continent_name, \
        Subcontinent_name, \
        Country_name, \
        Location_name, \
        Airport_name, \
        flightLegs_depInfo_airport_code, \
        flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, \
        flightLegs_depInfo_airport_places_depPosTerm_gateNumber, \
        flightLegs_depInfo_times_scheduled_date, \
        flightLegs_depInfo_times_scheduled_time, \
        flightLegs_depInfo_times_scheduled_year, \
        flightLegs_depInfo_times_scheduled_month, \
        flightLegs_depInfo_times_scheduled_day, \
        flightLegs_depInfo_times_scheduled_hour, \
        flightLegs_depInfo_times_scheduled_minute, \
        flightLegs_depInfo_times_number_week, \
        flightNumber, \
        airline_code, \
        airline_name, \
        flightStatusPublic, \
        flightLegs_aircraft_typeCode, \
        flightLegs_scheduledFlightDuration, \
        flightLegs_serviceType, \
        flightLegs_aircraft_ownerAirlineCode, \
        flightLegs_status, \
        delay_status, \
        flightLegs_irregularity_delayDuration, \
        flightLegs_irregularity_delayInfo_delayReasonPublicLong, \
        flightLegs_irregularity_delayInfo_delayReasonPublicShort \
            from Continent Continent \
            INNER JOIN Subcontinent Subcontinent ON Continent.Continent_ID = Subcontinent.Continent_ID \
            INNER JOIN Country Country ON Country.Subcontinent_ID = Subcontinent.Subcontinent_ID  \
            INNER JOIN Location Location ON Location.Country_ID = Country.Country_ID  \
            INNER JOIN Airport Airport ON Airport.Location_ID = Location.Location_ID  \
            INNER JOIN departure_airport departure_airport ON departure_airport.flightLegs_depInfo_airport_code = airport.Iata_code  \
            INNER JOIN flight flight ON flight.flight_id = departure_airport.flight_id \
            INNER JOIN delay delay ON delay.flight_id = flight.flight_id \
                where flightLegs_depInfo_times_scheduled_date > current_date;"

    df = pd.read_sql(query, engine)
    return df

df = get_data()

app = dash.Dash(__name__)

app.layout = html.Div([
    html.Th("Continent:"),
    dcc.Dropdown(
        id='dropdown1',
        options=[{'label': col, 'value': col} for col in df['continent_name'].unique()],
        value=df['continent_name'].unique()[0]
    ),

    html.Th("Subcontinent:"),
    dcc.Dropdown(
        id='dropdown2',
        value=df[df['continent_name'] == df['continent_name'].unique()[0]]['subcontinent_name'].iloc[0],
        options=[]
    ),

    html.Th("Country:"),
    dcc.Dropdown(
        id='dropdown3',
        value=df[df['subcontinent_name'] == df['subcontinent_name'].unique()[0]]['country_name'].iloc[0],
        options=[]
    ),

    html.Th("Location:"),
    dcc.Dropdown(
        id='dropdown4',
        value=df[df['country_name'] == df['country_name'].unique()[0]]['location_name'].iloc[0],
        options=[]
    ),

    html.Th("Departure airport:"),
    dcc.Dropdown(
        id='dropdown5',
        value=df[df['location_name'] == df['location_name'].unique()[0]]['flightlegs_depinfo_airport_code'].iloc[0],
        options=[]
    ),

    html.Th("Departure airline:"),
    dcc.Dropdown(
        id='dropdown6',
        value=df[df['flightlegs_depinfo_airport_code'] == df['flightlegs_depinfo_airport_code'].unique()[0]]['airline_code'].iloc[0],
        options=[]
    ),

    html.Th("Departure flight_id:"),
    dcc.Dropdown(
        id='dropdown7',
        value=df[df['airline_code'] == df['airline_code'].unique()[0]]['flight_id'].iloc[0],
        options=[]
    ),
    html.Button("Demande de prédiction", id="prediction"),
    html.Div(id='output')

])

@app.callback(
    Output('dropdown2', 'options'),
    Output('dropdown2', 'value'),
    Input('dropdown1', 'value')
)
def update_dropdown2(col1_value):
    df_col = df[df['continent_name'] == col1_value]
    options = [{'label': str(val), 'value': val} for val in df_col['subcontinent_name'].unique()]
    value = df_col['subcontinent_name'].iloc[0]
    return options, value

@app.callback(
    Output('dropdown3', 'options'),
    Output('dropdown3', 'value'),
    Input('dropdown2', 'value')
)
def update_dropdown3(col2_value):
    df_col = df[df['subcontinent_name'] == col2_value]
    options = [{'label': str(val), 'value': val} for val in df_col['country_name'].unique()]
    value = df_col['country_name'].iloc[0]
    return options, value

@app.callback(
    Output('dropdown4', 'options'),
    Output('dropdown4', 'value'),
    Input('dropdown3', 'value')
)
def update_dropdown4(col3_value):
    df_col = df[df['country_name'] == col3_value]
    options = [{'label': str(val), 'value': val} for val in df_col['location_name'].unique()]
    value = df_col['location_name'].iloc[0]
    return options, value

@app.callback(
    Output('dropdown5', 'options'),
    Output('dropdown5', 'value'),
    Input('dropdown4', 'value')
)
def update_dropdown5(col4_value):
    df_col = df[df['location_name'] == col4_value]
    options = [{'label': str(val), 'value': val} for val in df_col['flightlegs_depinfo_airport_code'].unique() + " - " + df_col['airport_name'].unique()]
    value = df_col['flightlegs_depinfo_airport_code'].iloc[0] + " - " +  df_col['airport_name'].iloc[0]
    return options, value

@app.callback(
    Output('dropdown6', 'options'),
    Output('dropdown6', 'value'),
    Input('dropdown5', 'value')
)
def update_dropdown6(col5_value):
    df_col = df[df['flightlegs_depinfo_airport_code'] + " - " + df['airport_name'] == col5_value]
    options = [{'label': str(val), 'value': val} for val in df_col['airline_code'].unique() + " - " + df_col['airline_name'].unique()]
    value = df_col['airline_code'].iloc[0] + " - " +  df_col['airline_name'].iloc[0]
    return options, value

@app.callback(
    Output('dropdown7', 'options'),
    Output('dropdown7', 'value'),
    Input('dropdown6', 'value')
)
def update_dropdown7(col6_value):
    df_col = df[df['airline_code'] + " - " + df['airline_name'] == col6_value]
    options = [{'label': str(val), 'value': val} for val in df_col['flight_id'].unique()]
    value = df_col['flight_id'].iloc[0]
    return options, value

@app.callback(
    Output('output', 'children'),
    Input('prediction', 'n_clicks'),
    State('dropdown1', 'value'), # continent_name
    State('dropdown2', 'value'), # subcontinent_name
    State('dropdown3', 'value'), # country_name
    State('dropdown4', 'value'), # location_name
    State('dropdown5', 'value'), # flightlegs_depinfo_airport_code
    State('dropdown6', 'value'), # airline_code
    State('dropdown7', 'value'), # flight_id
    prevent_initial_call=True
)
def prediction(n_clicks, continent_name, subcontinent_name, country_name, location_name, flightlegs_depinfo_airport_code, airline_code, flight_id):
    if not all([continent_name, subcontinent_name, country_name, location_name, flightlegs_depinfo_airport_code, airline_code, flight_id]):
        return "Missing item"

    result = {
                'continent_name': continent_name,
                'subcontinent_name': subcontinent_name,
                'country_name': country_name,
                'location_name': location_name,
                'flightlegs_depinfo_airport_code': flightlegs_depinfo_airport_code,
                'airline_code': airline_code,
                'flight_id': flight_id
            }
    try:
        #response = requests.post("http://127.0.0.1:8000/flight -H 'accept: application/json' -H Content-Type: application/json -d", json=result)
        print(f"{continent_name}, {subcontinent_name}, {country_name}, {location_name}, {flightlegs_depinfo_airport_code}, {airline_code}, {flight_id}")
        response = requests.post("http://127.0.0.1:8000/flight", json=result)
        print(response)
    except IndexError:
        raise HTTPException(status_code=500, detail='Server error')

if __name__ == '__main__':
    app.run(debug=True)
