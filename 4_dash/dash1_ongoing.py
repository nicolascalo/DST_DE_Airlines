import pandas as pd
import dash, requests, json
from dash import Dash, dash_table, dcc, html, Input, Output, callback, State
from dash.dependencies import Input, Output
from fastapi import FastAPI, Depends, HTTPException
from typing import List, Optional
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy import URL
from fastapi.responses import JSONResponse

username = 'daniel'
password = 'datascientest'
host = '54.170.154.220'
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

app = Dash()

app.layout = html.Div([
    dash_table.DataTable(
        id='datatable-interactivity',
        columns=[
            {"name": i, "id": i, "deletable": True, "selectable": True} for i in df.columns
        ],
        data=df.to_dict('records'),
        editable=True,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        column_selectable="single",
        row_selectable="multi",
        row_deletable=True,
        selected_columns=[],
        selected_rows=[],
        page_action="native",
        page_current= 0,
        page_size= 20,
    ),
    html.Div(id='datatable-interactivity-container'),
    html.Button("Prediction request", id="prediction"),
    html.Div(id='output')
])

@callback(
    Output('datatable-interactivity', 'style_data_conditional'),
    Input('datatable-interactivity', 'selected_columns')
)
def update_styles(selected_columns):
    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]

@callback(
    Output('datatable-interactivity-container', "children"),
    Input('datatable-interactivity', "derived_virtual_data"),
    Input('datatable-interactivity', "derived_virtual_selected_rows"))
def update_graphs(rows, derived_virtual_selected_rows):
    # Lors du premier rendu du tableau, `derived_virtual_data` et
    # `derived_virtual_selected_rows` seront `None`. Ceci est dû à une
    # particularité de Dash (les propriétés non fournies sont toujours `None` et Dash
    # appelle les fonctions de rappel dépendantes lors du premier rendu du composant).
    # Ainsi, si `rows` est `None`, cela signifie que le composant vient d'être rendu
    # et sa valeur sera identique à celle du dataframe du composant.
    # Au lieu de définir `None` ici, vous pouvez également définir
    # `derived_virtual_data=df.to_rows('dict')` lors de l'initialisation
    # du composant.
    if derived_virtual_selected_rows is None:
        derived_virtual_selected_rows = []

    dff = df if rows is None else pd.DataFrame(rows)

    colors = ['#7FDBFF' if i in derived_virtual_selected_rows else '#0074D9'
              for i in range(len(dff))]

    return [
        dcc.Graph(
            id=column,
            figure={
                "data": [
                    {
                        "x": dff["country"],
                        "y": dff[column],
                        "type": "bar",
                        "marker": {"color": colors},
                    }
                ],
                "layout": {
                    "xaxis": {"automargin": True},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": column}
                    },
                    "height": 250,
                    "margin": {"t": 10, "l": 10, "r": 10},
                },
            },
        )
        # Vérifier si la colonne existe - l'utilisateur l'a peut-être supprimée
        # Si `column.deletetable=False`, alors vous n'avez pas
        # besoin d'effectuer cette vérification.
        for column in ["pop", "lifeExp", "gdpPercap"] if column in dff
    ]

@app.callback(
    Output('output', 'children'),
    Input('prediction', 'n_clicks'),
    State('dropdown1', 'value'), # continent_dep
    State('dropdown2', 'value'), # subcontinent_dep
    State('dropdown3', 'value'), # country_dep
    State('dropdown4', 'value'), # location_dep
    State('dropdown5', 'value'), # airline_code
    State('dropdown6', 'value'), # flightlegs_depinfo_airport_code
    State('dropdown7', 'value'), # flight_id
    prevent_initial_call=True
)
def prediction(n_clicks, continent_name, subcontinent_name, country_name, location_name, airline_code, flightlegs_depinfo_airport_code, flight_id):
    if not all([continent_name, subcontinent_name, country_name, location_name, airline_code, flightlegs_depinfo_airport_code, flight_id]):
        return "Missing item"

    query_res = f"select distinct(flight.flight_id), \
                    Continent_name, \
                    Subcontinent_name, \
                    Country_name, \
                    Location_name, \
                    Airport_name, \
                    flightLegs_depInfo_airport_code, \
                    flightLegs_depInfo_airport_places_depPosTerm_boardingTerminal, \
                    flightLegs_depInfo_airport_places_depPosTerm_gateNumber, \
                    CAST (departure_airport.flightLegs_depInfo_times_scheduled_date AS varchar),\
                    CAST (departure_airport.flightLegs_depInfo_times_scheduled_time AS varchar),\
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
                        where flight.flight_id = '{flight_id}';"

    df_res = pd.read_sql(query_res, engine)

    try:
        json_tosend = df_res.to_dict(orient="records")
        json_tosend = json_tosend[0]
        response = requests.post("http://127.0.0.1:8000/flight", json=json_tosend)
        print(response)
        print(json_tosend)

    except IndexError:
        raise HTTPException(status_code=500, detail='Server error')


if __name__ == '__main__':
    app.run(debug=True)
