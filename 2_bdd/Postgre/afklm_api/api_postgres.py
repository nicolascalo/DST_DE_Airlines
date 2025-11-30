import pandas as pd
import requests, json

import psycopg2





username = 'postgres'
password = 'postgres'
host = 'afklm_postgres'
port = '5432'
database_name = 'afklm'


conn = psycopg2.connect(database=database_name,
                        host=host,
                        user=username,
                        password=password,
                        port=port)


cur = conn.cursor()





try:
    cur.execute("INSERT INTO nom_table (col1) VALUES (%s)", ('valeur',))
    conn.commit()
except Exception as e:
    conn.rollback()
    print(f"Erreur: {e}")



app = FastAPI(
    title="AFKLM API",
    version="1.0.0",
    docs_url="/"
)


@app.get("/download_historic_flights")
def download_historic_flights():


    query = f"select *  from v_future_flight where flight_id = '{active_row_id}';"

    df_row = get_sql_data(query)



    route = "historic/export"

    file_name = "afklm_historic_from_mongo"
    path = download_data(file_name, route)

    if path is None:
        raise HTTPException(status_code=404, detail="historic flights not found")
    return {'message': file_name + 'saved in' + path}


