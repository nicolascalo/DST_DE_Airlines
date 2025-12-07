from workflow_mongodb_postgresql_functions.utilities import (get_dataframe_from_mongodb,
                                                            load_postgres_config, 
                                                            copy_dataframe_to_postgres, 
                                                            run_sql_from_string,
                                                            create_temporary_tables)

from workflow_mongodb_postgresql_functions.gcp_logger import logger
import json
from io import BytesIO
from google.cloud import storage
import os
from dotenv import load_dotenv
load_dotenv()



if __name__=="__main__":
    # loading inputs
    collection_name,table_name, postgre_db_config = load_postgres_config()
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    SOURCE_BLOB_NAME = os.getenv("MAPPING_FILE_BLOB_NAME")
    logger.info(f"Fetching column mapping JSON from GCS bucket '{BUCKET_NAME}', blob '{SOURCE_BLOB_NAME}' ...")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(SOURCE_BLOB_NAME)
    

    json_data = blob.download_as_bytes()
    column_mapping = json.load(BytesIO(json_data))
    logger.info(f"Column mapping loaded with {len(column_mapping)} entries.")

    df = get_dataframe_from_mongodb(collection=collection_name)
    # import pandas as pd 
    # df = pd.read_csv("Workflow_Mongodb_Postgrsql_Package/afklm_flight_from_mongo_filtered_20251113-21-36-51.csv.gz")

    # Rename DataFrame columns according to the mapping
    logger.info("Renaming DataFrame columns...")
    df.rename(columns=column_mapping, inplace=True)
    logger.info("Columns renamed successfully.")
    # creating temporary table
    try:
        create_temporary_tables(postgre_db_config, table_name)
    except Exception as e:
        raise Exception(f"An Error has occured while creating {table_name}: {e}")
    # insertion of data
    try:
        copy_dataframe_to_postgres(df, table_name, postgre_db_config)
    except Exception as e:
        raise Exception(f"An Error has occured while copying mongodb data into postgre Sql database: {e}")
    # update flight id of sql tables
    update_flight_id_sql_table =f"""
    ALTER TABLE {table_name} ADD COLUMN flight_id VARCHAR(50);
    UPDATE {table_name} SET flight_id = CONCAT (id, flightLegs_depInfo_airport_code, flightLegs_arrInfo_airport_code);
    """
    try:
        run_sql_from_string(update_flight_id_sql_table, postgre_db_config)
    except Exception as e:
        raise Exception(f"An Error has occured while updating flight id: {e}")

    # creating index
    creating_index_sql = f"""
    drop index if exists {table_name}_flight_id;
    CREATE INDEX {table_name}_flight_id ON {table_name}(flight_id);
    """
    try:
        run_sql_from_string(creating_index_sql, postgre_db_config)
    except Exception as e:
        raise Exception(f"An Error has occured while creating index: {e}")
    


    # # executing transformation requests
    # try:
    #     run_sql_from_string(SQL_QUERIES, postgre_db_config)
    # except Exception as e:
    #     raise Exception(f"An Error has occured while executing SQL queries: {e}")


