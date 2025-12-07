from workflow_mongodb_postgresql_functions.sql_requests import (CLEANING_TEMPORARY_TABLES,
                                                                CLEANING_AIROPORT,
                                                                INSERT_SELECT_FLIGHT_PAST,
                                                                INSERT_SELECT_FLIGHT_FUTURE,
                                                                REMOVING_FROM_FUTURE_BASED_ON_PAST,
                                                                FINAL_CLEANING)
from workflow_mongodb_postgresql_functions.utilities import run_sql_from_string, load_postgres_config
from dotenv import load_dotenv
from workflow_mongodb_postgresql_functions.gcp_logger import logger
load_dotenv()

if __name__=="__main__":
    # loading inputs
    postgre_db_config = load_postgres_config()[-1]
    try:
        run_sql_from_string(CLEANING_TEMPORARY_TABLES, postgre_db_config)
        logger.info("cleaning temporary tables done successfully")
    except Exception as e:
        raise Exception(f"An Error has occured while creating index: {e}")
    try:
        run_sql_from_string(CLEANING_AIROPORT, postgre_db_config)
        logger.info("cleaning airport done successfully")
    except Exception as e:
        raise Exception(f"An Error has occured while creating index: {e}")
    try:
        run_sql_from_string(INSERT_SELECT_FLIGHT_PAST, postgre_db_config)
        logger.info("insert select flight past done successfully")
    except Exception as e:
        raise Exception(f"An Error has occured while creating index: {e}")
    try:
        run_sql_from_string(INSERT_SELECT_FLIGHT_FUTURE, postgre_db_config)
        logger.info("insert select flight future done successfully")
    except Exception as e:
        raise Exception(f"An Error has occured while creating index: {e}")
    try:
        run_sql_from_string(REMOVING_FROM_FUTURE_BASED_ON_PAST, postgre_db_config)
        logger.info("removing from future based on past done successfully")
    except Exception as e:
        raise Exception(f"An Error has occured while creating index: {e}")
    try:
        run_sql_from_string(FINAL_CLEANING, postgre_db_config)
        logger.info("final cleaning done successfully")
    except Exception as e:
        raise Exception(f"An Error has occured while creating index: {e}")
