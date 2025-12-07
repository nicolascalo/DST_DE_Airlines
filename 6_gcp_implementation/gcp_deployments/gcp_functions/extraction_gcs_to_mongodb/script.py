import base64
import functions_framework
from dst_de_airlines_api.SCRIPTS.insert_by_operational_flights_with_batch import  import_operationalflights_in_mongodb, clean, add_date_insertion_in_flights

@functions_framework.cloud_event
def populate_mongodb(cloud_event):
    import_operationalflights_in_mongodb()
    clean()
    add_date_insertion_in_flights()
