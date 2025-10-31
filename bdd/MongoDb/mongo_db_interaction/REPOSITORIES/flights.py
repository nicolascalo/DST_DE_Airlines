from DB_CONTEXT.db_context import mongo_db_connect

collection = "flights"

def insert_one(flight):
    mongo_db_connect[collection].insert_one(flight)



def get_by_id(id):
    try:
        return mongo_db_connect[collection].find_one(
            {"id": id}
        )
    except (TypeError, KeyError):
        return None
    except Exception as e: 
        print(f"critical error : {e}")
        raise 
    

  

def create_index():
    mongo_db_connect[collection].create_index([("id", 1)], unique=True)


def count_flight():
    return mongo_db_connect[collection].count_documents({})


def get_all(nb_flight_limit):

    pipeline =  [{
           "$unwind":"$flightLegs"
        },
    {
    "$project": {
              
              
"id":"$id",
"airline_code":"$airline.code",
"airline_name":"$airline.name",
"flightLegs_aircraft_ownerAirlineCode":"$flightLegs.aircraft.ownerAirlineCode",
"flightLegs_aircraft_typeCode":"$flightLegs.aircraft.typeCode",
"flightLegs_arrivalInformation_airport_city_country_areaCode":"$flightLegs.arrivalInformation.airport.city.country.areaCode",
"flightLegs_arrivalInformation_airport_city_country_code":"$flightLegs.arrivalInformation.airport.city.country.code",
"flightLegs_arrivalInformation_airport_city_country_name":"$flightLegs.arrivalInformation.airport.city.country.name",
"flightLegs_arrivalInformation_airport_code":"$flightLegs.arrivalInformation.airport.code",
"flightLegs_arrivalInformation_airport_location_latitude":"$flightLegs.arrivalInformation.airport.location.latitude",
"flightLegs_arrivalInformation_airport_location_longitude":"$flightLegs.arrivalInformation.airport.location.longitude",
"flightLegs_arrivalInformation_airport_places_arrivalPositionTerminal":"$flightLegs.arrivalInformation.airport.places.arrivalPositionTerminal",
"flightLegs_arrivalInformation_times_actual":"$flightLegs.arrivalInformation.times.actual",
"flightLegs_arrivalInformation_times_actualTouchDownTime":"$flightLegs.arrivalInformation.times.actualTouchDownTime",
"flightLegs_arrivalInformation_times_estimated_value":"$flightLegs.arrivalInformation.times.estimated.value",
"flightLegs_arrivalInformation_times_latestPublished":"$flightLegs.arrivalInformation.times.latestPublished",
"flightLegs_arrivalInformation_times_scheduled":"$flightLegs.arrivalInformation.times.scheduled",
"flightLegs_departureInformation_airport_city_country_areaCode":"$flightLegs.departureInformation.airport.city.country.areaCode",
"flightLegs_departureInformation_airport_city_country_code":"$flightLegs.departureInformation.airport.city.country.code",
"flightLegs_departureInformation_airport_city_country_name":"$flightLegs.departureInformation.airport.city.country.name",
"flightLegs_departureInformation_airport_code":"$flightLegs.departureInformation.airport.code",
"flightLegs_departureInformation_airport_location_latitude":"$flightLegs.departureInformation.airport.location.latitude",
"flightLegs_departureInformation_airport_location_longitude":"$flightLegs.departureInformation.airport.location.longitude",
"flightLegs_departureInformation_airport_places_departurePositionTerminal_boardingTerminal":"$flightLegs.departureInformation.airport.places.boardingTerminal",
"flightLegs_departureInformation_airport_places_departurePositionTerminal_gateNumber":"$flightLegs.departureInformation.airport.places.gateNumber",
"flightLegs_departureInformation_times_actual":"$flightLegs.departureInformation.times.actual",
"flightLegs_departureInformation_times_actualTakeOffTime":"$flightLegs.departureInformation.times.actualTakeOffTime",
"flightLegs_departureInformation_times_latestPublished":"$flightLegs.departureInformation.times.latestPublished",
"flightLegs_departureInformation_times_scheduled":"$flightLegs.departureInformation.times.scheduled",
"flightLegs_irregularity_delayDuration":"$flightLegs.irregularity.delayDuration",
"flightLegs_irregularity_delayInformation_delayReasonPublicLong":"$flightLegs.irregularity.delayInformation.delayReasonPublicLong",
"flightLegs_irregularity_delayInformation_delayReasonPublicShort":"$flightLegs.irregularity.delayInformation.delayReasonPublicShort",
"flightLegs_irregularity_delayReason":"$flightLegs.irregularity.delayReason",
"flightLegs_scheduledFlightDuration":"$flightLegs.scheduledFlightDuration",
"flightLegs_serviceType":"$flightLegs.serviceType",
"flightLegs_status":"$flightLegs.status",
"flightNumber":"$flightNumber",
"flightStatusPublic":"$flightStatusPublic"
              }
        },
    {"$limit":nb_flight_limit}]       

    return list(mongo_db_connect[collection].aggregate(pipeline))

    