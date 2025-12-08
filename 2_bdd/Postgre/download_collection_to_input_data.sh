#!/bin/bash
set -e

#HTTP_CODE=$(curl -w "%{http_code}" -s -o /dev/null -X GET "${AFKLM_API_URL}download_historic_flights" -H 'accept: application/json')
#echo "historic_flights: HTTP ${HTTP_CODE}"

#HTTP_CODE=$(curl -w "%{http_code}" -s -o /dev/null -X GET "${AFKLM_API_URL}download_update_scheduled_d1_flights" -H 'accept: application/json')
#echo "update_scheduled_d1_flights: HTTP ${HTTP_CODE}"

#HTTP_CODE=$(curl -w "%{http_code}" -s -o /dev/null -X GET "${AFKLM_API_URL}download_scheduled_flights" -H 'accept: application/json')
#echo "scheduled_flights: HTTP ${HTTP_CODE}"


HTTP_CODE=$(curl -w "%{http_code}" -s -o /dev/null -X GET "${AFKLM_API_URL}export_all_collections_to_postgres_data_input" -H 'accept: application/json')
echo "scheduled_flights: HTTP ${HTTP_CODE}"

