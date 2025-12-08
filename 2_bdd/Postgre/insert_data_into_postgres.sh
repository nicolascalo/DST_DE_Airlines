#!/bin/bash
set -e

HTTP_CODE=$(curl -w "%{http_code}" -s -o /dev/null -X GET "${AFKLM_API_URL}load_mongodb_data_into_postgres" -H 'accept: application/json')
echo "load_mongodb_data_into_postgres: HTTP ${HTTP_CODE}"

