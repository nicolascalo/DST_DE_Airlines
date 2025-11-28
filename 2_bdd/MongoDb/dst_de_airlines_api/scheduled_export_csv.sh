read -p "nb_flight: " nb_flights

read -p "Date (YYYYMMDD-HH-MM-SS, optionnel - press enter to ignore): " date_param


if [ -n "$date_param" ]; then
    if ! [[ "$date_param" =~ ^[0-9]{8}-[0-9]{2}-[0-9]{2}-[0-9]{2}$ ]]; then
        echo "Eroor: Le format de la date doit être YYYYMMDD-HH-MM-SS"
        echo "Example: 20240115-14-30-45"
        exit 1
    fi
fi

read -p "start_id (optionnel - press enter to ignore): " start_id

args=("$nb_flights")

if [ -n "$date_param" ]; then
    args+=("$date_param")
else
    args+=("")  
fi

if [ -n "$start_id" ]; then
    args+=("$start_id")
fi


./venv/bin/python3 -m SCRIPTS.scheduled_export_csv "${args[@]}"



