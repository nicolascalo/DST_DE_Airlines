PATTERN='^dump-[0-9]{8}-[0-9]{2}-[0-9]{2}-[0-9]{2}\.archive$'
FOLDER="./data_dump"
CONTAINER_NAME=$(python3 -c "
import yaml
with open('docker-compose.yml', 'r') as f:
    config = yaml.safe_load(f)
    print(config['services']['mongodb']['container_name'])
")
USER_NAME=$(python3 -c "
import yaml
with open('docker-compose.yml', 'r') as f:
    config = yaml.safe_load(f)
    print(config['services']['mongodb']['environment']['MONGO_INITDB_ROOT_USERNAME'])
")
PASSWORD=$(python3 -c "
import yaml
with open('docker-compose.yml', 'r') as f:
    config = yaml.safe_load(f)
    print(config['services']['mongodb']['environment']['MONGO_INITDB_ROOT_PASSWORD'])
")


wait_for_mongodb()
{
    local max_attempts=30  
    local attempt=0 

    while [ $attempt -lt $max_attempts ]; do
        if docker exec "$CONTAINER_NAME" mongosh --quiet --username "$USER_NAME" --password "$PASSWORD" --authenticationDatabase admin --eval "db.adminCommand('ping')" &>/dev/null; then
            echo "MongoDB is ready !"
            return 0
        fi
        sleep 7
        attempt=$((attempt + 1))
        echo "Attempt $attempt/$max_attempts to connect mongodb"
    done

    echo "Faild after $max_attempts attemps to mongoDb." >&2
    return 1
}



is_valid_file()
 {  
    not_valid_file=$(find "$FOLDER" -maxdepth 1 -type f ! -name '.gitkeep' -exec basename {} \; | grep -vE "$PATTERN")
    if [ -n "$not_valid_file" ]; then
        echo "false" 
    else
        echo "true"   
    fi
}

import_dump()
 {  
    local latest_dump=$1

    if ! cat "$latest_dump" | docker exec -i "$CONTAINER_NAME" mongorestore --username="$USER_NAME" --password="$PASSWORD" --authenticationDatabase=admin --archive; then
        echo "Erreur : La restauration du dump a échoué." >&2
        return 1
    else
         echo "La bdd mongo db a été monté avec le dump : $latest_dump"

    fi
}

get_lastest_dump()
 { 
    latest_dump=$(ls -t "$FOLDER"/dump-*.archive 2>/dev/null | head -n 1)

    echo "$latest_dump"  
}

main()
 {  echo "Start container: $CONTAINER_NAME"
    echo "User: $USER_NAME"
    is_valid_file_result=$(is_valid_file)  
    if [ "$is_valid_file_result" = "false" ]; then  
        echo "INVALID FILE :"
        echo "$not_valid_file"
        echo "File name must be: dump-yyyymmdd-hh-mn-ss.archive"
        exit 1 
       
 
    else
        docker-compose up -d
        echo "Start container: $CONTAINER_NAME"
        echo "User: $USER_NAME"
        if ! wait_for_mongodb; then
            exit 1
        fi
       
        latest_dump=$(get_lastest_dump)
        import_dump "$latest_dump"
    fi
}

main
