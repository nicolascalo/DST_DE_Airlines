BDD mongoDb 



Container Docker 


Fill docker-compose.yml 

start the docker container 

docker exec -it <container_name> bash


Environment Variable

fill .env . You can see the required environement variables by reading envstruct.

Data loading


source venv/bin/activate

Go to MongoDb folder
cd ~MongoDb

Execute
python3 -m mongo_db_interaction.main
