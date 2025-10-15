from pymongo import MongoClient
from dotenv import load_dotenv
import os



load_dotenv()



mongo_db_connect = MongoClient(os.getenv('MONGODB_URI'))