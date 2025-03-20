from pymongo import MongoClient
<<<<<<< HEAD
import os
# from logs import logs_config

client = MongoClient("mongodb://35.154.221.2:27017/")
print("Database connection successful")


def get_database():
    try:

        database = client.pvvnl
        return database
    except Exception as e:
        # logs_config.logger.error("Database connection failed:", exc_info=True)
        raise e
=======
from dotenv import load_dotenv
import os
from logs import logs_config

load_dotenv()

client = MongoClient("mongodb://13.127.57.185:27017/")
logs_config.logger.info("Database connection successful")
def get_database():
    try:
        database = client.pvvnl 
        return database
    except Exception as e:
        logs_config.logger.error("Database connection failed:", exc_info=True)
        raise e
        
>>>>>>> 9b96abbeb2c5f32bd3020b6ddfa7456fb862d75c

