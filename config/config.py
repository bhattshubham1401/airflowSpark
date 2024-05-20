from pymongo import MongoClient
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
        

