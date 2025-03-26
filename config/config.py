from pymongo import MongoClient
from dotenv import load_dotenv
import os
# from logs import logs_config
load_dotenv()
client = MongoClient("mongodb://35.154.221.2:27017/")
# logs_config.logger.info("Database connection successful")
def get_database():
    try:
        database = client.jpdcl
        return database

    except Exception as e:
        # logs_config.logger.error("Database connection failed:", exc_info=True)
        raise e