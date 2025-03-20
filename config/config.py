from pymongo import MongoClient
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

