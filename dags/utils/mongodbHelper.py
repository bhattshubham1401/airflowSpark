import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


def get_connection():
    try:
        db = os.getenv("db")
        host = os.getenv("host")
        port = os.getenv("port")
        mongo_url = f"mongodb://{host}:{port}"
        client = MongoClient(mongo_url)
        db1 = client[db]
        return db1

    except Exception as e:
        print("Error in Mongo Helper", e)


def get_circle_data():
    try:
        dataList = []
        conn = get_connection()
        collection_name = os.getenv("sensor")
        sensor = conn[collection_name]
        data = sensor.find({"utility": "2", "type": "AC_METER", "admin_status": {"$in": ['N', 'S', 'U']}},
                           {"circle_id": 1}).distinct("circle_id")
        for item in data:
            dataList.append(item)

            # print(item)
        return dataList
    except Exception as e:
        print("Error fetching data from MongoDB:", e)


def get_sensor_data(circle):
    try:
        dataList = []
        conn = get_connection()
        collection_name = os.getenv("sensor")
        sensor = conn[collection_name]
        data = sensor.find({"circle_id": circle, "type": "AC_METER", "admin_status": {"$in": ['N', 'S', 'U']}},
                           {"name": 1, "_id": 0, "id": 1}).limit(50)
        for item in data:
            dataList.append(item)
        return dataList
    except Exception as e:
        print("Error fetching data from MongoDB:", e)


def get_process_sensor(circle_id, sensorId):
    try:
        dataList = []
        conn = get_connection()
        collection_name = os.getenv("loadProfileData")
        loadProfile = conn[collection_name]
        fromId = sensorId + "-2024-01-01 00:00:00"
        toId = sensorId + "-2024-03-31 23:59:59"
        data = loadProfile.find({"_id": {"$gte": fromId, "$lte": toId}})

        for doc in data:
            dataList.append(doc)
        return dataList
    except Exception as e:
        print("Error fetching data from MongoDB:", e)
