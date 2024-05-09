import os
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def get_connection():
    try:
        max_pool_size = 4000
        db = os.getenv("db")
        host = os.getenv("host")
        port = os.getenv("port")
        mongo_url = f"mongodb://{host}:{port}"
        client = MongoClient(mongo_url, maxPoolSize=max_pool_size)
        db1 = client[db]
        return db1

    except Exception as e:
        print("Error in Mongo Helper", e)


def get_circle_data():
    try:
        dataList = []
        conn = get_connection()
        collection_name = os.getenv("circle")
        circle = conn[collection_name]
        data = circle.find({"utility": "2"},{"id":1,"name":1}).limit(5)
        dataList.extend(data)
        return dataList
    except Exception as e:
        print("Error fetching circle data from MongoDB:", e)


def get_sensor_data(circle):
    try:
        dataList = []
        conn = get_connection()
        collection_name = os.getenv("sensor")
        sensor = conn[collection_name]
        data = sensor.find({"circle_id": circle, "type": "AC_METER", "admin_status": {"$in": ['N', 'S', 'U']}, "utility": "2"},
                           {"name": 1, "_id": 0, "id": 1, "meter_ct_mf": 1, "UOM": 1, "meter_MWh_mf": 1, "site_id": 1, "asset_id": 1}).limit(10)
        for item in data:
            dataList.append(item)
        return dataList
    except Exception as e:
        print("Error fetching data from MongoDB:", e)


def get_process_sensor(sensorId):
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


def get_process_sensorV1(sensorId):
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

        df = pd.DataFrame(dataList)
        return df
    except Exception as e:
        print("Error fetching data from MongoDB:", e)

def data_from_weather_api(site, startDate, endDate):
    ''' Fetch weather data from CSV file based on date range'''

    # logger.info("Weather data fetching")
    try:
        start_date = startDate.strftime('%Y-%m-%d %H:%M:%S')
        end_date = endDate.strftime('%Y-%m-%d %H:%M:%S')
        conn = get_connection()
        collection_name = os.getenv("weatherData")
        loadProfile = conn[collection_name]

        documents = []
        query = loadProfile.find({
            "site_id": site,
            "time": {
                "$gte": start_date,
                "$lte": end_date
            }
        })
        for doc in query:
            documents.append(doc)
        try:

            df = pd.DataFrame(documents)
            return df
        except Exception as e:
            print(e)
    except Exception as e:
        print("Error:", e)

def load_data(data):
    conn = get_connection()
    collection_name = os.getenv("transformed_data")
    t_data = conn[collection_name]

    for items in data:
        sensor_id = items["sensor_id"]
        creation_time_str = items['creation_time'].strftime("%Y-%m-%d %H:%M:%S")
        _id = f"{sensor_id}_{creation_time_str}"
        data = {
            'site_id': items.get("site_id", 0.0),
            'temperature_2m': items.get("temperature_2m", 0.0),
            'relative_humidity_2m': items.get("site_id", 0.0),
            'apparent_temperature': items.get("relative_humidity_2m", 0.0),
            'precipitation': items.get("precipitation", 0.0),
            'wind_speed_10m': items.get("wind_speed_10m", 0.0),
            'wind_speed_100m': items.get("wind_speed_100m", 0.0),
            'creation_time': items.get("creation_time", 0.0),
            'sensor_id': items.get("sensor_id", 0.0),
            'count':items.get("count", 0.0),
            'is_holiday':items.get("is_holiday", 0.0),
            'consumed_unit': items.get("consumed_unit", 0.0),
            'lag1': items.get("lag1", 0.0),
            'lag2': items.get("lag2", 0.0),
            'lag3': items.get("lag3", 0.0),
            'lag4': items.get("lag4", 0.0),
            'lag5': items.get("lag5", 0.0),
            'lag6': items.get("lag6", 0.0),
            'lag7': items.get("lag7", 0.0),
            'day': items.get("day", 0.0),
            'hour': items.get("hour", 0.0),
            'month': items.get("month", 0.0),
            'dayofweek': items.get("dayofweek", 0.0),
            'quarter': items.get("quarter", 0.0),
            'dayofyear': items.get("dayofyear", 0.0),
            'weekofyear': items.get("weekofyear", 0.0),
            'year': items.get("year", 0.0)
        }
        document = {
            "_id": _id,
            "sensor_id": sensor_id,
            "creation_time": creation_time_str,
            "data": data,
        }
        # Insert the document into MongoDB
        t_data.insert_one(document)
    return "success"
