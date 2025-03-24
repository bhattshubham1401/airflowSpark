import logging
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from pymongo.mongo_client import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup Logging
log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

# MongoDB Connection
client = MongoClient("mongodb://35.154.221.2:27017/", maxPoolSize=50)
db = client.jpdcl

# Default Args
default_args = {
    'owner': 'Shubham Bhatt',
    'start_date': datetime(2024, 4, 11),
    'retries': 3,
    'retry_delay': timedelta(seconds=120)
}

# Fetch Sensor Data Function
def fetch_sensor_data(sensor_id):
    from_id = sensor_id + ":2024-07-02 00:00:00"
    to_id = sensor_id + ":2025-03-18 23:59:59"
    log.info(f"Fetching data for sensor: {sensor_id}")

    cursor = db.loadprofile.find({"_id": {"$gte": from_id, "$lt": to_id}}, no_cursor_timeout=True)
    for doc in cursor:
        doc['_id'] = str(doc['_id'])
        process_sensor_data(doc)
    cursor.close()
    log.info(f"Finished fetching data for sensor: {sensor_id}")

# Placeholder for processing sensor data
def process_sensor_data(doc):
    log.info(f"Processing data for sensor: {doc['_id']}")

# Fetch All Sensors Function
def fetch_all_sensors():
    log.info("Fetching all sensors...")
    sensor_info = list(db.sensor.find({}, {"id": 1, "_id": 0}))  # Fetch all sensor IDs
    log.info(f"Total sensors fetched: {len(sensor_info)}")

    # Now process sensors in batches
    batch_size = 10  # Customize batch size as needed
    for i in range(0, len(sensor_info), batch_size):
        batch = sensor_info[i:i + batch_size]
        log.info(f"Processing batch: {i // batch_size + 1}")
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_sensor_data, doc["id"]): doc["id"] for doc in batch}
            for future in as_completed(futures):
                try:
                    future.result()
                    log.info(f"Fetched data for sensor: {futures[future]}")
                except Exception as e:
                    log.error(f"Error fetching data for sensor {futures[future]}: {e}")

    log.info("Finished processing all sensors")

# Define the DAG
with DAG(
        dag_id="ETL",
        default_args=default_args,
        schedule_interval=None,
        max_active_runs=100
) as dag:
    fetch_sensors_task = PythonOperator(
        task_id="fetch_all_sensors",
        python_callable=fetch_all_sensors,
    )

fetch_sensors_task
