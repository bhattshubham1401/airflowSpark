import logging
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from pymongo.mongo_client import MongoClient

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

# Fetch All Sensors Function
def fetch_all_sensors():
    log.info("Fetching all sensors...")
    sensor_info = list(db.sensor.find({}, {"id": 1, "_id": 0}))  # Fetch all sensor IDs
    log.info(f"Total sensors fetched: {len(sensor_info)}")

    # Log the sensor IDs (you can process this data further if required)
    for sensor in sensor_info:
        log.info(f"Fetched sensor: {sensor['id']}")

    log.info("Finished fetching all sensors")

# Define the DAG
with DAG(
        dag_id="fetch_sensors_only",
        default_args=default_args,
        schedule_interval=None,  # You can set a cron expression for periodic scheduling
        max_active_runs=1
) as dag:
    fetch_sensors_task = PythonOperator(
        task_id="fetch_all_sensors",
        python_callable=fetch_all_sensors,
    )

fetch_sensors_task
