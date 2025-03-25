# from datetime import datetime, timedelta
# from airflow.models import DAG
# from airflow.operators.python import PythonOperator
# from pymongo.mongo_client import MongoClient
# from concurrent.futures import ThreadPoolExecutor, as_completed
#
# # MongoDB Connection
# client = MongoClient("mongodb://35.154.221.2:27017/", maxPoolSize=50)
# db = client.jpdcl
#
# # Default Args
# default_args = {
#     'owner': 'Shubham Bhatt',
#     'start_date': datetime(2024, 4, 11),
#     'retries': 3,
#     'retry_delay': timedelta(seconds=120)
# }
#
# def fetch_sensor_data(sensor_id):
#     from_id = sensor_id + ":2024-07-02 00:00:00"
#     to_id = sensor_id + ":2025-03-18 23:59:59"
#     cursor = db.loadprofile.find({"_id": {"$gte": from_id, "$lt": to_id}}, no_cursor_timeout=True)
#     for doc in cursor:
#         doc['_id'] = str(doc['_id'])
#         process_sensor_data(doc)  # Process each document immediately to avoid memory spikes
#     cursor.close()
#
# def process_sensor_data(doc):
#     # Placeholder function to process each record
#     print(f"Processing data for sensor: {doc['_id']}")
#
# def fetch_all_sensors():
#     # return "hello"
#     sensor_info = db.sensor.find({}, {"id": 1, "_id": 0})
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         futures = {executor.submit(fetch_sensor_data, doc["id"]): doc["id"] for doc in sensor_info}
#         for future in as_completed(futures):
#             try:
#                 future.result()
#                 print(f"Fetched data for sensor: {futures[future]}")
#             except Exception as e:
#                 print(f"Error fetching data for sensor {futures[future]}: {e}")
#
# with DAG(
#         dag_id="ETL1",
#         default_args=default_args,
#         schedule_interval=None,
#         max_active_runs=100
# ) as dag:
#     fetch_sensors_task = PythonOperator(
#         task_id="fetch_all_sensors",
#         python_callable=fetch_all_sensors,
#     )
#
#     fetch_sensors_task

from datetime import datetime, timedelta
from threading import Thread
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import datatransformation
from utils.mongodbHelper import get_circle_data, get_sensor_data, get_process_sensor

default_args = {
    'owner': 'Shubham Bhatt',
    'start_date': datetime(2024, 4, 11),
    'retries': 3,
    'retry_delay': timedelta(seconds=120)
}


def process_sensor_wrapper(sensor_info, transformed_data):
    sen_id = sensor_info['id']
    site_id = sensor_info['site_id']
    processed_data = get_process_sensor(sen_id)
    if processed_data is None or len(processed_data) < 3000:
        return []
    else:
        tdata = transformed_data.append(datatransformation.init_transformation(processed_data, site_id))
        return tdata


def fetch_and_transform_sensor_data(sensor_data):
    threads = []
    transformed_data = []
    for sensor_info in sensor_data:
        thread = Thread(target=process_sensor_wrapper, args=(sensor_info, transformed_data))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    return "success"


with DAG(
        dag_id="ETL1",
        default_args=default_args,
        start_date=datetime(2024, 4, 11),
        schedule_interval=None,
        max_active_runs=100
) as dag:
    start = EmptyOperator(task_id="START")
    end = EmptyOperator(task_id="END")

    circle_data = get_circle_data()

    with TaskGroup("ParallelSensorTasks") as parallel_group:
        for circle_id in circle_data:
            sensor_data = get_sensor_data(circle_id['id'])
            fetch_sensor_task = PythonOperator(
                task_id=f"FetchSensor_{circle_id['id']}",
                python_callable=fetch_and_transform_sensor_data,
                op_args=[sensor_data]
            )
            fetch_sensor_task >> end

    start >> parallel_group