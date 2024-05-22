from datetime import datetime, timedelta
from threading import Thread
from airflow.models import DAG
from pymongo import MongoClient
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from airflow import DAG

import datatransformation
from utils.mongodbHelper import get_circle_data, get_sensor_data, get_process_sensor

client = MongoClient("mongodb://13.127.57.185:27017/")
db = client.pvvnl

default_args = {
    'owner': 'Shubham Bhatt',
    'start_date': datetime(2024, 4, 11),
    'retries': 3,
    'retry_delay': timedelta(seconds=120)
}


# def process_sensor_wrapper(sensor_info, transformed_data):
#     sen_id = sensor_info['id']
#     site_id = sensor_info['site_id']
#     processed_data = get_process_sensor(sen_id)
#     if processed_data is None or len(processed_data) < 3000:
#         return []
#     else:
#         tdata = transformed_data.append(datatransformation.init_transformation(processed_data, site_id))
#         return tdata


# def fetch_and_transform_sensor_data(sensor_data):
#     threads = []
#     transformed_data = []
#     for sensor_info in sensor_data:
#         thread = Thread(target=process_sensor_wrapper, args=(sensor_info, transformed_data))
#         thread.start()
#         threads.append(thread)
#     for thread in threads:
#         thread.join()
#     return "success"
def fetch_sensor_data(sensor_id):  
    from_id = sensor_id + "-2024-01-01 00:00:00"
    to_id = sensor_id + "-2024-03-31 23:59:59"
    results = list(db.load_profile_jdvvnl.find({"_id": {"$gte": from_id, "$lt": to_id}}))

    for doc in results:
        doc['_id'] = str(doc['_id'])
        
    
    return results
def get_circle_ids():
    return [doc["id"] for doc in db.circle.find({}, {"_id": 0, "id": 1})]
def fetch_data_for_circle(circle_id):
    sensor_info = db.jdvvnlSensor.find(
        {
            "circle_id": circle_id,
            "type": "AC_METER",
            "admin_status": {"$in": ["N", "S", "U"]},
            "utility": "2",
        },
        {"id": 1, "_id": 0},
    )
    chunk_size = 10
    all_sensor_data = []

    for i in range(0, len(sensor_info), chunk_size):
        sensor_ids_chunk = sensor_info[i : i + chunk_size]
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(fetch_sensor_data, sensor_id["id"])
                for sensor_id in sensor_ids_chunk
            ]

            for future in futures:
                try:
                    sensor_data = future.result()
                    all_sensor_data.extend(sensor_data)
                    print(f"Fetched data for sensor: {len(sensor_data)}")
                except Exception as e:
                    print(f"Error fetching data for sensor: {e}")
    return all_sensor_data



with DAG(
        dag_id="ETL1",
        default_args=default_args,
        start_date=datetime(2024, 4, 11),
        schedule_interval=None,
        max_active_runs=100
) as dag:
    # start = EmptyOperator(task_id="START")
    # end = EmptyOperator(task_id="END")

    circle_data = get_circle_ids()

    with TaskGroup("ParallelSensorTasks") as parallel_group:
        for circle_id in circle_data:
            # sensor_data = get_sensor_data(circle_id['id'])
            task = PythonOperator(
                task_id=f"task_{circle_id}",
                python_callable=fetch_data_for_circle,
                provide_context=True,
                op_kwargs={
                    "circle_id": circle_id,
                },
            )
            parallel_group
    #         fetch_sensor_task >> end

    # start >> parallel_group

# with DAG(
#         dag_id="SPARK",
#         default_args=default_args,
#         start_date=datetime(2024, 4, 21),
#         schedule_interval=None,
#         max_active_runs=100
# ) as dag:
#     start = EmptyOperator(task_id="START")
#     end = EmptyOperator(task_id="END")
