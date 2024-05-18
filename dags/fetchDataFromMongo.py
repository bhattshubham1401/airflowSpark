from datetime import datetime, timedelta
from threading import Thread
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import datatransformation
from utils.mongodbHelper import get_circle_data, get_sensor_data, get_process_sensor

default_args = {
    'owner': 'Shubham Bhatt1232222',
    'start_date': datetime(2024, 4, 11),
    'retries': 3,
    'retry_delay': timedelta(seconds=120)
}


def process_sensor_wrapper(sensor_info, transformed_data, circle_id):
    sen_id = sensor_info['id']
    site_id = sensor_info['site_id']
    processed_data = get_process_sensor(sen_id)
    if processed_data is None or len(processed_data) < 3000:
        return []
    else:
        tdata = transformed_data.append(datatransformation.init_transformation(processed_data, site_id, circle_id))
        return tdata


def fetch_and_transform_sensor_data(sensor_data, circle_id):
    threads = []
    transformed_data = []
    for sensor_info in sensor_data:
        thread = Thread(target=process_sensor_wrapper, args=(sensor_info, transformed_data, circle_id))
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
                op_args=[sensor_data, circle_id['id']]
            )
            fetch_sensor_task >> end

    start >> parallel_group