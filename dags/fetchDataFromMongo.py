from datetime import datetime, timedelta
from threading import Thread
import datatransformation
from airflow.models import DAG
from pymongo import MongoClient
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from airflow import DAG
import pika 
import json
from bson import json_util


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


def get_rabbitmq_connection():
    
    credentials = pika.PlainCredentials('admin', 'mypass')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host='rabbitmq',  
            port=5672, 
            credentials=credentials,
            virtual_host='admin'
        )
    )
    return connection

def publish_to_rabbitmq(sensor_id, site_id):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='sensor_data_queue', durable=True)

    message = json.dumps({"sensor_id": sensor_id, "site_id": site_id},default=json_util.default) 
    channel.basic_publish(
        exchange='',
        routing_key='sensor_data_queue',
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
    )

    connection.close()

def get_circle_ids():
    return [doc["id"] for doc in db.circle.find({}, {"_id": 0, "id": 1})]

def fetch_sensor_data_from_rabbitmq(**kwargs):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='sensor_data_queue', durable=True)

    method_frame, header_frame, body = channel.basic_get(queue='sensor_data_queue')
    if method_frame:
        message = json.loads(body.decode(),object_hook=json_util.object_hook)
        sensor_id = message['sensor_id']
        site_id = message['site_id']
        from_id = sensor_id + "-2024-01-01 00:00:00"
        to_id = sensor_id + "-2024-03-31 23:59:59"
        results = list(
            db.load_profile_jdvvnl.find({"_id": {"$gte": from_id, "$lt": to_id}})
        )
        for doc in results:
            doc["_id"] = str(doc["_id"])

        # Apply data transformation
        transformed_data = datatransformation.init_transformation(results, site_id)
        # Store transformed data (replace with your actual storage logic)
        # ...

        # Acknowledge message consumption
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    
    connection.close()
    print(transformed_data)

    return transformed_data

def fetch_data_for_circle(circle_id):
    sensor_info = db.jdvvnlSensor.find(
        {
            "circle_id": circle_id,
            "type": "AC_METER",
            "admin_status": {"$in": ["N", "S", "U"]},
            "utility": "2",
        },
        {"id": 1, "_id": 0 , "site_id": 1},
    )

    for sensor in sensor_info:
        publish_to_rabbitmq(sensor['id'],sensor['site_id']) 



with DAG(
        dag_id="ETL1",
        default_args=default_args,
        start_date=datetime(2024, 4, 11),
        schedule_interval=None,
        max_active_runs=100,
) as dag:

    circle_data = get_circle_ids()

    with TaskGroup("ParallelSensorTasks") as parallel_group:
        for circle_id in circle_data:
            task = PythonOperator(
                task_id=f"task_{circle_id}",
                python_callable=fetch_data_for_circle,
                provide_context=True,
                op_kwargs={
                    "circle_id": circle_id,
                },
            )
            task

    consume_task = PythonOperator(
        task_id="consume_from_rabbitmq",
        python_callable=fetch_sensor_data_from_rabbitmq,
        provide_context=True,
    )

    parallel_group >> consume_task



# with DAG(
#         dag_id="SPARK",
#         default_args=default_args,
#         start_date=datetime(2024, 4, 21),
#         schedule_interval=None,
#         max_active_runs=100
# ) as dag:
#     start = EmptyOperator(task_id="START")
#     end = EmptyOperator(task_id="END")
