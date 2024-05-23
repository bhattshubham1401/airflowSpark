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
            virtual_host='/'
        )
    )
    return connection

def get_circle_ids():
    return [doc["id"] for doc in db.circle.find({}, {"_id": 0, "id": 1})]

def publish_sensor_data_to_rabbitmq(sensor_id, site_id, results):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='sensor_data_queue', durable=True)

    for doc in results:
        message = json.dumps({"sensor_id": sensor_id, "site_id": site_id, "data": doc}, default=json_util.default)
        channel.basic_publish(
            exchange='',
            routing_key='sensor_data_queue',
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
    connection.close()


def fetch_sensor_data_from_rabbitmq(**kwargs):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='sensor_data_queue', durable=True)

    fetched_data_list = []

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode(), object_hook=json_util.object_hook)
            fetched_data_list.append(message)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Error processing message: {e}")

    while True:
        method_frame, header_frame, body = channel.basic_get(queue='sensor_data_queue')
        if not method_frame:
            break
        callback(channel, method_frame, header_frame, body)

    connection.close()
    return fetched_data_list

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
        sensor_id = sensor['id']
        site_id = sensor['site_id']
        from_id = sensor_id + "-2024-01-01 00:00:00"
        to_id = sensor_id + "-2024-03-31 23:59:59"

        results = list(
            db.load_profile_jdvvnl.find({"_id": {"$gte": from_id, "$lt": to_id}})
        )
        for doc in results:
            doc["_id"] = str(doc["_id"])

        publish_sensor_data_to_rabbitmq(sensor_id, site_id, results) 

    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='sensor_data_queue', durable=True)

    # transformed_data_list = []

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode(), object_hook=json_util.object_hook)
            sensor_id = message['sensor_id']
            site_id = message['site_id']

            sensor_circle_id = db.jdvvnlSensor.find_one({"id": sensor_id}, {"circle_id": 1})
            if sensor_circle_id and sensor_circle_id['circle_id'] == circle_id:
                from_id = sensor_id + "-2024-01-01 00:00:00"
                to_id = sensor_id + "-2024-03-31 23:59:59"
                results = list(
                    db.load_profile_jdvvnl.find({"_id": {"$gte": from_id, "$lt": to_id}})
                )

                for doc in results:
                    doc["_id"] = str(doc["_id"])

                # transformed_data = datatransformation.init_transformation(results, site_id)
                # transformed_data_list.extend(transformed_data)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            return results
        
        except Exception as e:
            print(f"Error processing message for sensor_id {sensor_id}: {e}")

    channel.basic_consume(queue='sensor_data_queue', on_message_callback=callback)

    try:
        channel.start_consuming()  
    except KeyboardInterrupt:  
        channel.stop_consuming()
    finally:
        connection.close()

    return results


with DAG(dag_id="ETL1", default_args=default_args) as dag:

    circle_data = get_circle_ids()
    transformed_data_tasks = []

    for circle_id in circle_data:
        task = PythonOperator(
            task_id=f"fetch_and_transform_{circle_id}",
            python_callable=fetch_data_for_circle,
            op_kwargs={"circle_id": circle_id},
        )
        transformed_data_tasks.append(task)
    
    # Add a downstream task for further processing/storage if needed:
    aggregate_task = EmptyOperator(task_id="aggregate_transformed_data")

    # Create dependency chain: Parallel tasks -> aggregate_task
    transformed_data_tasks >> aggregate_task


# with DAG(
#         dag_id="SPARK",
#         default_args=default_args,
#         start_date=datetime(2024, 4, 21),
#         schedule_interval=None,
#         max_active_runs=100
# ) as dag:
#     start = EmptyOperator(task_id="START")
#     end = EmptyOperator(task_id="END")
