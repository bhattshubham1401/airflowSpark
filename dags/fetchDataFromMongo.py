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

def fetch_and_transform_sensor_data(circle_id, **kwargs):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='sensor_data_queue', durable=True)

    transformed_data_list = []

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode(), object_hook=json_util.object_hook)
            sensor_id = message['sensor_id']
            site_id = message['site_id']

            # Ensure the sensor belongs to the current circle_id
            sensor_circle_id = db.jdvvnlSensor.find_one({"id": sensor_id}, {"circle_id": 1})
            if sensor_circle_id and sensor_circle_id['circle_id'] == circle_id:
                from_id = sensor_id + "-2024-01-01 00:00:00"
                to_id = sensor_id + "-2024-03-31 23:59:59"
                results = list(
                    db.load_profile_jdvvnl.find({"_id": {"$gte": from_id, "$lt": to_id}})
                )

                for doc in results:
                    doc["_id"] = str(doc["_id"])  # Convert ObjectId to string

                transformed_data = datatransformation.init_transformation(results, site_id)
                transformed_data_list.extend(transformed_data)

            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge message
        
        except Exception as e:
            print(f"Error processing message for sensor_id {sensor_id}: {e}")
            # Handle the error (e.g., log, retry, send to error queue)


    # Start consuming messages (use basic_consume for your custom loop)
    channel.basic_consume(queue='sensor_data_queue', on_message_callback=callback)

    try:
        channel.start_consuming()  # This will block until you stop it
    except KeyboardInterrupt:  # Allow graceful shutdown with Ctrl+C
        channel.stop_consuming()
    finally:
        connection.close()

    return transformed_data_list


with DAG(dag_id="ETL1", default_args=default_args) as dag:

    circle_data = get_circle_ids()
    transformed_data_tasks = []

    for circle_id in circle_data:
        task = PythonOperator(
            task_id=f"fetch_and_transform_{circle_id}",
            python_callable=fetch_and_transform_sensor_data,
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
