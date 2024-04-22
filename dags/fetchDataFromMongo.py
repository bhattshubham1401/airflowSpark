from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.mongodbHelper import get_circle_data, get_sensor_data, get_process_sensor

default_args = {
    'owner': 'Shubham Bhatt',
    'start_date': datetime(2024, 4, 11),
    'retries': 3,
    'retry_delay': timedelta(seconds=120)
}

with DAG(
        dag_id="ETL",
        default_args=default_args,
        start_date=datetime(2024, 4, 11),
        schedule_interval=None,
        max_active_runs=2000
) as dag:
    Start = EmptyOperator(task_id="START")
    circle_data = get_circle_data()

    with TaskGroup('CircleDataFetching') as circleGrp:
        for circle_id in circle_data:
            circle_task = PythonOperator(
                task_id=f"FetchingSensorID_{circle_id}",
                python_callable=get_sensor_data,
                op_args=[circle_id]
            )

            with TaskGroup(f'SensorsForCircle_{circle_id}') as sensorGrp:
                sensor_data = get_sensor_data(circle_id)
                for sensor_info in sensor_data:
                    sensor_id = sensor_info['id']
                    sensor_name = sensor_info['name']
                    sensor_task = PythonOperator(
                        task_id=f"ProcessSensor_{sensor_id}",
                        python_callable=get_process_sensor,
                        op_args=[circle_id, sensor_id]
                    )
                    circle_task >> sensor_task

    task5 = EmptyOperator(task_id="END")

    Start >> circleGrp >> task5
