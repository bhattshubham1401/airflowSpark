# from datetime import datetime, timedelta
# from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow import DAG
#
# default_args = {
#     'owner': 'Shubham Bhatt',
#     'start_date': datetime(2024, 4, 11),
#     'retries': 3,
#     'retry_delay': timedelta(seconds=120)
# }
#
# dag = DAG(
#     dag_id="modelBuilding",
#     default_args=default_args,
#     start_date=datetime(2024, 4, 11),
#     # schedule_interval="@daily",
#     max_active_runs=100
# )
#
# start = PythonOperator(task_id='Start',
#                        python_callable= lambda: print("jobs Started"),
#                        dag= dag)
#
# python_job = SparkSubmitOperator(
#     task_id='python_job',
#     conn_id="spark-conn",
#     application='jobs/preprocessing.py',
#     dag=dag
# )
#
# start >> python_job
#
#
