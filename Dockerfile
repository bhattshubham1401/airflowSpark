FROM apache/airflow:2.9.0
COPY requirements.txt /requirements.txt
# RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark
RUN pip install apache-airflow-providers-mongo
# Use the Apache Airflow 2.7.1 image as the base image
# FROM apache/airflow:2.7.1

# # Switch to the "airflow" user
# USER airflow

# # Install pip
# RUN curl -O 'https://bootstrap.pypa.io/get-pip.py' && \
#     python3 get-pip.py --user

# # Install libraries from requirements.txt
# COPY requirements.txt /requirements.txt
# RUN pip install --user -r /requirements.txt