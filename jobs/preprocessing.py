
from pyspark.sql import SparkSession
spark = SparkSession.builder \
            .appName("LocalSparkSession") \
            .master("local[*]") \
            .config("spark.network.timeout", "600s") \
            .config("spark.sql.debug.maxToStringFields", 100) \
            .getOrCreate()
print(spark)

# import sys
# sys.path.append('D:/Deployment/airflowSpark')  # Adjust the path as needed
from jobs.common.spark_common import initiate_spark
print("hi")

