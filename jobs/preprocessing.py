from pyspark.sql import SparkSession
spark = SparkSession.builder \
            .appName("LocalSparkSession") \
            .master("local[*]") \
            .config("spark.network.timeout", "600s") \
            .config("spark.sql.debug.maxToStringFields", 100) \
            .getOrCreate()
print(spark)