from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#create spark session
spark = SparkSession.builder \
     .master("local") \
     .appName("Word Count") \
     .config("spark.some.config.option", "some-value") \
     .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# read data from stream
df = spark.readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", "13.212.213.41:9092") \
     .option("subscribe", "test_topic",) \
     .option("startingOffsets", "earliest") \
     .load()

#convert data stream to string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

#select some fields stream
df = df.select(get_json_object(col("value"), "$.messageid").alias("messageid"),
     get_json_object(col("value"), "$.@timestamp").alias("timestamp"),
     get_json_object(col("value"), "$place").alias("place"),
     get_json_object(col("value"), "$.object.vehicle.license").alias("license"))\
     .filter('messageid is not null')

#rdd1 = df.

df.writeStream \
     .format("console") \
     .outputMode("update") \
     .start().awaitTermination()
#
#df.writeStream \
#     .format("csv") \
#     .outputMode("append") \
#     .option("checkpointLocation", "hdfs://localhost:9000/Users/Storage/checkpoint") \
#     .option("path", "hdfs://localhost:9000/kafkaout") \
#     .start().awaitTermination()
