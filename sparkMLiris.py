from pyspark.sql import SparkSession
from pyspark.ml.linalg import DenseVector

spark = SparkSession.builder.master("local") \
                            .appName("dat gi cung duoc") \
                            .config("spark.executor.memory", "lgb") \
                            .getOrCreate()

#rdd = sc.textFile
