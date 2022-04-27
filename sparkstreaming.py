from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("localhost", 9999)
result = lines.flatMap(lambda s: s.split()).map(lambda w: (w,1)).reduceByKey(lambda x,y: x+y)
result.pprint()

ssc.start()
ssc.awaitTermination()
