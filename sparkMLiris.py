from pyspark.sql import SparkSession
from pyspark.ml.linalg import DenseVector

spark = SparkSession.builder.master("local") \
                            .appName("dat gi cung duoc") \
                            .config("spark.executor.memory", "lgb") \
                            .getOrCreate()

#rdd = sc.textFile('/Users/Storage/Vicohub/Data/Iris.csv’)
df = spark.read.csv('/Users/Storage/Vicohub/Data/Iris.csv', header=True)
input_data = df.rdd.map(lambda x: (DenseVector(x[:4]), x[4]))
df1 = spark.createDataFrame(input_data, ["X", "Y"])

from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import RandomForestClassifier
indexer = StringIndexer(inputCol="Y", outputCol="Yn")
indexed = indexer.fit(df1).transform(df1)
(trainingData, testData) = indexed.randomSplit([0.7, 0.3])
rf = RandomForestClassifier(labelCol="Yn",
featuresCol="X",
numTrees=10)
model = rf.fit(trainingData)
predictions = model.transform(testData)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
predictions.select("Yn", "prediction")
evaluator = MulticlassClassificationEvaluator(
labelCol="Yn", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Accuracy = %g" % (accuracy))
