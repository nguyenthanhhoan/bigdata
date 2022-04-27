from pyspark import SparkContext

def filter_post_get(line):
   command = line.split(",")[2].split()[0]
   return command in ["GET","POST"]
  
def get_page(line):
   page = line.split(",")[2].split()[1]
   page = page.split("?")[0]
   return page
   
def filter_php(line):
   return ".php" in line


sc = SparkContext("local", "first app")
logs = sc.textFile("hdfs://localhost:9000/weblog.csv")
logs_filter_get_post = logs.filter(filter_post_get)
pages = logs_filter_get_post.map(get_page)
pages_php = pages.filter(filter_php)
result = pages_php.map(lambda s: (s,1)).reduceByKey(lambda x,y: x+y)
print(result.collect())
sc.stop()


