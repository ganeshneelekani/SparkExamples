from pyspark import SparkContext,SparkConf
from operator import add,mul

conf = SparkConf().setAppName("PySpark App").setMaster("local[16]")
sc = SparkContext(conf=conf).getOrCreate()

words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"])
data = words_new.value

words_new=["hi" ,"sdc", "uiids"]
ssc=sc.parallelize(words_new)
pairs = ssc.map(lambda x: (x[0], x))
print(pairs.collect())