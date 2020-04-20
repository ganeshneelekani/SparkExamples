
from pyspark import SparkContext,SparkConf
from operator import add,mul

conf = SparkConf().setAppName("PySpark App").setMaster("local[16]")
sc = SparkContext(conf=conf).getOrCreate()


if __name__ == '__main__':
    string_value = ["hi hi how are are are you"]
    rd=sc.parallelize(string_value).flatMap(lambda x:x.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)
    print(rd.collect())