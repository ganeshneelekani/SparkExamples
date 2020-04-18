

from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("PySpark App").setMaster("local[16]")
sc = SparkContext(conf=conf).getOrCreate()

read_file=sc.textFile("/home/g/Software/spark/practice/LabData/big.txt")
for line in read_file.collect():
    print(line)

rea

