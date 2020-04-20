from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("PySpark App").setMaster("local[16]")
sc = SparkContext(conf=conf).getOrCreate()

SparkSession=SparkSession.builder.master("local").appName("Word Count").getOrCreate()

df=SparkSession.read.load('/home/g/Software/spark/practice/employee.csv',
                         format='com.databricks.spark.csv',
                          header='true',
                          inferSchema='true')
df.filter(df['Emp ID'] > 100104).show()
# employeeDataFrame=df.createOrReplaceTempView("Employee")
# df3 = SparkSession.sql("select * from Employee")
# employeeDataFrame.show()
