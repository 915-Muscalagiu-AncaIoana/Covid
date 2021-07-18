import pyspark as py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
text = spark.read.options(header='True').csv("C:\\Users\\LAPTOP_MIA\\PycharmProjects\\Covid\\CovidCasesWithoutDate.txt",inferSchema=True)
text.printSchema()
text.show(n=10, truncate=False)