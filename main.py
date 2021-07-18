from pyspark.sql import SparkSession


def readAndProcess(spark):
    dataFr = spark.read.options(header='True').csv(
        "C:\\Users\\LAPTOP_MIA\\PycharmProjects\\Covid\\CovidCasesWithoutDate.txt",
        inferSchema=True, nullValue='')
    dataFr = dataFr.fillna({'High School Cases': 0, 'Middle School Cases': 0, 'Elementary School Cases': 0})
    return dataFr


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dataFrame = readAndProcess(spark)
dataFrame.printSchema()
dataFrame.show(n=40, truncate=False)
