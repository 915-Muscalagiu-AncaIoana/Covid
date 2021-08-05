from pyspark.sql import SparkSession
from IOUtilities import IOUtilities
from ProcessUtilities import ProcessUtilities


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
IO_utility = IOUtilities()
dataFrame = IO_utility.read_and_process(spark)
dataFrame.printSchema()
process_utility = ProcessUtilities()
dataFrame.show(n=40, truncate=False)
Processed = process_utility.get_average(dataFrame)
Processed.show(n=40, truncate=False)

IO_utility.save_dataframe(Processed)
