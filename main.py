from pyspark.sql import SparkSession
from IOUtilities import IOUtilities
from ProcessUtilities import ProcessUtilities

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
IO_utility = IOUtilities()
process_utility = ProcessUtilities()

answer = input("Which case do you want to use?")
if answer == '1':
    dataFrame = IO_utility.read_and_process(spark, "CovidCases.csv")
    dataFrame.printSchema()
    dataFrame.show(n=40, truncate=False)
    Processed = process_utility.get_average(dataFrame)
    Processed.show(n=40, truncate=False)
    IO_utility.save_dataframe(Processed, 'cases')
else:
    data = IO_utility.get_dataframe_from_db(spark, 'cases')
    data.show(n=50, truncate=False)
    newData = IO_utility.read_and_process(spark, "CovidNewCases.csv")
    newData.show(n=50, truncate=False)
    Processed = process_utility.get_average(newData)
    data = data.union(Processed)
    data.show(n=50, truncate=False)
    ProcessedData = process_utility.unite_averages(data)
    ProcessedData.show(n=50, truncate=False)
    IO_utility.overwrite_table(ProcessedData, 'newcases', 'cases')
