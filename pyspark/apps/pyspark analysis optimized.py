from pyspark.sql import SparkSession
import os, time
from pyspark.sql.types import StringType, DateType, TimestampType, StructField, StructType
from pyspark.sql.functions import col, coalesce, when, lit, avg, sum, count, cast, date_trunc

start = time.time()

print('\n]nCreating spark session using default configs')
spark = SparkSession.builder.appName('Spark Assessment').config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate()

schema = StructType([
    StructField("timestamp", TimestampType(), True),  
    StructField("level", StringType(), True),     
    StructField("message", StringType(), True)     
])

print('\n\nReading log data with spark fromm csv...\n\n')
logs_df = spark.read.csv('/opt/spark-data/sample_logs.csv', header=True, inferSchema=False, schema=schema).repartition(100)

print('\n\nPrinting spark dataframe schema...\n\n')
print('\n\n\n')
print(logs_df.printSchema())
print('\n\n\n')

## Cache some of the filtered records used
logs_df_filtered_july = logs_df.filter(col('timestamp') >= '2024-07-01 00:00:00').cache()
logs_df_filtered_sept = logs_df_filtered_july.filter(col('timestamp') >= '2024-09-01 00:00:00').cache()

## Count of log messages that was logged from Seotember, 2024 upward

print('\n\nLog messages from September, 2024 upward...\n\n')
print('\n\n\n')
print('count of log messages is...\n')
print(logs_df_filtered_sept.count())
print('\n\n\n')

print('\n\nLog ERROR messages from September, 2024 upward...\n\n')
print('\n\n\n')
print('count of logs with ERRORS is...\n')
print(logs_df_filtered_sept.filter((col('level') == 'ERROR')).count())
print('\n\n\n')

## Aggregate log messages by log level
print('\n\nBelow is the count of log messages from July 2024 upward by Log level...\n\n')
print('\n\n\n')
print(logs_df_filtered_july.groupBy('level').agg(count("message").alias("message_count")).orderBy("message_count", ascending=False).collect())
print('\n\n\n')

## Aggregate log messages from July 2024 upward by log date
print('\n\nBelow is the count of log messages from July 2024 upward by Log date...\n\n')
print('\n\n\n')
print(logs_df_filtered_july \
    .withColumn('Date', col("timestamp").cast(DateType())).groupBy('Date') \
    .agg({"message": "count"}).collect())
print('\n\n\n')

## Aggregate log ERROR messages from July 2024 upward by log month & date
print('\n\nBelow is the count of log ERROR messages from September 2024 upward by Log month & date...\n\n')
print('\n\n\n')
print(logs_df_filtered_sept.filter((col('level') == 'ERROR')) \
    .withColumn('Date', col("timestamp").cast(DateType())) \
    .withColumn('month', date_trunc("month", col('Date'))) \
    .groupBy(['month', 'Date']) \
    .agg(count("message").alias("error_count")).orderBy(['month', 'error_count'], ascending=[True, False]).collect())
print('\n\n\n')

## Pivot count of log messages in each log levels across the entire data
print('\n\nPrinting Pivot count of log messages in each log levels across the entire data\n]n')
print('\n\n')
print(logs_df \
    .withColumn('Date', col("timestamp").cast(DateType())) \
    .withColumn('month', date_trunc('month', col("Date"))) \
    .groupBy('level') \
    .pivot("month") \
    .agg(count("message")).collect())
print('\n\n')

print('\n\nStopping spark session ...\n\n')
spark.stop()

print('\n\nSpark sesision stopped successfully')

stop = time.time()

print('\n\n\n')
print(f'\n\nThis job took {(stop-start)/60} minutes to run')
print('\n\n\n')