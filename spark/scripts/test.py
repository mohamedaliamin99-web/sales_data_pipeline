from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, round,when
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

spark = SparkSession.builder \
    .appName("RetailSilver") \
    .master("spark://retail-spark-master:7077") \
    .getOrCreate()


df = spark.read.parquet("hdfs://namenode:8020/data/silver/retail_cleaned")

null_count = df.filter(col("invoice_date").isNull()).count()
print(f"Number of nulls in 'invoice_date': {null_count}")


