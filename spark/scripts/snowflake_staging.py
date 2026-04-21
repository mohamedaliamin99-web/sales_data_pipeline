from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

spark = SparkSession.builder \
    .appName("Retail_Snowflake_Staging") \
    .master("spark://retail-spark-master:7077") \
    .getOrCreate()

schema = StructType([
    StructField("invoice_no",   StringType(),  True),
    StructField("stock_code",   StringType(),  True),
    StructField("description", StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("invoice_date", StringType(),  True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("customer_id",  StringType(),  True),
    StructField("country",     StringType(),  True),
    StructField("revenue",     DoubleType(),  True),
    StructField("is_return",   IntegerType(),  True)
])


df = spark.read.csv("hdfs://namenode:8020/data/silver/retail_cleaned.csv", header=True, schema=schema)

df.write \
  .format("snowflake") \
  .option("sfURL", os.getenv("SNOWFLAKE_URL")) \
  .option("sfUser", os.getenv("SNOWFLAKE_USER")) \
  .option("sfPassword", os.getenv("SNOWFLAKE_PASSWORD")) \
  .option("sfDatabase", os.getenv("SNOWFLAKE_DATABASE")) \
  .option("sfSchema", os.getenv("SNOWFLAKE_SCHEMA")) \
  .option("sfWarehouse", os.getenv("SNOWFLAKE_WAREHOUSE")) \
  .option("dbtable", os.getenv("SNOWFLAKE_TABLE")) \
  .option("sfRole", os.getenv("SNOWFLAKE_ROLE")) \
  .mode("overwrite") \
  .save()
