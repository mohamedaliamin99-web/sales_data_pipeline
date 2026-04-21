from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, round,when,trim,regexp_replace
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

spark = SparkSession.builder \
    .appName("RetailSilver") \
    .master("spark://retail-spark-master:7077") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

schema = StructType([
    StructField("InvoiceNo",   StringType(),  True),
    StructField("StockCode",   StringType(),  True),
    StructField("Description", StringType(),  True),
    StructField("Quantity",    IntegerType(), True),
    StructField("InvoiceDate", StringType(),  True),
    StructField("UnitPrice",   DoubleType(),  True),
    StructField("CustomerID",  StringType(),  True),
    StructField("Country",     StringType(),  True)
])

df = spark.read.csv(
    "hdfs://namenode:8020/data/raw/Online_Retail.csv",
    header=True,
    schema=schema
)
# Drop rows with missing CustomerID
df =df.dropna(subset=["CustomerID"])


# Fix the InvoiceDate type and format

df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "MM/dd/yyyy HH:mm:ss"))

# Derive the total revenue column
df = df.withColumn("revenue" , round(col("Quantity") * col("UnitPrice")))


# Fixing column names
rename_map = {
    "InvoiceNo": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
    "revenue": "revenue"
}

for old_col, new_col in rename_map.items():
    df = df.withColumnRenamed(old_col, new_col)


# Flaging the returns where the Quantity is negative    

df_cleaned = df.withColumn("is_return" , when(col("Quantity") < 0 ,1).otherwise(0) )


# Writing the cleaned data to silver layer
df_cleaned.write.mode("overwrite").csv("hdfs://namenode:8020/data/silver/retail_cleaned.csv")


df_cleaned.show(5)


num = df.filter(col("InvoiceDate").isNull()).count()
print(f"Number of nulls in 'invoice_date': {num}")

df_cleaned.printSchema()







