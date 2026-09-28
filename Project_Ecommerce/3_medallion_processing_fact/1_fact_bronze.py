# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Fact Data into Bronze Layer

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

#  Here we are using all as stringType beacuse we have some data Quality issues in the data 
# we will solved these issues later in the silver level  

order_items_schema = StructType([
    StructField("dt",                 StringType(), True),
    StructField("order_ts",           StringType(), True),
    StructField("customer_id",        StringType(), True),
    StructField("order_id",           StringType(), True),
    StructField("item_seq",           StringType(), True),
    StructField("product_id",         StringType(), True),
    StructField("quantity",           StringType(), True),
    StructField("unit_price_currency",StringType(), True),
    StructField("unit_price",         StringType(), True),
    StructField("discount_pct",       StringType(), True),
    StructField("tax_amount",         StringType(), True),
    StructField("channel",            StringType(), True),
    StructField("coupon_code",        StringType(), True),
])

# COMMAND ----------

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/order_items/landing/*.csv"

df = spark.read.option("header", "true").option("delimiter", ",").schema(order_items_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp())

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

df.count()
#  We have 183378 records in the data 
# its 3 months full data Records from 2022-01-01 to 2022-03-31 

# COMMAND ----------


df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_order_items")

# COMMAND ----------

