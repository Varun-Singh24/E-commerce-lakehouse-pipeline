# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver to Gold: Building BI Ready Tables

# COMMAND ----------

import pyspark.sql.functions as F 
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

df = spark.table(f"{catalog_name}.silver.slv_order_items")

df.limit(10).display()

# COMMAND ----------

# 1) Add gross amount
df = df.withColumn(
    "gross_amount",
    F.col("quantity") * F.col("unit_price")
    )

# 2) Add discount_amount (discount_pct is already numeric, e.g., 21 -> 21%)
df = df.withColumn(
    "discount_amount",
    F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0))
)

# 3) Add sale_amount = gross - discount
df = df.withColumn(
    "sale_amount",
    F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
)

# add date id
df = df.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))  # Create date_key

# Coupon flag
#  coupon flag = 1 if coupon_code is not null else 0
df = df.withColumn(
    "coupon_flag",
    F.when(F.col("coupon_code").isNotNull(), F.lit(1))
     .otherwise(F.lit(0))
)

df.limit(5).display()    

# COMMAND ----------

# MAGIC %md
# MAGIC currency conversion

# COMMAND ----------

# --- 1) Define your fixed FX rates (as of 2025-10-15, like your PBI note) ---
fx_rates = {
    "INR": 1.00,
    "AED": 24.18,
    "AUD": 57.55,
    "CAD": 62.93,
    "GBP": 117.98,
    "SGD": 68.18,
    "USD": 88.29,
}

rates = [(k, float(v)) for k, v in fx_rates.items()]
rates_df = spark.createDataFrame(rates, ["currency", "inr_rate"])
rates_df.show()

# COMMAND ----------

df = (
    df
    .join(
        rates_df,
        rates_df.currency == F.upper(F.trim(F.col("unit_price_currency"))),
        "left"
    )
    .withColumn("sale_amount_inr", F.col("sale_amount") * F.col("inr_rate"))
    .withColumn("sale_amount_inr", F.ceil(F.col("sale_amount_inr")))
)

# COMMAND ----------

df.limit(5).display()    

# COMMAND ----------

orders_gold_df = df.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"),
    F.col("sale_amount_inr").alias("net_amount_inr")
)

# COMMAND ----------

orders_gold_df.limit(5).display()

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_fact_order_items)
orders_gold_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_fact_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC Sanity Check
# MAGIC
# MAGIC Checking the number of records 

# COMMAND ----------

spark.sql(f"SELECT count(*) FROM {catalog_name}.gold.gld_fact_order_items").show()

# COMMAND ----------

