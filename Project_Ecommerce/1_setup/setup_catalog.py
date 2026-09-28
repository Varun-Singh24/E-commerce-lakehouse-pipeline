# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce ; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze ; 
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver ; 
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce
# MAGIC .gold ; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC Show Databases from Ecommerce ; 

# COMMAND ----------

