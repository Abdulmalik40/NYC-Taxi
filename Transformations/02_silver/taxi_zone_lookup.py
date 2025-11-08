# Databricks notebook source
from pyspark.sql.functions import current_timestamp , lit , col
from pyspark.sql.types import TimestampType , IntegerType

# COMMAND ----------

df = spark.read.format("csv").option("header" , "true").load("/Volumes/nyctaxi/00_landing/data_srouces/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

df.describe()

# COMMAND ----------

df = df.select(
    col("LocationID").cast(IntegerType()).alias("locationID"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC     ALTER TABLE nyctaxi.02_silver.taxi_zone_lookup
# MAGIC ADD COLUMNS (
# MAGIC   service_zone STRING
# MAGIC );
# MAGIC

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

