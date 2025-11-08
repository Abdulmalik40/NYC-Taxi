# Databricks notebook source
from pyspark.sql.functions import col , when , timestamp_diff

# COMMAND ----------

df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

 df = df.filter(
  (col("tpep_pickup_datetime") >= "2025-01-01") &
  (col("tpep_pickup_datetime") < "2025-07-01")
)

# COMMAND ----------

df = df.select(
    when(col("VendorID") == 1 , "Creative Mobile Technologies, LLC")
    .when(col("VendorID") == 2 , "Crub Mobility, LLC")
    .when(col("VendorID") == 1 , "Myle Technologies Inc")
    .when(col("VendorID") == 7 , "Helix")
    .otherwise("Unkown")
    .alias("vendor"),
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    timestamp_diff("MINUTE",df.tpep_pickup_datetime,df.tpep_dropoff_datetime).alias("trip_duration"),
    "passenger_count",
    "trip_distance",

    when(col("RatecodeID") == 1 , "Standard Rate")
     .when(col("RatecodeID") == 2 , "JFK")
     .when(col("RatecodeID") == 3 , "Newark")
     .when(col("RatecodeID") == 4 , "Nassau or Westchester")
     .when(col("RatecodeID") == 5 , "Negotiated Fare")
     .when(col("RatecodeID") == 6 , "Group Ride")
     .otherwise("Unkown")
     .alias("rate_type"),
     "store_and_fwd_flag",
     col("PULocationID").alias("pu_location_id"),
     col("DOLocationID").alias("do_location_id"),
     
     when(col("payment_type") == 0 , "Flex Fare trip")
        .when(col("payment_type") == 1 , "Credit Card")
        .when(col("payment_type") == 2 , "Cash")
        .when(col("payment_type") == 3 , "No charge")
        .when(col("payment_type") == 4 , "Dispute")
        .when(col("payment_type") == 6 , "Voided trip")
        .otherwise("Unkown")
        .alias("payment_type"),
    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "processed_timestamp"    
    
)

# COMMAND ----------

df_fixed = df.withColumnRenamed("pu_location_id", "pu_loacation_id")
df_fixed.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.yellow_trips_cleansed")

# COMMAND ----------

