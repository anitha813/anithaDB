# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/nyctaxi/

# COMMAND ----------

yellow_df = spark.read.option("header", True).csv("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/")
display(yellow_df)

# COMMAND ----------

taxi_payment_df=  spark.read.option("header", True).csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")
display(taxi_payment_df)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType,StringType,DoubleType,IntegerType
taxi_payment_df = taxi_payment_df.withColumn("payment_type",F.col("payment_type").cast(IntegerType()))

# COMMAND ----------

taxi_rate_code_df=  spark.read.option("header", True).csv("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv")
display(taxi_rate_code_df)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType,StringType,DoubleType,IntegerType
taxi_rate_code_df =taxi_rate_code_df.withColumn("RateCodeID",F.col("RateCodeID").cast(IntegerType()))

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType,StringType,DoubleType,IntegerType,DecimalType
yellow_df = yellow_df\
    .withColumn("vendor_id",F.col("vendor_id").cast(IntegerType()))\
    .withColumn("pickup_datetime",F.col("pickup_datetime").cast(TimestampType()))\
    .withColumn("dropoff_datetime",F.col("dropoff_datetime").cast(TimestampType()))\
    .withColumn("passenger_count",F.col("passenger_count").cast(IntegerType()))\
    .withColumn("trip_distance",F.col("trip_distance").cast(DoubleType()))\
     .withColumn("pickup_longitude",F.col("pickup_longitude").cast(TimestampType()))\
     .withColumn("pickup_latitude",F.col("pickup_latitude").cast(TimestampType()))\
    .withColumn("rate_code",F.col("rate_code").cast(StringType()))\
    .withColumn("store_and_fwd_flag",F.col("store_and_fwd_flag").cast(StringType()))\
    .withColumn("dropoff_longitude",F.col("dropoff_longitude").cast(TimestampType()))\
    .withColumn("dropoff_latitude",F.col("dropoff_latitude").cast(TimestampType()))\
    .withColumn("payment_type",F.col("payment_type").cast(StringType()))\
    .withColumn("surcharge",F.col("surcharge").cast(StringType()))\
    .withColumn("fare_amount",F.col("fare_amount").cast(DoubleType()))\
    .withColumn("mta_tax",F.col("mta_tax").cast(DoubleType()))\
    .withColumn("tip_amount",F.col("tip_amount").cast(DoubleType()))\
    .withColumn("tolls_amount",F.col("tolls_amount").cast(DoubleType()))\
    .withColumn("total_amount",F.col("total_amount").cast(DoubleType()))\





    

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists mydata_bronze1

# COMMAND ----------

taxi_payment_df.write.mode("append").saveAsTable("mydata_bronze1.payment_ref")

# COMMAND ----------

taxi_rate_code_df.write.mode("append").saveAsTable("mydata_bronze1.rate_code_ref")

# COMMAND ----------

yellow_df.write.mode("append").saveAsTable("mydata_bronze1.yellow_ref")
