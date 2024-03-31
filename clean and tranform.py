# Databricks notebook source
payment_df = spark.read.table("mydata_bronze1.payment_ref")
rates_df = spark.read.table("mydata_bronze1.rate_code_ref")
zone_df = spark.read.table("mydata_bronze1.zone_lookup_ref")
yellow_df = spark.read.table("mydata_bronze1.yellow_ref")

# COMMAND ----------

display(rates_df)

# COMMAND ----------

rates_df = rates_df.withColumnRenamed("RateCodeID","rate_code_id").withColumnRenamed("RateCodeDesc","rate_code_desc")

# COMMAND ----------

display(payment_df)

# COMMAND ----------

payment_df = payment_df.withColumnRenamed("payment_type", "payment_type_id")

# COMMAND ----------

spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
display(yellow_df.select("total_amount").summary("count","min","max","25%","50%","max"))

# COMMAND ----------

display(yellow_df.select("trip_distance").summary("count","min","max","25%","50%","max"))

# COMMAND ----------

display(yellow_df.select("passenger_count").summary("count","min","max","25%","50%","max"))

# COMMAND ----------

import pyspark.sql.functions as f
clean_yellow_df = yellow_df.filter((f.col("passenger_count")> 1) & (f.col("passenger_count")< 6))

# COMMAND ----------

clean_yellow_df = yellow_df.filter((f.col("trip_distance")< 1) & (f.col("trip_distance")> 500))
clean_yellow_df = yellow_df.filter((f.col("total_amount")> 1) & (f.col("trip_distance")< 500))


# COMMAND ----------

clean_yellow_df = yellow_df.filter((f.col("vendor_id").isNotNull()) & (f.col("rate_code").isNotNull()) & (f.col("store_and_fwd_flag").isNotNull()))

# COMMAND ----------

display(clean_yellow_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA if NOT EXISTS mydata_silver1

# COMMAND ----------

enriched_yellow_df = clean_yellow_df.withColumn("pickup_day_of_week",f.date_format(f.col("pickup_datetime"), "E"))\
                                      .withColumn("dropoff_day_of_week",f.date_format(f.col("dropoff_datetime"), "E"))\
                                      .withColumn("hour_of_day",f.hour(f.col("pickup_datetime")) +1)\
                                      .withColumn("peak_or_nonpeak",f.expr("Case WHEN hour_of_day >7 AND hour_of_day <19 THEN 'peak' ELSE 'nonpeak' END" ))\
                                      .withColumn("month_of_year",f.month(f.col("pickup_datetime")))
                                      

# COMMAND ----------

display(enriched_yellow_df)

# COMMAND ----------

yellow_rates_df = enriched_yellow_df.join(rates_df,enriched_yellow_df.rate_code == rates_df.rate_code_id, "leftouter")
display(yellow_rates_df)

# COMMAND ----------

display(rates_df)

# COMMAND ----------

yellow_payment_df = yellow_rates_df.join(payment_df,enriched_yellow_df.payment_type == payment_df.payment_type_id, "leftouter")

# COMMAND ----------

display(yellow_payment_df)

# COMMAND ----------

yellow_payment_df.write.mode("overwrite").saveAsTable("mydata_silver1.yellow_cabs_trip")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  mydata_silver1.yellow_cabs_trip
