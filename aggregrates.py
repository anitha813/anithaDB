# Databricks notebook source
yellow_rides_df = spark.read.table("mydata_silver1.yellow_cabs_trip")

# COMMAND ----------

display(yellow_rides_df)

# COMMAND ----------

yellow_rides_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import year,count
year_rides_df = yellow_rides_df.select(year("pickup_datetime").alias("year")) \
                               .groupBy("year")\
                               .agg(count('*').alias("total_no_of_trips")) \
                               .orderBy("year")
                            

# COMMAND ----------

display(year_rides_df)

# COMMAND ----------

from pyspark.sql.functions import year,count,month
year_rides_df = yellow_rides_df.select(year("pickup_datetime").alias("year"),month("pickup_datetime").alias("month"))\
                               .groupBy("year","month")\
                                   .agg(count("*").alias("total_no_of_trips")) \
                               .orderBy("year","month")

# COMMAND ----------

display(year_rides_df)

# COMMAND ----------

from pyspark.sql.functions import year, count, month, avg, col, dayofmonth, date_format,round

year_rides_df = yellow_rides_df.select(
  year("pickup_datetime").alias("year"),
  month("pickup_datetime").alias("month"),
  dayofmonth("pickup_datetime").alias("day_of_month"),
  'passenger_count',
  'fare_amount'
) \
.groupBy("year", "month") \
.agg(
  count("*").alias("total_no_of_trips"),
  avg("passenger_count").alias("avg_passenger_count"),
  avg("fare_amount").alias("avg_fare_amount_per_trip"),
  avg(col("fare_amount") / col("passenger_count")).alias("avg_amt_paid_by_customer")
) \
    .withColumn("avg_passenger_count", round(col("avg_passenger_count"), 2))\
    .withColumn("avg_fare_amount_per_trip", round(col("avg_fare_amount_per_trip"), 2))\
    .withColumn("avg_amt_paid_by_customer", round(col("avg_amt_paid_by_customer"), 2))\
.orderBy("year", "month")

# COMMAND ----------

display(year_rides_df)

# COMMAND ----------

from pyspark.sql.functions import year, count, month, avg, hour, date_format, desc, rank, round,col
from pyspark.sql.window import Window

year_rides_df = yellow_rides_df.select(
    year("pickup_datetime").alias("year"),
    month("pickup_datetime").alias("month"),
    date_format("pickup_datetime", "E").alias("day_of_week"),
    hour("pickup_datetime").alias("hour_of_week"),
    "passenger_count",
    "fare_amount"
).groupBy("year", "month", "day_of_week", "hour_of_week").agg(
    count("*").alias("total_no_of_trips"),
    avg("passenger_count").alias("avg_passenger_count"),
    avg("fare_amount").alias("avg_fare_amount"),
    avg(col("fare_amount") / col("passenger_count")).alias("avg_amt_paid_by_customer")
).withColumn("avg_passenger_count", round("avg_passenger_count", 2))\
.withColumn("avg_fare_amount", round("avg_fare_amount", 2))\
.withColumn("avg_amt_paid_by_customer", round("avg_amt_paid_by_customer", 2))\

windowSpec = Window.partitionBy("year", "month").orderBy(desc("total_no_of_trips"))

# Adding a rank column to the dataframe
ranked_trips_df = year_rides_df.withColumn("rank", rank().over(windowSpec))

# Filtering out the rows with rank 1, which represents the day with the most trips for each year and month
most_trips_per_month_df = ranked_trips_df.filter("rank == 1").drop("rank").orderBy("year", "month", "day_of_week", "hour_of_week")
year_rides_df = most_trips_per_month_df
year_rides_df = year_rides_df.filter(col("hour_of_week") != 0)

# COMMAND ----------

display(year_rides_df)
