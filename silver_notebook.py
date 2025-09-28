# Databricks notebook source
display(dbutils.fs.ls("abfss://container-reignite@storageprimaryy.dfs.core.windows.net/bronze"))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# reading csv data
df_trip_type = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://container-reignite@storageprimaryy.dfs.core.windows.net/bronze/trip_type/trip_type.csv")

# COMMAND ----------

display(df_trip_type)

# COMMAND ----------

df_trip_zone = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://container-reignite@storageprimaryy.dfs.core.windows.net/bronze/trip_zone/")

# COMMAND ----------

display(df_trip_zone)

# COMMAND ----------

myschema = '''
    VendorID BIGINT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    RatecodeID BIGINT,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type BIGINT,
    trip_type BIGINT,
    congestion_surcharge DOUBLE
'''
df_trip=spark.read.format("parquet")\
    .schema(myschema)\
    .option("header",True)\
    .option("recursiveFileLookup",True)\
    .load("abfss://container-reignite@storageprimaryy.dfs.core.windows.net/bronze/trip-data/")

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Change Column Name

# COMMAND ----------

df_trip_type=df_trip_type.withColumnRenamed("description","trip_description")
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://container-reignite@storageprimaryy.dfs.core.windows.net/silver/trip_type/")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Splitting  and indexing the column Zone

# COMMAND ----------

df_trip_zone=df_trip_zone.withColumn("zone1",split(col("zone"),"/")[0])\
    .withColumn("zone2",split(col("zone"),"/")[1])
df_trip_zone.display()


# COMMAND ----------

df_trip_zone.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://container-reignite@storageprimaryy.dfs.core.windows.net/silver/trip_zone/")\
    .save()

# COMMAND ----------

df_trip = df_trip.withColumn("date",to_date(col("lpep_pickup_datetime")))\
    .withColumn("year",year(col("lpep_pickup_datetime")))\
    .withColumn("month",month(col("lpep_pickup_datetime")))


# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip=df_trip.select("VendorID","PULocationID","DOLocationID","trip_distance","fare_amount","total_amount")

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://container-reignite@storageprimaryy.dfs.core.windows.net/silver/trip_data/")\
    .save()