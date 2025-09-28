# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_trip_zone=spark.read.format("parquet").option("inferSchema",True).option("header",True).load("abfss://container-reignite@storageprimaryy.dfs.core.windows.net/silver/trip_zone")
df_trip_zone.display()



# COMMAND ----------

# MAGIC %sql
# MAGIC create database demo_uc.trips;

# COMMAND ----------

df_trip_zone.write.format("delta")\
    .mode("append")\
    .option("path","abfss://container-reignite@storageprimaryy.dfs.core.windows.net/gold/trip_zone")\
    .saveAsTable("demo_uc.trips.trip_zone")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_uc.trips.trip_zone

# COMMAND ----------

df_trip_type=spark.read.format("parquet").option("inferSchema",True).option("header",True).load("abfss://container-reignite@storageprimaryy.dfs.core.windows.net/silver/trip_type")

# COMMAND ----------

df_trip_type.write.format("delta")\
    .mode("append")\
    .option("path","abfss://container-reignite@storageprimaryy.dfs.core.windows.net/gold/trip_type")\
    .saveAsTable("demo_uc.trips.trip_type")


# COMMAND ----------

df_trip=spark.read.format("parquet").option("inferSchema",True).option("header",True).load("abfss://container-reignite@storageprimaryy.dfs.core.windows.net/silver/trip_data")

# COMMAND ----------

df_trip.write.format("delta")\
    .mode("append")\
    .option("path","abfss://container-reignite@storageprimaryy.dfs.core.windows.net/gold/trip_data")\
    .saveAsTable("demo_uc.trips.trip_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_uc.trips.trip_data

# COMMAND ----------

# MAGIC %md
# MAGIC ##Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC update demo_uc.trips.trip_zone
# MAGIC set Borough = "EMR"
# MAGIC where LocationID =1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_uc.trips.trip_zone where LocationID=1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from demo_uc.trips.trip_zone
# MAGIC where LocationID=1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo_uc.trips.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_uc.trips.trip_zone where LocationID=1

# COMMAND ----------

# MAGIC %md
# MAGIC Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC restore demo_uc.trips.trip_zone to version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_uc.trips.trip_zone where LocationID=1