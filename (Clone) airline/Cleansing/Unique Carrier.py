# Databricks notebook source
# MAGIC %run /Workspace/airline/Utility

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/dbfs/FileStore/tables/schema/UNIQUE_CARRIERS")
    .option("cloudFiles.schemaEvolutionMode", "rescue") 
    .load("/mnt/raw_datalake/UNIQUE_CARRIERS/")
)

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/UNIQUE CARRIERS',True)

# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/UNIQUE CARRIERS")\
    .start("/mnt/cleansed_datalake/UNIQUE CARRIERS")

# COMMAND ----------

# MAGIC %md
# MAGIC #create sql table 

# COMMAND ----------

f_delta_cleansed_load('unique_carrier','/mnt/cleansed_datalake/UNIQUE CARRIERS','cleansed_airline_db')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_airline_db.unique_carrier

# COMMAND ----------



# COMMAND ----------


