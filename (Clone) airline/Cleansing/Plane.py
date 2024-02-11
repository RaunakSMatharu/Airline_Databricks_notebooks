# Databricks notebook source
# MAGIC %run /Workspace/airline/Utility

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudfiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/PLANE")\
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .load('/mnt/raw_datalake/PLANE/')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/PLANE',True)

# COMMAND ----------

df_base=df.selectExpr("tailnum as tailid","type","manufacturer","to_date(issue_date) as issue_date","model","status","aircraft_type","engine_type","year","to_date(Date_part,'yyyy-MM-dd')as Date_part")
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/PLANE")\
    .start("/mnt/cleansed_datalake/plane")

# COMMAND ----------

# MAGIC %md
# MAGIC #create sql table 

# COMMAND ----------

#create sql table 
f_delta_cleansed_load('plane','/mnt/cleansed_datalake/plane','cleansed_airline_db')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_airline_db.plane

# COMMAND ----------


