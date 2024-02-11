# Databricks notebook source
# MAGIC %run /Workspace/airline/Utility

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudfiles.format","csv")\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/Airport")\
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .load('/mnt/raw_datalake/Airport/')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Airport',True)

# COMMAND ----------

df_base=df.selectExpr("Code",
                      "split(Description,',')[0] as city",
                      "split(split(Description,',')[1],':')[0] as country",
                      "split(split(Description,',')[1],':')[1] as airport_name",
                      "to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Airport")\
    .start("/mnt/cleansed_datalake/Airport")

# COMMAND ----------

# MAGIC %md
# MAGIC #create sql table 

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %py
# MAGIC def f_delta_cleansed_load(table_name,location,database):
# MAGIC     try:       
# MAGIC        # cleansed_airline_db
# MAGIC         schema=pre_schema(f'{location}')
# MAGIC         spark.sql(f"""DROP TABLE IF EXISTS {database}.{table_name}""");
# MAGIC         spark.sql(f"""create table {database}.{table_name}({schema})
# MAGIC         using delta
# MAGIC         location '{location}'
# MAGIC        """)
# MAGIC     except Exception as err:
# MAGIC         print("Error Occured",str(err))

# COMMAND ----------

#create sql table 
f_delta_cleansed_load('airport','/mnt/cleansed_datalake/Airport','cleansed_airline_db')

# COMMAND ----------

# MAGIC  %sql
# MAGIC select * from cleansed_airline_db.airport

# COMMAND ----------


