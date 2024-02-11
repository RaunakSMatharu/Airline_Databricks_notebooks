# Databricks notebook source
# Databricks notebook source
%run /Workspace/airline/Utility

# COMMAND ----------

# COMMAND ----------
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/dbfs/FileStore/tables/schema/flight")
    .load("/mnt/raw_datalake/flight/")
)

# COMMAND ----------

# COMMAND ----------
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

# COMMAND ----------

from pyspark.sql.functions import concat_ws

df_base = df.selectExpr(
"to_date(concat_ws('-',year,month,dayofmonth),'yyyy-MM-dd') as date",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as deptime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSDepTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as ArrTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSArrTime",
"UniqueCarrier",
"cast(FlightNum as int) as FlightNum",
"cast(TailNum as int) as TailNum" ,
"cast(ActualElapsedTime as int) as ActualElapsedTime",
"cast(CRSElapsedTime as int) as CRSElapsedTime",
"cast(AirTime as int) as AirTime",
"cast(ArrDelay as int) as ArrDelay",
"cast(DepDelay as int) as DepDelay",
 "Origin",
 "Dest",
 "cast(Distance as int) as  Distance",
 "cast(TaxiIn as int) as TaxiIn",
 "cast(TaxiOut as int) as TaxiOut",
 "Cancelled",
 "CancellationCode",
 "cast(Diverted as int) as castDiverted",
 "cast(CarrierDelay as int) as CarrierDelay",
 "cast(WeatherDelay as int) as WeatherDelay" ,
 "cast(NASDelay as int) as NASDelay",
 "cast(SecurityDelay as int) as SecurityDelay",
 "cast(LateAircraftDelay as int) as LateAircraftDelay" ,
 "to_date(Date_Part,'yyyy-MM-dd') as Date_Part "
)

df_base.writeStream.trigger(once=True).format("delta").option(
    "checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/flight"
).start("/mnt/cleansed_datalake/flight")




# COMMAND ----------

#create sql table 
f_delta_cleansed_load('flight','/mnt/cleansed_datalake/flight','cleansed_airline_db')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cleansed_airline_db.flight

# COMMAND ----------

# MAGIC %py
# MAGIC spark.sql("""desc HISTORY cleansed_airline_db.flight""").createOrReplaceTempView("Table_count")

# COMMAND ----------



# COMMAND ----------

display(count_curr)

# COMMAND ----------



# COMMAND ----------

print(count__previous.first())

# COMMAND ----------



# COMMAND ----------


