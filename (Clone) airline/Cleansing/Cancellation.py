# Databricks notebook source
# MAGIC %md
# MAGIC #create sql table 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS cleansed_airline_db.Cancellation

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS cleansed_airline_db.Cancellation;
# MAGIC
# MAGIC CREATE TABLE cleansed_airline_db.Cancellation 
# MAGIC (
# MAGIC     Code VARCHAR(512),
# MAGIC     Description	VARCHAR(512)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO cleansed_airline_db.Cancellation (Code, Description) VALUES ('A', 'Carrier');
# MAGIC INSERT INTO cleansed_airline_db.Cancellation (Code, Description) VALUES ('B', 'Weather');
# MAGIC INSERT INTO cleansed_airline_db.Cancellation (Code, Description) VALUES ('C', 'National Air System');
# MAGIC INSERT INTO cleansed_airline_db.Cancellation (Code, Description) VALUES ('D', 'Security');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_airline_db.Cancellation
