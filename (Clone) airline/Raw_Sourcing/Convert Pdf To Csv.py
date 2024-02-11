# Databricks notebook source
!pip install tabula-py

# COMMAND ----------

import tabula
from datetime import date
import os
print(date.today())
today=str(date.today())

# COMMAND ----------

#from tabula import convert_into

# COMMAND ----------

dbutils.fs.ls('mnt/source_blob')

# COMMAND ----------

#output_file_path=f'/dbfs/mnt/raw_datalake/PLANE/Date_Part={date.today()}/PLANE.csv'

# COMMAND ----------

#output_dir = f'/dbfs/mnt/raw_datalake/PLANE/Date_Part={date.today()}/'
#if not os.path.exists(output_dir):
#    os.makedirs(output_dir)

# COMMAND ----------

#tabula.convert_into('/dbfs/mnt/source_blob/PLANE.pdf',output_file_path,output_format='csv',pages='all')

# COMMAND ----------

list_file=[(i.name,i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if(i.name.split('.')[1]=='pdf')]
print(list_file)

# COMMAND ----------

def f_source_pdf_datalake(source_path,sink_path,output_format,page,file_name):
    try:
        dbutils.fs.mkdirs(f"/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/")
        tabula.convert_into(f'{source_path}{file_name}',f"/dbfs/{sink_path}/{file_name.split('.')[0]}/Date_Part={date.today()}/{file_name.split('.')[0]}.{output_format}",output_format=output_format,pages=page)
    except Exception as err:
        print("error occured",str(err))

# COMMAND ----------

list_file=[(i.name,i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if(i.name.split('.')[1]=='pdf')]
for i in list_file:
    f_source_pdf_datalake('/dbfs/mnt/source_blob/','mnt/raw_datalake/','csv','all',i[0])
