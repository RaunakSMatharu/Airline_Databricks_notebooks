# Databricks notebook source
# MAGIC %scala 
# MAGIC val containerName=dbutils.secrets.get(scope="airline-secret",key="blob-cointainer-name")//"source"
# MAGIC val storageAccountName=dbutils.secrets.get(scope="airline-secret",key="storage-account-name")//"airlinestorageaccsource"
# MAGIC val sas =dbutils.secrets.get(scope="airline-secret",key="blob-sas-token")
# MAGIC val config="fs.azure.sas."+containerName+"."+storageAccountName+".blob.core.windows.net"
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC     source=dbutils.secrets.get(scope="airline-secret",key="blob-mnt-path"),
# MAGIC     mountPoint="/mnt/source_blob/",
# MAGIC     extraConfigs=Map(config -> sas)
# MAGIC )

# COMMAND ----------

# MAGIC %py
# MAGIC dbutils.fs.ls('mnt/source_blob/')

# COMMAND ----------

# MAGIC %py
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="airline-secret",key="data-app-id"),
# MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="airline-secret",key="data-app-secret"),
# MAGIC           "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="airline-secret",key="data-client-refreash-url")}
# MAGIC
# MAGIC # Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC
# MAGIC mountPoint="/mnt/raw_datalake/"
# MAGIC if not any(mount.mountPoint==mountPoint for mount in dbutils.fs.mounts()):
# MAGIC     dbutils.fs.mount(
# MAGIC     source = dbutils.secrets.get(scope="airline-secret",key="datalake-raw"),
# MAGIC     mount_point = mountPoint,
# MAGIC     extra_configs = configs)
# MAGIC
# MAGIC    # abfss://raw@airlinedatalakedev.dfs.core.windows.net/

# COMMAND ----------

dbutils.fs.ls('/mnt/raw_datalake/')

# COMMAND ----------

# MAGIC %py
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="airline-secret",key="data-app-id"),
# MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="airline-secret",key="data-app-secret"),
# MAGIC           "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="airline-secret",key="data-client-refreash-url")}
# MAGIC
# MAGIC # Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC
# MAGIC mountPoint="/mnt/cleansed_datalake/"
# MAGIC if not any(mount.mountPoint==mountPoint for mount in dbutils.fs.mounts()):
# MAGIC     dbutils.fs.mount(
# MAGIC     source = dbutils.secrets.get(scope="airline-secret",key="datalake-cleansed"),
# MAGIC     mount_point = mountPoint,
# MAGIC     extra_configs = configs)
# MAGIC

# COMMAND ----------


