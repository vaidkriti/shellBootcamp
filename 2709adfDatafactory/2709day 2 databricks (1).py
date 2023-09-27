# Databricks notebook source
# dbutils.fs.mount(
#   source = "wasbs://raw@kritistorage.blob.core.windows.net",
#   mount_point = "/mnt/kritistorage/raw",
#   extra_configs = {"fs.azure.account.key.kritistorage.blob.core.windows.net":"fpHR+OYSu55mqqP31uJ4mfQ0k9fM1jnVMiFUTMCyDI5tSl+nxE3EtqhgkRcXh2iCx5+Kty04Rcw/+AStE9WqRg=="})

# COMMAND ----------

# MAGIC %fs ls
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/kritistorage/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/kritistorage/raw/

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/kritistorage/raw/json/")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists json

# COMMAND ----------

df1.write.mode("overwrite").option("path","dbfs:/mnt/kritistorage/raw/json/kriti/json").saveAsTable("json.bronzetable")

# COMMAND ----------



# COMMAND ----------


