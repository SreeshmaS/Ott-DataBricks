# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Bronze - Watch History
# Read watch history
df_watch_history = spark.table("workspace.default.watch_history")
# Write to Bronze layer
df_watch_history.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.watch_history")
display(df_watch_history.limit(5))

# COMMAND ----------

# DBTITLE 1,Bronze - Content
# Read content from source AS-IS (raw data, no transformations)
df_content = spark.table("workspace.default.content")
# Write to Bronze layer (raw data preservation)
df_content.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.content")
display(df_content.limit(10))

# COMMAND ----------

# DBTITLE 1,Show Bronze Tables
# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.bronze
