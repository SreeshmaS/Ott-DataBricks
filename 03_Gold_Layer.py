# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold - Genre Analytics
from pyspark.sql.functions import *
# Read from Silver
df_silver = spark.table("workspace.silver.watch_content")

# Genre Analytics: total views and total watch time by genre
df_genre_analytics = df_silver.groupBy("genre").agg(count("watch_id").alias("total_views"),sum("watch_minutes").alias("total_watch_time")).orderBy(col("total_views").desc())

# Write to Gold layer
df_genre_analytics.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.genre_analytics")

print(f"Genre Analytics: {df_genre_analytics.count()} genres analyzed")
display(df_genre_analytics)

# COMMAND ----------

# DBTITLE 1,Gold - User Engagement with Rank
from pyspark.sql.window import Window
from pyspark.sql.functions import *
# Read from Silver
df_silver = spark.table("workspace.silver.watch_content")

# User Engagement: total watch time per user with ranking
df_user_engagement = df_silver.groupBy("user_id").agg(sum("watch_minutes").alias("watch_time"))

# Add window function to rank users by watch time
window_spec = Window.orderBy(col("watch_time").desc())
df_user_engagement = df_user_engagement.withColumn("rank",dense_rank().over(window_spec))

# Write to Gold layer
df_user_engagement.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.user_engagement")

print(f"User Engagement: {df_user_engagement.count()} users ranked")
display(df_user_engagement.orderBy("rank").limit(20))

# COMMAND ----------

# DBTITLE 1,Gold - All Ranking Functions
# Read from Silver
df_silver = spark.table("workspace.silver.watch_content")

df_user_ranking = df_silver.groupBy("user_id").agg(sum("watch_minutes").alias("watch_time")
)

window_spec = Window.orderBy(col("watch_time").desc())

# Add ALL three ranking functions: row_number, rank, dense_rank
df_user_ranking = df_user_ranking.withColumn("row_number",row_number().over(window_spec)).withColumn("rank",rank().over(window_spec)).withColumn("dense_rank",dense_rank().over(window_spec))

# Write to Gold layer
df_user_ranking.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.user_ranking")
display(df_user_ranking)

# COMMAND ----------

# DBTITLE 1,Gold - Daily Watch Time with Lag/Lead
# Read from Silver
df_silver = spark.table("workspace.silver.watch_content")

# Daily watch time per user
df_daily_watch = df_silver.groupBy("user_id", "watch_date").agg(sum("watch_minutes").alias("daily_watch_time"))


# Add lag and lead functions
window_spec = Window.partitionBy("user_id").orderBy("watch_date")
df_daily_watch = df_daily_watch.withColumn("previous_session_watch_time",lag("daily_watch_time", 1).over(window_spec)).withColumn("next_session_watch_time",lead("daily_watch_time", 1).over(window_spec))

# Find users whose watch time increased from previous session
df_daily_watch = df_daily_watch.withColumn("watch_time_increased",when((col("previous_session_watch_time").isNotNull()) & (col("daily_watch_time") > col("previous_session_watch_time")),True).otherwise(False)).withColumn("increase_amount",when(col("watch_time_increased") == True,col("daily_watch_time") - col("previous_session_watch_time")).otherwise(0))

# Write to Gold layer
df_daily_watch.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.daily_user_watch")

display(df_daily_watch.limit(10))

# COMMAND ----------

# DBTITLE 1,Delta Merge - Update Genre Analytics
# Delta Merge - Upsert Operations
from delta.tables import DeltaTable
from pyspark.sql.functions import col 
new_data = [
    ("Action", 150, 8500),
    ("Drama", 100, 6000),
    ("Comedy-Horror",100,10000)

]

df_new = spark.createDataFrame(new_data, ["genre", "total_views", "total_watch_time"])

# Get Delta table
delta_table = DeltaTable.forName(spark, "workspace.gold.genre_analytics")

# Perform merge
delta_table.alias("target").merge(df_new.alias("source"),"target.genre = source.genre").whenMatchedUpdate(set={"total_views": col("target.total_views") + col("source.total_views"),"total_watch_time": col("target.total_watch_time") + col("source.total_watch_time")}
).whenNotMatchedInsert(values={"genre": col("source.genre"),"total_views": col("source.total_views"),"total_watch_time": col("source.total_watch_time")}).execute()

display(spark.table("workspace.gold.genre_analytics"))

# COMMAND ----------

# DBTITLE 1,Time Travel - Version History
#Delta Time Travel
#  STEP 1: Show version history of gold_genre_analytics
history_df = spark.sql("DESCRIBE HISTORY workspace.gold.genre_analytics")

# STEP 2: Query Version 0 (initial state - before data quality fixes)
df_version_0 = spark.read.format("delta").option("versionAsOf", 0).table("workspace.gold.genre_analytics")
display(df_version_0)

# STEP 3: Query latest version (current state - after all fixes and merge)
df_latest_version = spark.read.format("delta").table("workspace.gold.genre_analytics")
display(df_latest_version)


# COMMAND ----------

# DBTITLE 1,Show All Gold Tables
# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.gold
