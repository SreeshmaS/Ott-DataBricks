# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Cleansing - Content Data

# Read from Bronze
df_content_raw = spark.table("workspace.bronze.content")

# 1. Column Selection - Keep only relevant columns
df_content_clean = df_content_raw.select("content_id", "title", "genre", "release_year")

# 2. NULL HANDLING - Replace nulls with default values
df_content_clean = df_content_clean.fillna({
    "content_id": "UNKNOWN_CONTENT",
    "title": "Unknown Title",
    "genre": "Unclassified",
    "release_year": 0
})

# 3. DEDUPLICATION - Remove duplicate content records
df_content_clean = df_content_clean.dropDuplicates(["content_id"])

print(f"Content Cleansing: {df_content_clean.count()} records after cleansing")
print(f"Records with 'Unknown Title': {df_content_clean.filter(col('title') == 'Unknown Title').count()}")
print(f"Records with 'Unclassified' genre: {df_content_clean.filter(col('genre') == 'Unclassified').count()}")

# COMMAND ----------

# DBTITLE 1,Silver - Joined Data with Watch Category
# Read from Bronze
df_watch = spark.table("workspace.bronze.watch_history")
df_content = df_content_clean  

# Join watch history with content
df_silver = df_watch.join(df_content, "content_id", "inner")

# Add watch_category using CASE WHEN logic
df_silver = df_silver.withColumn(
    "watch_category",
    when(col("watch_minutes") <= 30, "Short")
    .when(col("watch_minutes") <= 90, "Medium")
    .otherwise("Long")
)

# NULL
df_silver = df_silver.fillna({
    "watch_id": "MISSING_WATCH_ID",
    "user_id": "ANONYMOUS_USER",
    "watch_minutes": 0
})


# Deduplicate based on watch_id
df_silver = df_silver.dropDuplicates(["watch_id"])

# Write to Silver layer (overwriteSchema to handle new data_quality_flag column)
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.silver.watch_content")

print(f"Silver Layer: {df_silver.count()} records processed")
display(df_silver.limit(10))

# COMMAND ----------

# DBTITLE 1,Verify Watch Categories
SELECT 
    watch_category,
    COUNT(*) as count,
    ROUND(AVG(watch_minutes), 2) as avg_minutes
FROM workspace.silver.watch_content
GROUP BY watch_category
ORDER BY watch_category
