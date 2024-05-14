# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Group 6
# MAGIC #### Final Project - Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Development Environment Setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Variables definition

# COMMAND ----------

import re

dataStore = "/FileStore/group_06/"
databaseName = "group_06"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Database initialization

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Streaming

# COMMAND ----------

## Stop all active streams

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

# Delete checkpoint before start streaming
dbutils.fs.rm("/tmp/_checkpoint", True)

# Read the data from the bronze_properties Delta table as a streaming DataFrame
streaming_properties = spark.readStream.format("delta").table("silver_properties")

# Store stream
query = (
    streaming_properties
    .writeStream
    .trigger(once = True)
    .outputMode("append")
    .format("delta")
    .option("path", "streaming_properties")
    .option("checkpointLocation", "/tmp/_checkpoint")
    .start()
)

# COMMAND ----------

## Stop all active streams

for stream in spark.streams.active:
    stream.stop()