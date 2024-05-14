# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Group 6
# MAGIC #### Final Project

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

# Path + Prefix
# bronzeDB = f"/{databaseName}/bronze"
# silverDB = f"/{databaseName}/silver"
# goldDB = f"/{databaseName}/gold"

# lookupDB = f"{bronzeDB}"

# print('dataStore: ' + dataStore)
# print('bronzeDB: ' + bronzeDB)
# print('silverDB: ' + silverDB)
# print('goldDB: ' + goldDB)
# print('lookupDB: ' + lookupDB)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Database initialization

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")