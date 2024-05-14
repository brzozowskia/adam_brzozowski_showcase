# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### <img style="float: left; padding-right: 10px; width: 45px" src="https://upload.wikimedia.org/wikipedia/en/8/89/ExtensionFlag.png"> CSCI E-103 Data Engineering for Analytics to Solve Business Challenges
# MAGIC
# MAGIC # Final Project | Group 6
# MAGIC
# MAGIC **Student Name: Adam Brzozowski, Sam Ippisch, Stefan Mcneil, Gordon Hew, Brandon Hong, Noura Almansoori**
# MAGIC
# MAGIC **Harvard University**<br/>
# MAGIC **Fall 2023**<br/>
# MAGIC **Instructors**: Eric Gieseke ALM, Chief Executive Officer and Founder, Pago Capital | Anindita Mahapatra ALM, Solutions Architect, Databricks\
# MAGIC **Teaching Fellows** Ramdas Murali | Sriharsha Tikkireddy
# MAGIC
# MAGIC
# MAGIC <hr style="height:2pt">
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Group 6
# MAGIC #### Final Project - Bronze: Energy Prediction Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Summary
# MAGIC
# MAGIC This job scans through the Enefit data directory for CSV files and loads them into a corresponding bronze table

# COMMAND ----------

import re

# COMMAND ----------

dbutils.widgets.text("schema", "fa2023_group06_enefit")

# COMMAND ----------

databaseName = dbutils.widgets.get("schema")
raw_data_path = "/mnt/data/2023-kaggle-final/energy-prediction/"

# COMMAND ----------

userName = spark.sql("SELECT CURRENT_USER").collect()[0]['current_user()']
userName0 = userName.split("@")[0]
userName0 = re.sub('[!#$%&\'*+-/=?^`{}|\.]+', '_', userName0)
userName1 = userName.split("@")[1]
userName = f'{userName0}@{userName1}'
dbutils.fs.mkdirs(f"/Users/{userName}/data")
userDir = f"/Users/{userName}/data"

dataStore = "/FileStore/group_06/" + databaseName

print('databaseName ' + databaseName)
print('UserDir ' + userDir)
print('userName '+ userName)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")

# COMMAND ----------

def create_bronze_tables(data_path, write_mode='overwrite'):
  files = dbutils.fs.ls(data_path)

  csv_ext = '.csv'

  for file in files:
    if (file.name[len(file.name) - len(csv_ext):] == csv_ext):
      table_name = 'bronze_' + file.name[:len(file.name) - len(csv_ext)]
      data_df = spark.read.format("csv").load(file.path, header=True, inferSchema=True)
      print('creating', table_name, 'using ', write_mode, 'mode')
      data_df.write.mode(write_mode).saveAsTable(table_name)


# COMMAND ----------

create_bronze_tables(raw_data_path)