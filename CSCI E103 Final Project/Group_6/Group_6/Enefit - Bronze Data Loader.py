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
# MAGIC # Final Project | Group 6 | Energy Prediction Dataset
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
# MAGIC #### Bronze Data Loader

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC This job scans through the Enefit data directory for CSV files and loads them into a corresponding bronze table

# COMMAND ----------

import re
import json, pprint

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("schema", "fa2023_group06_enefit_train")
dbutils.widgets.text("data_block_ids_to_load", "400")
dbutils.widgets.dropdown("dataset", "train", ["train", "test"])
dbutils.widgets.text("data_files_path", "/mnt/data/2023-kaggle-final/energy-prediction/")

# COMMAND ----------

databaseName = dbutils.widgets.get("schema")
raw_data_path = dbutils.widgets.get("data_files_path")
dataset = dbutils.widgets.get("dataset")

# COMMAND ----------

userName = spark.sql("SELECT CURRENT_USER").collect()[0]['current_user()']
userName0 = userName.split("@")[0]
userName0 = re.sub('[!#$%&\'*+-/=?^`{}|\.]+', '_', userName0)
userName1 = userName.split("@")[1]
userName = f'{userName0}@{userName1}'
dbutils.fs.mkdirs(f"/Users/{userName}/data")
userDir = f"/Users/{userName}/data"

dataStore = "/FileStore/group_06/" + databaseName

job_run_dict = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
run_id = job_run_dict['currentRunId']

if run_id is not None:
  run_id = run_id['id']

job_group = job_run_dict['jobGroup']

print('databaseName ' + databaseName)
print('UserDir ' + userDir)
print('userName '+ userName)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")
print('using database', databaseName)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_loading_checkpoint (
# MAGIC   DATA_BLOCK_ID INT NOT NULL,
# MAGIC   LOAD_TIMESTAMP TIMESTAMP NOT NULL
# MAGIC );

# COMMAND ----------


blocks_to_load = int(dbutils.widgets.get("data_block_ids_to_load"))

bronze_loading_checkpoint = spark.sql("SELECT data_block_id FROM bronze_loading_checkpoint").take(1)

if len(bronze_loading_checkpoint) == 0:
  last_loaded_block = 0
else:
  last_loaded_block = bronze_loading_checkpoint[0].data_block_id

print('Last loaded block data id into bronze tables', last_loaded_block)

max_data_block_id = last_loaded_block + blocks_to_load
print('Loading up to data block id', max_data_block_id)

# COMMAND ----------

def create_bronze_tables(data_path, last_loaded_block_id, max_data_block_id):
  files = dbutils.fs.ls(data_path)

  csv_ext = '.csv'

  max_loaded_data_block_id = last_loaded_block_id

  for file in files:
    if (file.name[len(file.name) - len(csv_ext):] == csv_ext):
      print('reading file', file.name)

      table_name = 'bronze_' + file.name[:len(file.name) - len(csv_ext)]
      data_df = spark.read.format("csv").load(file.path, header=True, inferSchema=True)

      data_df = data_df.withColumn("file_path", F.lit(file.path)) \
        .withColumn("file_name", F.lit(file.name)) \
        .withColumn("file_size", F.lit(file.size)) \
        .withColumn("file_modification_time", F.lit(file.modificationTime)) \
        .withColumn("run_id", F.lit(run_id).cast(StringType())) \
        .withColumn("job_group", F.lit(job_group)) \
        .withColumn("process_timestamp", F.current_timestamp())

      if 'data_block_id' in data_df.columns and max_data_block_id > 0:
        data_df = data_df.filter((data_df.data_block_id < max_data_block_id) & (data_df.data_block_id > last_loaded_block_id))


        # assumes last loaded is the highest
        if len(data_df.tail(1)) > 0:
          last_loaded_data_block_id = data_df.tail(1)[0].data_block_id        
          max_loaded_data_block_id = last_loaded_data_block_id if last_loaded_data_block_id > max_loaded_data_block_id else max_loaded_data_block_id
          
          data_df.write.mode('append') \
            .partitionBy("data_block_id") \
            .saveAsTable(table_name)

          print('saving', table_name)
        else:
          print('no new data to load from', file.name)
      else:
        # reference tables 
        data_df.write.mode('overwrite').saveAsTable(table_name)
        print('saving', table_name)
        
  spark.sql("INSERT OVERWRITE bronze_loading_checkpoint VALUES (" + str(max_loaded_data_block_id) + ", CURRENT_TIMESTAMP())")

# COMMAND ----------

create_bronze_tables(raw_data_path, last_loaded_block, max_data_block_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_loading_checkpoint