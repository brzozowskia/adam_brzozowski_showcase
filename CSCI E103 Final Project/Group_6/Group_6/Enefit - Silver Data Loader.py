# Databricks notebook source
import re

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("schema", "fa2023_group06_enefit_train")
dbutils.widgets.dropdown("dataset", "train", ["train", "test"])

# COMMAND ----------

databaseName = dbutils.widgets.get("schema")
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

print('databaseName ' + databaseName)
print('UserDir ' + userDir)
print('userName '+ userName)

# COMMAND ----------

spark.sql(f"use {databaseName}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Creating/Updating silver_weather_station_to_county_mapping table 

# COMMAND ----------

if dataset == 'train':
  bronze_weather_station_to_county_mapping_sql = """
  SELECT county_name, longitude, latitude, county FROM {databaseName}.bronze_weather_station_to_county_mapping
  """.format(databaseName=databaseName)

  bronze_weather_station_to_county_mapping_df = spark.sql(bronze_weather_station_to_county_mapping_sql)
  bronze_weather_station_to_county_mapping_original_count = bronze_weather_station_to_county_mapping_df.count()
  bronze_weather_station_to_county_mapping_df = bronze_weather_station_to_county_mapping_df.na.drop(how='any')
  bronze_weather_station_to_county_mapping_new_count = bronze_weather_station_to_county_mapping_df.count()

  print('original count:', bronze_weather_station_to_county_mapping_original_count, 'new count:', bronze_weather_station_to_county_mapping_new_count, 'after dropping rows with any nulls')

  # we overwrite as the bronze table is always overridden as its a reference table
  bronze_weather_station_to_county_mapping_df.write.mode('overwrite').saveAsTable('silver_weather_station_to_county_mapping')

# COMMAND ----------

BRONZE_META_DATA_COLUMNS = ['file_path', 'file_name', 'file_size', 'file_modification_time', 'run_id', 'job_group', 'process_timestamp']

def create_update_silver_tables(database_name, logical_table_name, silver_columns):

  max_timestamp_clause = ""

  for idx, bronze_column in enumerate(silver_columns):
    if idx == 0:
      max_timestamp_clause = "b1." + bronze_column + " = b2." + bronze_column
    else:
      max_timestamp_clause = max_timestamp_clause + " AND b1." + bronze_column + " = b2." + bronze_column

  bronze_full_table_name = database_name + ".bronze_" + logical_table_name
  bronze_sql = """
  SELECT * 
  FROM {full_table_name} b1
  WHERE process_timestamp = (SELECT MAX(process_timestamp) FROM {full_table_name} b2 WHERE {clause})
  """.format(full_table_name=bronze_full_table_name, clause=max_timestamp_clause)

  bronze_df = spark.sql(bronze_sql)

  print('Retrieved', bronze_df.count(), 'results from', bronze_full_table_name)

  silver_table_name = 'silver_' + logical_table_name
  silver_full_table_name = database_name + "." + silver_table_name

  if spark.catalog.tableExists(silver_full_table_name):
    silver_df = spark.sql("SELECT * FROM " + silver_full_table_name)
    new_bronze_df = bronze_df.join(silver_df, on=silver_columns, how='left_anti')
  else:
    new_bronze_df = bronze_df

  print('silver table', silver_full_table_name)   
  new_bronze_df = new_bronze_df.drop(*BRONZE_META_DATA_COLUMNS)
  new_bronze_df_original_count = new_bronze_df.count()
  new_bronze_df = new_bronze_df.na.drop(how='any')
  new_bronze_df_count = new_bronze_df.count()

  print('original count:', new_bronze_df_original_count, 'new count:', new_bronze_df_count, 'after dropping rows with any nulls')

  new_bronze_df.write.mode('append') \
    .partitionBy("data_block_id") \
    .saveAsTable(silver_full_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating silver feature tables

# COMMAND ----------

#TODO: need to refine keys
create_update_silver_tables(databaseName, 'client', ['product_type', 'county', 'eic_count', 'installed_capacity', 'is_business', 'date', 'data_block_id'])
create_update_silver_tables(databaseName, 'electricity_prices', ['forecast_date', 'origin_date', 'data_block_id'])
create_update_silver_tables(databaseName, 'forecast_weather', ['latitude', 'longitude', 'origin_datetime', 'hours_ahead', 'data_block_id', 'forecast_datetime'])
create_update_silver_tables(databaseName, 'gas_prices', ['forecast_date', 'origin_date', 'data_block_id'])
create_update_silver_tables(databaseName, 'historical_weather', ['datetime', 'latitude', 'longitude', 'data_block_id'])
create_update_silver_tables(databaseName, dataset, ['row_id'])