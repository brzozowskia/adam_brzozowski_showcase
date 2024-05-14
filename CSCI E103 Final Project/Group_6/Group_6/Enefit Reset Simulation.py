# Databricks notebook source
import re

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("train_schema", "fa2023_group06_enefit_train")
dbutils.widgets.text("test_schema", "fa2023_group06_enefit_test")

# COMMAND ----------

train_schema = dbutils.widgets.get("train_schema")
spark.sql(f"use {train_schema}")
print('using database', train_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold_all_features;
# MAGIC DROP TABLE IF EXISTS gold_consumption_production_aggregate;
# MAGIC DROP TABLE IF EXISTS gold_electricity_aggregate;
# MAGIC DROP TABLE IF EXISTS gold_gas_prices_aggregate;
# MAGIC DROP TABLE IF EXISTS gold_energy_prices_aggregate;
# MAGIC DROP TABLE IF EXISTS gold_train;
# MAGIC
# MAGIC DROP TABLE IF EXISTS silver_client;
# MAGIC DROP TABLE IF EXISTS silver_electricity_prices;
# MAGIC DROP TABLE IF EXISTS silver_forecast_weather;
# MAGIC DROP TABLE IF EXISTS silver_gas_prices;
# MAGIC DROP TABLE IF EXISTS silver_historical_weather;
# MAGIC DROP TABLE IF EXISTS silver_loading_checkpoint;
# MAGIC DROP TABLE IF EXISTS silver_train;
# MAGIC DROP TABLE IF EXISTS silver_weather_station_to_county_mapping;
# MAGIC
# MAGIC DROP TABLE IF EXISTS bronze_client;
# MAGIC DROP TABLE IF EXISTS bronze_electricity_prices;
# MAGIC DROP TABLE IF EXISTS bronze_forecast_weather;
# MAGIC DROP TABLE IF EXISTS bronze_gas_prices;
# MAGIC DROP TABLE IF EXISTS bronze_historical_weather;
# MAGIC DROP TABLE IF EXISTS bronze_loading_checkpoint;
# MAGIC DROP TABLE IF EXISTS bronze_train;
# MAGIC DROP TABLE IF EXISTS bronze_weather_station_to_county_mapping;

# COMMAND ----------

test_schema = dbutils.widgets.get("test_schema")
spark.sql(f"use {test_schema}")
print('using database', test_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold_all_features;
# MAGIC DROP TABLE IF EXISTS gold_consumption_production_aggregate;
# MAGIC DROP TABLE IF EXISTS gold_electricity_aggregate;
# MAGIC DROP TABLE IF EXISTS gold_gas_prices_aggregate;
# MAGIC DROP TABLE IF EXISTS gold_test;
# MAGIC DROP TABLE IF EXISTS gold_energy_prices_aggregate;
# MAGIC
# MAGIC DROP TABLE IF EXISTS silver_client;
# MAGIC DROP TABLE IF EXISTS silver_electricity_prices;
# MAGIC DROP TABLE IF EXISTS silver_forecast_weather;
# MAGIC DROP TABLE IF EXISTS silver_gas_prices;
# MAGIC DROP TABLE IF EXISTS silver_historical_weather;
# MAGIC DROP TABLE IF EXISTS silver_loading_checkpoint;
# MAGIC DROP TABLE IF EXISTS silver_test;
# MAGIC
# MAGIC DROP TABLE IF EXISTS bronze_client;
# MAGIC DROP TABLE IF EXISTS bronze_electricity_prices;
# MAGIC DROP TABLE IF EXISTS bronze_forecast_weather;
# MAGIC DROP TABLE IF EXISTS bronze_gas_prices;
# MAGIC DROP TABLE IF EXISTS bronze_historical_weather;
# MAGIC DROP TABLE IF EXISTS bronze_loading_checkpoint;
# MAGIC DROP TABLE IF EXISTS bronze_test;
# MAGIC DROP TABLE IF EXISTS bronze_revealed_targets;
# MAGIC DROP TABLE IF EXISTS bronze_sample_submission;

# COMMAND ----------

# Remove checkpoint for streaming
userName = spark.sql("SELECT CURRENT_USER").collect()[0]['current_user()']
userName0 = userName.split("@")[0]
userName0 = re.sub('[!#$%&\'*+-/=?^`{}|\.]+', '_', userName0)
userName1 = userName.split("@")[1]
userName = f'{userName0}@{userName1}'
userDir = f"/Users/{userName}/data"

checkpoint_path = userDir + '/final_project/'+ train_schema +'/_checkpoint/'
print('train_checkpoint_path', checkpoint_path)
dbutils.fs.rm(checkpoint_path, True) 

checkpoint_path = userDir + '/final_project/'+ test_schema +'/_checkpoint/'
print('test_checkpoint_path', checkpoint_path)
dbutils.fs.rm(checkpoint_path, True) 

# COMMAND ----------

checkpoint_path = userDir + '/final_project/'+ train_schema +'/_checkpoint/'
print('train_checkpoint_path', checkpoint_path)
dbutils.fs.rm(checkpoint_path, True) 


# COMMAND ----------

checkpoint_path = userDir + '/final_project/'+ test_schema +'/_checkpoint/'
print('test_checkpoint_path', checkpoint_path)
dbutils.fs.rm(checkpoint_path, True) 