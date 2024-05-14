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

spark.sql(f"use {databaseName}")

print('UserDir ' + userDir)
print('userName '+ userName)
print('Using database '+ databaseName)

# COMMAND ----------

if dataset == 'train':
  flattened_sql = """
  select 
  a.county as {dataset}_county,
  a.is_business as {dataset}_is_business,
  a.product_type as {dataset}_product_type,
  a.target as {dataset}_target,
  a.is_consumption as {dataset}_is_consumption, 
  a.datetime as {dataset}_datetime,
  a.data_block_id as {dataset}_data_block_id,
  a.row_id as {dataset}_row_id,
  a.prediction_unit_id as {dataset}_prediction_unit_id,

  b.forecast_date as gas_prices_forecast_date,
  b.lowest_price_per_mwh as gas_prices_lowest_price_per_mwh,
  b.highest_price_per_mwh as gas_prices_highest_price_per_mwh,
  b.origin_date as gas_prices_origin_date,
  b.data_block_id as gas_prices_data_block_id,

  c.forecast_date as electric_prices_forecast_date,
  c.euros_per_mwh as electric_prices_euros_per_mwh,
  c.origin_date as electric_prices_origin_date,
  c.data_block_id as electric_prices_data_block_id,

  d.product_type as client_product_type,
  d.county as client_county,
  d.eic_count as client_eic_count,
  d.installed_capacity as client_installed_capacity,
  d.is_business as client_is_business,
  d.date as client_date,
  d.data_block_id as client_data_block_id

  from silver_{dataset} as a
  left join silver_gas_prices as b

    on date(a.datetime) = dateadd(b.forecast_date , 1)

  left join silver_electricity_prices
    as c on date(a.datetime) = dateadd(c.forecast_date , 1)

  left join silver_client d on d.product_type = a.product_type and d.county = a.county and d.is_business = a.is_business
    and d.date =  dateadd(date(a.datetime), 0 )
  """.format(dataset=dataset)

  gold_all_features_df = spark.sql(flattened_sql)
  gold_all_features_df.write.mode('overwrite').saveAsTable('gold_all_features')

# COMMAND ----------

if dataset == "train":
  spark.sql("SELECT * FROM gold_all_features LIMIT 10")

# COMMAND ----------

if dataset == "train":
  spark.sql("SELECT COUNT(1) FROM gold_all_features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating/Merging/Upserting gold_electricity_aggregate

# COMMAND ----------

def create_merge_upsert(logical_table_name, select_sql, merge_upsert_sql):
  data_df = spark.sql(select_sql)
  gold_table = 'gold_' + logical_table_name

  if not spark.catalog.tableExists(gold_table):
    print('gold_electricity_aggregate does not exist, creating')
    data_df.write.mode('overwrite').saveAsTable(gold_table)
  else:
    print('merge/upsert into', gold_table)
    temp_table = 'temp_' + logical_table_name
    data_df.createOrReplaceTempView(temp_table)
    spark.sql(merge_upsert_sql)

    spark.catalog.dropTempView(temp_table)

# COMMAND ----------

electricity_sql = """
SELECT 
  forecast_date,
  MEDIAN(euros_per_mwh) AS euros_per_mwh_median,
  AVG(euros_per_mwh) AS euros_per_mwh_average
FROM silver_electricity_prices
GROUP BY forecast_date
ORDER BY forecast_date
"""

electricity_merge_upsert_sql = """
  MERGE INTO gold_electricity_aggregate
  USING temp_electricity_aggregate
  ON gold_electricity_aggregate.forecast_date = temp_electricity_aggregate.forecast_date
  WHEN MATCHED THEN
    UPDATE SET
      euros_per_mwh_median = temp_electricity_aggregate.euros_per_mwh_median,
      euros_per_mwh_average = temp_electricity_aggregate.euros_per_mwh_average
  WHEN NOT MATCHED
    THEN INSERT (
      forecast_date,
      euros_per_mwh_median,
      euros_per_mwh_average
    )
    VALUES (
      temp_electricity_aggregate.forecast_date,
      temp_electricity_aggregate.euros_per_mwh_median,
      temp_electricity_aggregate.euros_per_mwh_average
    )
  WHEN NOT MATCHED BY SOURCE THEN
    DELETE    
  """

create_merge_upsert('electricity_aggregate', electricity_sql, electricity_merge_upsert_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_electricity_aggregate ORDER BY forecast_date DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM gold_electricity_aggregate

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating/Merging/Upserting gold_gas_prices_aggregate

# COMMAND ----------

gas_prices_sql = """
SELECT 
  forecast_date,
  MEDIAN(lowest_price_per_mwh) AS lowest_price_per_mwh_median,
  AVG(lowest_price_per_mwh) AS lowest_price_per_mwh_average,
  MEDIAN(highest_price_per_mwh) AS highest_price_per_mwh_median,
  AVG(highest_price_per_mwh) AS highest_price_per_mwh_average
FROM silver_gas_prices
GROUP BY forecast_date
ORDER BY forecast_date
"""

gas_prices_merge_upsert_sql = """
  MERGE INTO gold_gas_prices_aggregate
  USING temp_gas_prices_aggregate
  ON gold_gas_prices_aggregate.forecast_date = temp_gas_prices_aggregate.forecast_date
  WHEN MATCHED THEN
    UPDATE SET
      lowest_price_per_mwh_median = temp_gas_prices_aggregate.lowest_price_per_mwh_median,
      lowest_price_per_mwh_average = temp_gas_prices_aggregate.lowest_price_per_mwh_average,
      highest_price_per_mwh_median = temp_gas_prices_aggregate.highest_price_per_mwh_median,
      highest_price_per_mwh_average = temp_gas_prices_aggregate.highest_price_per_mwh_average
  WHEN NOT MATCHED
    THEN INSERT (
      forecast_date,
      lowest_price_per_mwh_median,
      lowest_price_per_mwh_average,
      highest_price_per_mwh_median,
      highest_price_per_mwh_average
    )
    VALUES (
      temp_gas_prices_aggregate.forecast_date,
      temp_gas_prices_aggregate.lowest_price_per_mwh_median,
      temp_gas_prices_aggregate.lowest_price_per_mwh_average,
      temp_gas_prices_aggregate.highest_price_per_mwh_median,
      temp_gas_prices_aggregate.highest_price_per_mwh_average
    )
  WHEN NOT MATCHED BY SOURCE THEN
    DELETE 
"""

create_merge_upsert('gas_prices_aggregate', gas_prices_sql, gas_prices_merge_upsert_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_gas_prices_aggregate ORDER BY forecast_date DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM gold_gas_prices_aggregate

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating/Merging/Upserting gold_consumption_production_aggregate

# COMMAND ----------

if dataset == 'train':
  consumption_production_sql = """
  WITH total_consumption AS (
    SELECT 
      TO_DATE(datetime) AS date,
      sum(target) AS total_consumption_amt
    FROM silver_train
    WHERE 
      is_consumption = 1
    GROUP BY date    
  ),
  total_production AS (
    SELECT 
        TO_DATE(datetime) AS date,
        sum(target) AS total_production_amt
      FROM silver_train
      WHERE 
        is_consumption = 0
      GROUP BY date
  )
  SELECT 
    coalesce(c.date, p.date) AS date,
    total_consumption_amt,
    total_production_amt
  FROM 
    total_consumption c JOIN total_production p ON c.date = p.date
  ORDER BY date ASC
  """

  consumption_production_merge_upsert_sql = """
    MERGE INTO gold_consumption_production_aggregate
    USING temp_consumption_production_aggregate
    ON gold_consumption_production_aggregate.date = temp_consumption_production_aggregate.date
    WHEN MATCHED THEN
      UPDATE SET
        total_consumption_amt = temp_consumption_production_aggregate.total_consumption_amt,
        total_production_amt = temp_consumption_production_aggregate.total_production_amt
    WHEN NOT MATCHED
      THEN INSERT (
        date,
        total_consumption_amt,
        total_production_amt
      )
      VALUES (
        temp_consumption_production_aggregate.date,
        temp_consumption_production_aggregate.total_consumption_amt,
        temp_consumption_production_aggregate.total_production_amt
      )
    WHEN NOT MATCHED BY SOURCE THEN
      DELETE 
  """

  create_merge_upsert('consumption_production_aggregate', consumption_production_sql, consumption_production_merge_upsert_sql)

# COMMAND ----------

if dataset == 'train':
  spark.sql("SELECT * FROM gold_consumption_production_aggregate ORDER BY date DESC LIMIT 10")

# COMMAND ----------

if dataset == 'train':
  spark.sql("SELECT COUNT(1) FROM gold_consumption_production_aggregate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream data to gold train/test dataset

# COMMAND ----------

dataset_table = "gold_" + dataset

checkpoint_path = userDir + '/final_project/' + databaseName + '/_checkpoint/' + dataset_table

print('checkpoint path', checkpoint_path)

input_stream_df = spark.readStream \
  .table('silver_' + dataset) \
  .writeStream \
  .option("checkpointLocation", checkpoint_path) \
  .outputMode("append") \
  .trigger(once=True) \
  .toTable(dataset_table)

# COMMAND ----------

display(spark.sql('SELECT * FROM gold_' + dataset + ' LIMIT 10'))

# COMMAND ----------

display(spark.sql('SELECT COUNT(1) FROM gold_' + dataset))

# COMMAND ----------

