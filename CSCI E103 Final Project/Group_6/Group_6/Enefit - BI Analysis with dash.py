# Databricks notebook source
# MAGIC %fs ls /mnt/data/2023-kaggle-final/energy-prediction/

# COMMAND ----------

#temp to train while pipeline is being built

train_csv = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/train.csv', header=True, inferSchema=True)
train_csv.createOrReplaceTempView("train_csv")

weather_station_county_mapping = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/weather_station_to_county_mapping.csv', header=True, inferSchema=True)
weather_station_county_mapping.createOrReplaceTempView("weather_station_county_mapping")

gas_prices = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/gas_prices.csv', header=True, inferSchema=True)
gas_prices.createOrReplaceTempView("gas_prices")

electricity_prices = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/electricity_prices.csv', header=True, inferSchema=True).createOrReplaceTempView("electricity_prices")

client_temp = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/client.csv',header=True, inferSchema=True)
client_temp.createOrReplaceTempView("client_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_df
# MAGIC AS
# MAGIC Select 
# MAGIC a.county as train_county,
# MAGIC a.is_business as train_is_business,
# MAGIC a.product_type as train_product_type,
# MAGIC a.target as train_target,
# MAGIC a.is_consumption as train_is_consumption, 
# MAGIC a.datetime as train_datetime,
# MAGIC a.data_block_id as train_data_block_id,
# MAGIC a.row_id as train_row_id,
# MAGIC a.prediction_unit_id as train_prediction_unit_id,
# MAGIC
# MAGIC b.forecast_date as gas_prices_forecast_date,
# MAGIC b.lowest_price_per_mwh as gas_prices_lowest_price_per_mwh,
# MAGIC b.highest_price_per_mwh as gas_prices_highest_price_per_mwh,
# MAGIC b.origin_date as gas_prices_origin_date,
# MAGIC b.data_block_id as gas_prices_data_block_id,
# MAGIC
# MAGIC c.forecast_date as electric_prices_forecast_date,
# MAGIC c.euros_per_mwh as electric_prices_euros_per_mwh,
# MAGIC c.origin_date as electric_prices_origin_date,
# MAGIC c.data_block_id as electric_prices_data_block_id,
# MAGIC
# MAGIC d.product_type as client_product_type,
# MAGIC d.county as client_county,
# MAGIC d.eic_count as client_eic_count,
# MAGIC d.installed_capacity as client_installed_capacity,
# MAGIC d.is_business as client_is_business,
# MAGIC d.date as client_date,
# MAGIC d.data_block_id as client_data_block_id
# MAGIC
# MAGIC from train_csv as a
# MAGIC left join gas_prices as b
# MAGIC
# MAGIC   on date(a.datetime) = dateadd(b.forecast_date , 1)
# MAGIC
# MAGIC left join electricity_prices
# MAGIC   as c on date(a.datetime) = dateadd(c.forecast_date , 1)
# MAGIC
# MAGIC left join client_temp d on d.product_type = a.product_type and d.county = a.county and d.is_business = a.is_business
# MAGIC   and d.date =  dateadd(date(a.datetime), 0 )
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Read SQL table into a Spark DataFrame
table_name = "silver_df"  # Replace with your table name and schema

#NOTE Testing limit 100,000
df = spark.read.table(table_name).limit(1000000)
rowsBeforeNADrop = df.count()
df = df.na.drop()
df = df.filter(df.train_target != 0)
# df = spark.read.table(table_name)

display(df)

# COMMAND ----------

#further clean data
rowsBeforeNADrop = df.count()
df = df.na.drop()
df = df.filter(df.train_target != 0)
rowsAfterNADrop = df.count()

print(f'rows before NA drop: {rowsBeforeNADrop}\nrows after NA drop: {rowsAfterNADrop}')
print('Dropping NA values')

consumption = df.filter(df['train_is_consumption'] == 1).count()
production = df.filter(df['train_is_consumption'] == 0).count()
total = consumption + production
print("consumption: {0} ({1:.4f}%)".format(consumption, consumption / total * 100))
print("production: {0} ({1:.4f}%)".format(production, production / total * 100))

# COMMAND ----------

#making of model

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

input_cols = list(df.columns)
categoricalInputCols = ['train_county','train_is_business','train_product_type','client_product_type','client_county','client_is_business']
#removing ID rows as those values don't contribute to predictabiltiy
columnsNotUsedForTraining = {'train_is_consumption','train_data_block_id','train_row_id','train_prediction_unit_id','gas_prices_data_block_id','electric_prices_data_block_id','client_data_block_id','train_datetime','gas_prices_forecast_date','gas_prices_origin_date','electric_prices_forecast_date','electric_prices_origin_date','client_date'}

#removing train_target because that's what's being predicted
colsToRemove = columnsNotUsedForTraining.union({'train_target'})
input_cols = [x for x in input_cols if x not in colsToRemove]

#take the input cols and conform them together to a column name features
stringIndexers = [StringIndexer(inputCol=col, outputCol=col+'_index').fit(df) for col in categoricalInputCols]
stringIndexerInputs = [col+'_index' for col in categoricalInputCols]
vector_assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
stages = stringIndexers + [vector_assembler]
pipeline = Pipeline(stages=stages)

featurizedDf = pipeline.fit(df).transform(df)
display(featurizedDf)

# COMMAND ----------

train_ratio = 0.8
test_ratio = 0.2

train, test = featurizedDf.randomSplit([train_ratio, test_ratio], seed=42)

print("Train / test count: {} / {}".format(train.count(), test.count()))

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor
import mlflow
import mlflow.spark

with mlflow.start_run() as run:
    gbt = GBTRegressor(labelCol="train_target", featuresCol="features")

    # grid
    grid = (ParamGridBuilder()
                .addGrid(gbt.maxDepth, [2, 6])
                .addGrid(gbt.maxBins, [30, 32])
                .build())

    # evaluator
    ev = RegressionEvaluator(labelCol='train_target', metricName="rmse")

    # n-fold cross validation
    cv = CrossValidator(estimator=gbt, estimatorParamMaps=grid, evaluator=ev, numFolds=5)

    # cross-validated model
    cvModel = cv.fit(train)
  
    # Save best model
    tuned_model = cvModel.bestModel
    mlflow.spark.log_model(tuned_model, "gbt_model")
    
    # the parent run
    runinfo = run.info


# COMMAND ----------

loaded_model = mlflow.spark.load_model("runs:/{}/gbt_model".format(runinfo.run_id))
loaded_model

# COMMAND ----------

predictions = loaded_model.transform(test)
ev.setMetricName("mae")
mae = ev.evaluate(predictions)

ev.setMetricName("rmse")
rmse = ev.evaluate(predictions)

print("Mean Absolute Error / Mean Square Error: {} / {}".format(mae, rmse))

display(predictions("train_target", "prediction").count())

# COMMAND ----------

#push model to production

#helper functions

def print_registered_model_info(rm):
    print("name: {}".format(rm.name))
    print("tags: {}".format(rm.tags))
    print("description: {}".format(rm.description))
    print(rm)
    print('----------')

def print_model_version_info(mv):
    print("Name: {}".format(mv.name))
    print("Version: {}".format(mv.version))
    print("Tags: {}".format(mv.tags))    
    print(mv)
    print('----------')

def print_models_info(mv):
    for m in mv:
        print(m)
    print('-----------')
        #print("name: {}".format(m.name))
        #print("latest version: {}".format(m.version))
        #print("run_id: {}".format(m.run_id))
        #print("current_stage: {}".format(m.current_stage))  

import mlflow
import traceback

def delete_registered_model(client,modelname) : 
  mv = client.get_registered_model(modelname)
  mvv = mv.latest_versions
  for i in mvv:
    try:
      if i.current_stage != "Archived":
        client.transition_model_version_stage(modelname, i.version, "archived")
      client.delete_registered_model(modelname)
    except Exception as e:
      print(traceback.format_exc())
      
model_name = 'enefit_prediction_model'
client = mlflow.tracking.MlflowClient()
try:
  model_version = mlflow.register_model(model_uri=f"runs:/{runinfo.run_uuid}/gbt_model", name=model_name)
  delete_registered_model(client,model_name)
except Exception as e:
  #print('---------')
  #print(traceback.format_exc())
  pass

tags = {"data": "enefit consumption or production prediction model"}
description = 'A model to predict consumption or production target values, data sourced from kaggle'

client.create_registered_model(model_name, tags=tags, description=description)

registered_model = client.get_registered_model(model_name)

from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
runs_uri = "runs:/" + runinfo.run_uuid + "/gbt_model"
model_src = RunsArtifactRepository.get_underlying_uri(runs_uri)

print_registered_model_info(client.get_registered_model(model_name))
regmodel = client.create_model_version(model_name, model_src, run.info.run_id, description=description)
print_model_version_info(regmodel)

import time

time.sleep(2) # Just to make sure it's had a second to register

model_details = client.get_registered_model(name=model_name)
latest_versions = model_details.latest_versions
print(latest_versions)
first_version = latest_versions[0]
mv = first_version
print_model_version_info(mv)

client.transition_model_version_stage(
  name=model_name,
  version=mv.version,
  stage="Production",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_df_stefan
# MAGIC AS
# MAGIC Select 
# MAGIC a.county as train_county,
# MAGIC a.is_business as train_is_business,
# MAGIC
# MAGIC a.target as train_target,
# MAGIC a.is_consumption as train_is_consumption, 
# MAGIC a.datetime as train_datetime,
# MAGIC a.data_block_id as train_data_block_id,
# MAGIC a.row_id as train_row_id,
# MAGIC a.prediction_unit_id as train_prediction_unit_id,
# MAGIC
# MAGIC b.forecast_date as gas_prices_forecast_date,
# MAGIC b.lowest_price_per_mwh as gas_prices_lowest_price_per_mwh,
# MAGIC b.highest_price_per_mwh as gas_prices_highest_price_per_mwh,
# MAGIC b.origin_date as gas_prices_origin_date,
# MAGIC b.data_block_id as gas_prices_data_block_id,
# MAGIC
# MAGIC c.forecast_date as electric_prices_forecast_date,
# MAGIC c.euros_per_mwh as electric_prices_euros_per_mwh,
# MAGIC c.origin_date as electric_prices_origin_date,
# MAGIC c.data_block_id as electric_prices_data_block_id,
# MAGIC
# MAGIC d.product_type as client_product_type,
# MAGIC d.county as client_county,
# MAGIC d.eic_count as client_eic_count,
# MAGIC d.installed_capacity as client_installed_capacity,
# MAGIC d.is_business as client_is_business,
# MAGIC d.date as client_date,
# MAGIC d.data_block_id as client_data_block_id
# MAGIC
# MAGIC from train_csv as a
# MAGIC left join gas_prices as b
# MAGIC
# MAGIC   on date(a.datetime) = dateadd(b.forecast_date , 1)
# MAGIC
# MAGIC left join electricity_prices
# MAGIC   as c on date(a.datetime) = dateadd(c.forecast_date , 1)
# MAGIC
# MAGIC left join client_temp d on d.product_type = a.product_type and d.county = a.county and d.is_business = a.is_business
# MAGIC   and d.date =  dateadd(date(a.datetime), 0 )
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

