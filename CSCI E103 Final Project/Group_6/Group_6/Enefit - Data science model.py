# Databricks notebook source
databaseNameTrain = 'fa2023_group06_enefit_train'
#for training
#for repeatability fix the silver_train data block ids to a specific block
spark.sql(f"use {databaseNameTrain}")
silver_df = spark.sql("""SELECT * FROM silver_train WHERE data_block_id IN (
    SELECT data_block_id
    FROM (
        SELECT data_block_id, ROW_NUMBER() OVER (ORDER BY data_block_id DESC) AS rn
        FROM silver_train
    ) ranked
    WHERE rn <= 5
);""")
df = silver_df

# COMMAND ----------

#further clean data
rowsBeforeNADrop = df.count()
df = df.na.drop()
#throgh experimentation, it appears removing target=0 provides better results in predictions (lower MAE, MSE)
df = df.filter(df.target != 0) 
rowsAfterNADrop = df.count()

print(f'rows before NA drop: {rowsBeforeNADrop}\nrows after NA drop: {rowsAfterNADrop}')
print('Dropping NA values')

consumption = df.filter(df['is_consumption'] == 1).count()
production = df.filter(df['is_consumption'] == 0).count()
total = consumption + production
print("consumption: {0} ({1:.4f}%)".format(consumption, consumption / total * 100))
print("production: {0} ({1:.4f}%)".format(production, production / total * 100))

# COMMAND ----------

#making of model that predicts target. A single model, can be used for consumption/production data

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

#dropping datetime column becasue not supported for mlflow model creation
dfDroppedDateTime = df.drop('datetime')

input_cols = list(dfDroppedDateTime.columns)
categoricalInputCols = ['county','is_business','product_type','is_consumption']
#removing ID rows as those values don't contribute to predictabiltiy
columnsNotUsedForTraining = {'row_id','data_block_id','prediction_unit_id', 'datetime'}

#removing target because that's what's being predicted
colsToRemove = columnsNotUsedForTraining.union({'target'})
input_cols = [x for x in input_cols if x not in colsToRemove or x not in categoricalInputCols]

#take the input cols and conform them together to a column name features
stringIndexers = [StringIndexer(inputCol=col, outputCol=col+'_index').fit(df) for col in categoricalInputCols]
stringIndexerInputs = [col+'_index' for col in categoricalInputCols]
vector_assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
stages = stringIndexers + [vector_assembler]
pipeline = Pipeline(stages=stages)

featurizedDf = pipeline.fit(dfDroppedDateTime).transform(dfDroppedDateTime)
display(featurizedDf)

# COMMAND ----------

#train/test split
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
    gbt = GBTRegressor(labelCol="target", featuresCol="features")

    grid = (ParamGridBuilder()
                .addGrid(gbt.maxDepth, [2, 6])
                .addGrid(gbt.maxBins, [30, 32])
                .build())

    ev = RegressionEvaluator(labelCol='target', metricName="rmse")

    cv = CrossValidator(estimator=gbt, estimatorParamMaps=grid, evaluator=ev, numFolds=5)

    cvModel = cv.fit(train)
  
    tuned_model = cvModel.bestModel
    mlflow.spark.log_model(tuned_model, "gbt_model")
    
    runinfo = run.info


# COMMAND ----------

loaded_model = mlflow.spark.load_model("runs:/{}/gbt_model".format(runinfo.run_id))
loaded_model

# COMMAND ----------

#predictions on the training test segment
predictions = loaded_model.transform(test)
ev.setMetricName("mae")
mae = ev.evaluate(predictions)
ev.setMetricName("rmse")
rmse = ev.evaluate(predictions)
predictionMaxVal = predictions.agg({"prediction": "max"}).collect()[0][0]
predictionMinVal = predictions.agg({"prediction": "min"}).collect()[0][0]
percentRMSE = rmse/(predictionMaxVal-predictionMinVal) * 100
print("Training Test Percent Root Mean Square Error: {} %".format(percentRMSE))
display(predictions)

table_name = "EnefitModelPredictionsOnTrain"
if spark.catalog.tableExists(table_name):
    spark.sql(f"DROP TABLE {table_name}")
predictions.write.mode("overwrite").saveAsTable(table_name)

#predictions on true test
databaseNameTest = 'fa2023_group06_enefit_test'
spark.sql(f"use {databaseNameTest}")
silverTest_df = spark.sql("""SELECT *, 0 as target FROM silver_test WHERE data_block_id IN (
    SELECT data_block_id
    FROM (
        SELECT data_block_id, ROW_NUMBER() OVER (ORDER BY data_block_id DESC) AS rn
        FROM silver_test
    ) ranked
    WHERE rn <= 5
);""")
silverTestDf = silverTest_df
silverTestDfDroppedDateTime = silverTestDf.drop('datetime')
silverTestDfDroppedDateTimeFeaturized = pipeline.fit(silverTestDfDroppedDateTime).transform(silverTestDfDroppedDateTime)
predictionsTest = loaded_model.transform(silverTestDfDroppedDateTimeFeaturized)
display(predictionsTest)
table_name = "EnefitModelPredictionsOnTest"
if spark.catalog.tableExists(table_name):
    spark.sql(f"DROP TABLE {table_name}")
predictionsTest.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

#generate model prediction visualization graphs

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#Choose a random 100 rows of the dataframe to observe predictions vs target
np.random.seed(1)
consumptionDF = predictions[predictions['is_consumption'] == 1]
productionDF = predictions[predictions['is_consumption'] == 0]

#consumption predictions
df = consumptionDF.toPandas().sample(n=100)
plt.figure(figsize=(8, 6))
plt.scatter(df['row_id'], df['target'], label='Target', marker='o')
plt.scatter(df['row_id'], df['prediction'], label='Prediction', marker='x')
plt.xlabel('Row ID')
plt.ylabel('Values')
plt.title('Consumption Target vs Prediction')
plt.legend()
plt.grid(True)
plt.show()

#production predictions
df = productionDF.toPandas().sample(n=100)
plt.figure(figsize=(8, 6))
plt.scatter(df['row_id'], df['target'], label='Target', marker='o')
plt.scatter(df['row_id'], df['prediction'], label='Prediction', marker='x')
plt.xlabel('Row ID')
plt.ylabel('Values')
plt.title('Production Target vs Prediction')
plt.legend()
plt.grid(True)
plt.show()


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