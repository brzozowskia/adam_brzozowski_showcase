# Databricks notebook source
df_gold = spark.sql(f"Select * from `hive_metastore`.`group_06`.`gold_properties`")
df_gold.createOrReplaceTempView("gl_matt")
display(df_gold)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT fireplaceflag FROM gl_matt;

# COMMAND ----------

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
import xgboost

from math import sqrt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, ElasticNet, Ridge, Lasso, SGDRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.ensemble import RandomForestRegressor, AdaBoostRegressor, GradientBoostingRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.preprocessing import PolynomialFeatures
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import cross_val_score

%matplotlib inline
pd.set_option('display.max_columns',None)
warnings.simplefilter(action='ignore')

# COMMAND ----------

ds_df = df_gold.toPandas()

# COMMAND ----------

ds_df.drop(columns=['BuildingQualityTypeDesc', 'decktypeid', 'fullbathcnt', 'hashottuborspa','pooltypeid2' ,'pooltypeid10', 'propertycountylandusecode', 'yardbuildingsqft26', 'basementsqft', 'state_code', 'state_fips', 'landtaxvaluedollarcnt', 'poolcnt', 'unitcnt', 'propertylandusetypeid', 'censustractandblock', 'regionidzip'], axis=1, inplace=True)

ds_df.drop_duplicates(subset ="parcelid", keep = 'first', inplace = True)

# COMMAND ----------

display(ds_df)

# COMMAND ----------

# What needs to happen... nulls
# fireplaceflag -> null to 0, true to 1
ds_df['fireplaceflag'].fillna(False, inplace=True)
ds_df['fireplaceflag'] = ds_df['fireplaceflag'].astype(int)
# calculatedfinishedsquarefeet -> null to 0
ds_df['calculatedfinishedsquarefeet'].fillna(0, inplace=True)
#lotsizesquarefeet -> null to 0
ds_df['lotsizesquarefeet'].fillna(0, inplace=True)
# yardbuildingsqft17 -> null to 0
ds_df['yardbuildingsqft17'].fillna(0, inplace=True)
# yearbuilt
ds_df['yearbuilt'].fillna(0, inplace=True)

# Categoricals...
def years(row):
  if row['yearbuilt'] == 0:
    return "No Structure"
  elif row['yearbuilt'] < 1960:
    return "Over 60 Years"
  elif row['yearbuilt'] < 2010:
    return "10 to 50 years old"
  else:
    return "Newer than 10 years"

ds_df['agegroup'] = ds_df.apply(lambda row: years(row), axis=1)
ds_df.drop('agegroup', axis = 1)

ds_df = pd.get_dummies(ds_df, prefix=['agegroup', 'PropertyLandUseDesc', 'fips'], columns=['agegroup', 'PropertyLandUseDesc', 'fips'])

ds_df.head()
ds_df.isnull().mean()*100

# COMMAND ----------

for col in ds_df.columns:
  print(col)

# COMMAND ----------

ds_df.shape
X = ds_df.drop('taxvaluedollarcnt', axis=1)
y = ds_df['taxvaluedollarcnt']

new_df = ds_df.copy()

print(X.shape, y.shape)

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state = 123)

X_train.shape, y_train.shape, X_test.shape, y_test.shape

# COMMAND ----------

scaler = StandardScaler()
train_vars = [var for var in X_train.columns if var not in ['parcelid']]
len(train_vars)

scaler.fit(X_train[train_vars]) 

X_train[train_vars] = scaler.transform(X_train[train_vars])
X_test[train_vars] = scaler.transform(X_test[train_vars])

# COMMAND ----------

X_train_new = X_train.copy()
X_test_new = X_test.copy()

X_train.drop(columns='parcelid', axis=1, inplace=True)
X_test.drop(columns='parcelid', axis=1, inplace=True)

# COMMAND ----------

linear_reg = LinearRegression()
linear_reg.fit(X_train, y_train)

# COMMAND ----------

linear_reg_pred = linear_reg.predict(X_test)
print('Mean Absolute Error : {}'.format(mean_absolute_error(y_test, linear_reg_pred)))
print()
print('Mean Squared Error : {}'.format(mean_squared_error(y_test, linear_reg_pred)))
print()
print('Root Mean Squared Error : {}'.format(sqrt(mean_squared_error(y_test, linear_reg_pred))))

# COMMAND ----------

model_pred = pd.DataFrame({'parcelid':X_test_new.parcelid, 'predtaxvaluedollarcnt':linear_reg_pred, 'acttaxvaluedollarcnt':y_test})
model_pred.to_csv('model_predictions.csv',index=False)
model_pred.head()

# COMMAND ----------

model_pred['difference'] = model_pred['predtaxvaluedollarcnt'] - model_pred['acttaxvaluedollarcnt']

# COMMAND ----------

model_pred.display()

# COMMAND ----------

dataStore = "/FileStore/group_06/"
databaseName = "group_06"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")
spark.sql('DROP TABLE IF EXISTS model_predictions')
model_pred.write.format("delta").mode("overwrite").saveAsTable('model_predictions')