# Databricks notebook source
# MAGIC %md
# MAGIC ## EDA -  Data Exploration and Analysis

# COMMAND ----------

import re
dataStore = "/FileStore/group_06/"

databaseName = "group_06"

# Path + Prefix
bronzeDB = f"/{databaseName}/bronze"
silverDB = f"/{databaseName}/silver"
goldDB = f"/{databaseName}/gold"

lookupDB = f"{bronzeDB}"

print('dataStore: ' + dataStore)
print('bronzeDB: ' + bronzeDB)
print('silverDB: ' + silverDB)
print('goldDB: ' + goldDB)
print('lookupDB: ' + lookupDB)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the data and create the properties dataframe

# COMMAND ----------

properties_df = spark.sql(f"Select * from `hive_metastore`.`group_06`.`bronze_properties`")
display(properties_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Count of the input files

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from `hive_metastore`.`group_06`.`bronze_properties`

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA ANALYTICS - DataSet Column Level Exploration

# COMMAND ----------

from pyspark.sql.functions import isnull, when, count, col

property_nulldf = properties_df.select([count(when(isnull(c), c)).alias(c) for c in properties_df.columns])
display(property_nulldf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration - buildingqualitytype

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), buildingqualitytypeid from `hive_metastore`.`group_06`.`bronze_properties` group by buildingqualitytypeid

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration - regionidcity

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), regionidcity from `hive_metastore`.`group_06`.`bronze_properties` group by regionidcity

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration - heatingorsystemtype

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as count, 
# MAGIC case when a.heatingorsystemtypeid is not null then a.heatingorsystemtypeid else 0 end heatingorsystemtypeid, 
# MAGIC b.HeatingOrSystemDesc 
# MAGIC from `hive_metastore`.`group_06`.`bronze_properties` a left join `hive_metastore`.`group_06`.`bronze_heatingorsystem_type` b
# MAGIC on a.heatingorsystemtypeid = b.HeatingOrSystemTypeID
# MAGIC group by a.heatingorsystemtypeid, b.HeatingOrSystemDesc
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration - yearbuilt

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), yearbuilt from `hive_metastore`.`group_06`.`bronze_properties` group by yearbuilt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration - hashottuborspa

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as hottubspacount, 
# MAGIC case when hashottuborspa is null then 'False' else 'True' end hashottuborspa from `hive_metastore`.`group_06`.`bronze_properties` group by hashottuborspa

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA ENGINEERING II - EXPLORATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### ERD model - Property Bronze data

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="/files/karthik_chandrasekaran/Database_ER_diagram_zillow.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Data Partitions for table - Property (Exploration)

# COMMAND ----------

propCsvPath = f"/FileStore/group_06/dataset_1/input_files"
propCsvPath
propertiestDF = (spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(propCsvPath))
databaseName = "group_06"
# Path + Prefix
bronzeDB = f"/{databaseName}/bronze"
from pyspark.sql.functions import input_file_name, split, element_at, substring, to_date
propertiestDF_1 = propertiestDF.withColumn("file_name",input_file_name().cast("String"))
propertiestDF_2 = propertiestDF_1.withColumn("file_name", element_at(split(input_file_name(), "/"),-1))
propertiestDF_3 = propertiestDF_2.withColumn("file_name", element_at(split(input_file_name(), "_"),-1))
propertiestDF_4 = propertiestDF_3.withColumn("propfile_year", substring('file_name', 1, 4))
propertiestDF_4.write.format("delta").mode("overwrite").partitionBy("propfile_year").option("overwriteSchema", "true").saveAsTable(f"{databaseName}.prop_data_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS group_06.prop_data_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicates Observation - fireplacecnt

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), fireplacecnt from `hive_metastore`.`group_06`.`bronze_properties` group by fireplacecnt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicates Observation - fireplaceflag

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), 
# MAGIC case when fireplaceflag is null then 'NA' else 'True' end fireplaceflag  from `hive_metastore`.`group_06`.`bronze_properties` group by fireplaceflag

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicates Observation - parcelid

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), parcelid from `hive_metastore`.`group_06`.`silver_properties` group by parcelid having count(*) > 1 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), parcelid from `hive_metastore`.`group_06`.`silver_properties` group by parcelid having count(*) = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from `hive_metastore`.`group_06`.`silver_properties` limit 5;
# MAGIC select * from `hive_metastore`.`group_06`.`silver_properties` where parcelid = 12789910;

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA SCIENCE - Exploration

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

df = spark.sql(f"Select * from `hive_metastore`.`group_06`.`gold_properties`")
ds_df = df.toPandas()

# COMMAND ----------

## SCHEMA DETAILS
# bathroomcnt: double
# bedroomcnt: double
# parcelid: int,
# calculatedfinishedsquarefeet: double, 
# fips: int, 
# latitude: int, 
# longitude: int, 
# lotsizesquarefeet: double, 
# propertylandusetypeid: int,
# censustractandblock: bigint, 
# regionidzip: int,
# roomcnt: double, 
# taxvaluedollarcnt: double, 
# yearbuilt: double,

# ## columns to be dropped for ds model
# BuildingQualityTypeDesc: string,
# decktypeid: int,
# unitcnt: int, 
# fireplaceflag: boolean, 
# fullbathcnt: int, 
# hashottuborspa: boolean, 
# poolcnt: int, 
# pooltypeid2: int, 
# pooltypeid10: int, 
# propertycountylandusecode: string, 
# PropertyLandUseDesc: string, 
# yardbuildingsqft17: int, 
# yardbuildingsqft26: int, 
# basementsqft: int, 
# state_code: string, 
# state_fips: string, 
# landtaxvaluedollarcnt: double]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Engineering - Drop columns

# COMMAND ----------

ds_df.drop(columns=['BuildingQualityTypeDesc', 'decktypeid', 'fireplaceflag', 'fullbathcnt', 'hashottuborspa','pooltypeid2' ,'pooltypeid10', 'propertycountylandusecode' ,'PropertyLandUseDesc' ,'yardbuildingsqft17', 'yardbuildingsqft26', 'basementsqft', 'state_code', 'state_fips', 'landtaxvaluedollarcnt', 'poolcnt', 'unitcnt'], axis=1, inplace=True)

# COMMAND ----------

display(ds_df)

# COMMAND ----------

def replace_missing_data(df, mis_vars):
    print('##### Replacing missing values with mode of features #####')
    for var in mis_vars:
        df[var] = df[var].fillna(df[var].mode()[0])
    return df

mis_var = [var for var in ds_df.columns if ds_df[var].isnull().sum() > 0]
ds_df = replace_missing_data(ds_df, mis_var)
ds_df.head()
ds_df.isnull().mean()*100

# COMMAND ----------

ds_df.shape
ds_df.drop_duplicates(subset ="parcelid", keep = 'first', inplace = True)
X = ds_df.drop('taxvaluedollarcnt', axis=1)
y = ds_df['taxvaluedollarcnt']

new_df = ds_df.copy()

print(X.shape, y.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ML Training - Train Test Split

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state = 23)

X_train.shape, y_train.shape, X_test.shape, y_test.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scaling - columns

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

# X_train.drop(columns='parcelid', axis=1, inplace=True)
# X_test.drop(columns='parcelid', axis=1, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Training - Linear Regression

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

# MAGIC %md
# MAGIC ### Prediction - Model Results

# COMMAND ----------

model_pred = pd.DataFrame({'parcelid':X_test_new.parcelid, 'predtaxvaluedollarcnt':linear_reg_pred, 'acttaxvaluedollarcnt':y_test})
model_pred.to_csv('model_predictions.csv',index=False)
model_pred.head()