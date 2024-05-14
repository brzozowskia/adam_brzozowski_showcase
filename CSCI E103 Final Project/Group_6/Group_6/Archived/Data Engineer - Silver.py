# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Group 6
# MAGIC #### Final Project - Silver Layer

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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Database initialization

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Load bronze data

# COMMAND ----------

# Read the data from the bronze_properties Delta table as a DataFrame

propertiesDF = spark.read.format("delta").table("bronze_properties")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Load lookup tables

# COMMAND ----------

acDF = spark.read.format("delta").table("bronze_airconditioning_type")

# COMMAND ----------

asDF = spark.read.format("delta").table("bronze_architecturalstyle_type")

# COMMAND ----------

bcDF = spark.read.format("delta").table("bronze_buildingclass_type")

# COMMAND ----------

hsDF = spark.read.format("delta").table("bronze_heatingorsystem_type")

# COMMAND ----------

pluDF = spark.read.format("delta").table("bronze_propertylanduse_type")

# COMMAND ----------

storyDF = spark.read.format("delta").table("bronze_story_type")

# COMMAND ----------

tcDF = spark.read.format("delta").table("bronze_typeconstruction_type")

# COMMAND ----------

bqDF = spark.read.format("delta").table("bronze_buildingquality_type")

# COMMAND ----------

stateDF = spark.read.format("delta").table("bronze_state_fips")

# COMMAND ----------

ac_1_DF = spark.read.format("delta").table("bronze_properties")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Silver Database
# MAGIC
# MAGIC Silver layer stored in Delta format.
# MAGIC
# MAGIC **Transformations:**
# MAGIC - Descriptions for ID columns added
# MAGIC - FIPS converted to string
# MAGIC - Leading zeroes added to FIPS
# MAGIC - State FIPS added
# MAGIC - State abreviations added
# MAGIC - State names added

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Air conditioning description

# COMMAND ----------

from pyspark.sql.functions import col

# Rename ID column to avoid ambiguities in join
ac_1_DF = acDF.withColumnRenamed('AirConditioningTypeID', 'joinID')

# Add air conditioning type description
properties_1_DF = propertiesDF.join(ac_1_DF, propertiesDF.airconditioningtypeid == ac_1_DF.joinID, how = 'left')

# remove extra column add by join
properties_1_DF = properties_1_DF.drop(col('joinID'))

# Display dataframe
display(properties_1_DF)
# display(properties_1_DF.filter(properties_1_DF.airconditioningtypeid.isNotNull()))

# Display number of rows
print(' ')
print('Total of records: ' + str(properties_1_DF.count()))

# Print schema
print(' ')
print('Schema:')
properties_1_DF.printSchema()  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Architectural style description

# COMMAND ----------

# Rename ID column to avoid ambiguities in join
as_1_DF = asDF.withColumnRenamed('ArchitecturalStyleTypeID', 'joinID')

# Add architectural style type description
properties_1_DF = properties_1_DF.join(as_1_DF, properties_1_DF.architecturalstyletypeid == as_1_DF.joinID, how = 'left')

# remove extra column add by join
properties_1_DF = properties_1_DF.drop(col('joinID'))

# Display dataframe
display(properties_1_DF)
# display(properties_1_DF.filter(properties_1_DF.airconditioningtypeid.isNotNull()))

# Display number of rows
print(' ')
print('Total of records: ' + str(properties_1_DF.count()))

# Print schema
print(' ')
print('Schema:')
properties_1_DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Building class description

# COMMAND ----------

# Rename ID column to avoid ambiguities in join
bc_1_DF = bcDF.withColumnRenamed('BuildingClassTypeID', 'joinID')

# Add building class type description
properties_1_DF = properties_1_DF.join(bc_1_DF, properties_1_DF.buildingclasstypeid == bc_1_DF.joinID, how = 'left')

# remove extra column add by join
properties_1_DF = properties_1_DF.drop(col('joinID'))

# Display dataframe
display(properties_1_DF)
# display(properties_1_DF.filter(properties_1_DF.airconditioningtypeid.isNotNull()))

# Display number of rows
print(' ')
print('Total of records: ' + str(properties_1_DF.count()))

# Print schema
print(' ')
print('Schema:')
properties_1_DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Heating or system description

# COMMAND ----------

# Rename ID column to avoid ambiguities in join
hs_1_DF = hsDF.withColumnRenamed('HeatingOrSystemTypeID', 'joinID')

# Add heating or system type description
properties_1_DF = properties_1_DF.join(hs_1_DF, properties_1_DF.heatingorsystemtypeid == hs_1_DF.joinID, how = 'left')

# remove extra column add by join
properties_1_DF = properties_1_DF.drop(col('joinID'))

# Display dataframe
display(properties_1_DF)
# display(properties_1_DF.filter(properties_1_DF.airconditioningtypeid.isNotNull()))

# Display number of rows
print(' ')
print('Total of records: ' + str(properties_1_DF.count()))

# Print schema
print(' ')
print('Schema:')
properties_1_DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Property land use description

# COMMAND ----------

# Rename ID column to avoid ambiguities in join
plu_1_DF = pluDF.withColumnRenamed('PropertyLandUseTypeID', 'joinID')

# Add property land use type description
properties_1_DF = properties_1_DF.join(plu_1_DF, properties_1_DF.propertylandusetypeid == plu_1_DF.joinID, how = 'left')

# remove extra column add by join
properties_1_DF = properties_1_DF.drop(col('joinID'))

# Display dataframe
display(properties_1_DF)
# display(properties_1_DF.filter(properties_1_DF.airconditioningtypeid.isNotNull()))

# Display number of rows
print(' ')
print('Total of records: ' + str(properties_1_DF.count()))

# Print schema
print(' ')
print('Schema:')
properties_1_DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Story description

# COMMAND ----------

# Rename ID column to avoid ambiguities in join
story_1_DF = storyDF.withColumnRenamed('StoryTypeID', 'joinID')

# Add story type description
properties_1_DF = properties_1_DF.join(story_1_DF, properties_1_DF.storytypeid == story_1_DF.joinID, how = 'left')

# remove extra column add by join
properties_1_DF = properties_1_DF.drop(col('joinID'))

# Display dataframe
display(properties_1_DF)
# display(properties_1_DF.filter(properties_1_DF.airconditioningtypeid.isNotNull()))

# Display number of rows
print(' ')
print('Total of records: ' + str(properties_1_DF.count()))

# Print schema
print(' ')
print('Schema:')
properties_1_DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Type construction description

# COMMAND ----------

# Rename ID column to avoid ambiguities in join
tc_1_DF = tcDF.withColumnRenamed('TypeConstructionTypeID', 'joinID')

# Add type construction type description
properties_1_DF = properties_1_DF.join(tc_1_DF, properties_1_DF.typeconstructiontypeid == tc_1_DF.joinID, how = 'left')

# remove extra column add by join
properties_1_DF = properties_1_DF.drop(col('joinID'))

# Display dataframe
display(properties_1_DF)

# Display number of rows
print(' ')
print('Total of records: ' + str(properties_1_DF.count()))

# Print schema
print(' ')
print('Schema:')
properties_1_DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Building quality description

# COMMAND ----------

# Rename ID column to avoid ambiguities in join
bq_1_DF = bqDF.withColumnRenamed('BuildingQualityTypeID', 'joinID')

# Add building quality type description
properties_1_DF = properties_1_DF.join(bq_1_DF, properties_1_DF.buildingqualitytypeid == bq_1_DF.joinID, how = 'left')

# remove extra column add by join
properties_1_DF = properties_1_DF.drop(col('joinID'))

# Display dataframe
display(properties_1_DF)

# Display number of rows
print(' ')
print('Total of records: ' + str(properties_1_DF.count()))

# Print schema
print(' ')
print('Schema:')
properties_1_DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### FIPS column in string format

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import lpad

# Add FIPS column in string format with leading zero
properties_2_DF = properties_1_DF.withColumn('fips_str', lpad(properties_1_DF.fips.cast(StringType()), 5, '0'))

# Display dataframe
display(properties_2_DF)

# Print schema
print(' ')
print('Schema:')
properties_2_DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### State information

# COMMAND ----------

from pyspark.sql.functions import substring

# Add state name, state 2-character code and state FIPS columns
properties_2_DF = properties_2_DF.join(stateDF, substring(properties_2_DF.fips_str, 0, 2) == stateDF.state_fips, how = 'left')

# Display dataframe
display(properties_2_DF)

# Display number of rows
print(' ')
print('Total of records: ' + str(properties_2_DF.count()))

# Print schema
print(' ')
print('Schema:')
properties_2_DF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Store silver data with partition

# COMMAND ----------

# Store properties silver table

spark.sql('DROP TABLE IF EXISTS silver_properties')

(properties_2_DF
   .write
   .format("delta")
   .mode("overwrite")
   .partitionBy("regionidneighborhood")
   .saveAsTable('silver_properties')
)