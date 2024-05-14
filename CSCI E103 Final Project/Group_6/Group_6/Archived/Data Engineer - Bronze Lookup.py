# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Group 6
# MAGIC #### Final Project - Bronze - Lookup

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
# MAGIC ## Read Raw Data 
# MAGIC Source data format: .csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataset 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Property data

# COMMAND ----------

from pyspark.sql.functions import input_file_name, split, element_at, substring, to_date

# Dataset 1: Properties
files = [f"{dataStore}dataset_1/properties_2016.csv", f"{dataStore}dataset_1/properties_2017.csv"]

# Read CSV files
propertiesDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Add filename column - To be used in partition
propertiesDF = propertiesDF.withColumn("file_name",input_file_name().cast("String"))
propertiesDF = propertiesDF.withColumn("file_name", element_at(split(input_file_name(), "/"),-1))
propertiesDF = propertiesDF.withColumn("file_name", element_at(split(input_file_name(), "_"),-1))

# Add filename column - To be used in partition
propertiesDF = propertiesDF.withColumn("file_year", substring('file_name', 1, 4))

# Display dataframe
display(propertiesDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(propertiesDF.count()))

# Print schema
print(' ')
print('Schema:')
propertiesDF.printSchema()  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Lookup data

# COMMAND ----------

# Dataset 1: Air conditioning lookup table
files = [f"{dataStore}dataset_1/LU_AirConditioningType.csv"]

# Read CSV files
acDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(acDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(acDF.count()))

# Print schema
print(' ')
print('Schema:')
acDF.printSchema()  

# COMMAND ----------

# Dataset 1: Architectural style lookup table
files = [f"{dataStore}dataset_1/LU_ArchitecturalStyleType.csv"]

# Read CSV files
asDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(asDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(asDF.count()))

# Print schema
print(' ')
print('Schema:')
asDF.printSchema()   

# COMMAND ----------

# Dataset 1: Building class lookup table
files = [f"{dataStore}dataset_1/LU_BuildingClassType.csv"]

# Read CSV files
bcDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(bcDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(bcDF.count()))

# Print schema
print(' ')
print('Schema:')
bcDF.printSchema()  

# COMMAND ----------

# Dataset 1: Heating or system lookup table
files = [f"{dataStore}dataset_1/LU_HeatingOrSystemType.csv"]

# Read CSV files
hsDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(hsDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(hsDF.count()))

# Print schema
print(' ')
print('Schema:')
hsDF.printSchema()

# COMMAND ----------

# Dataset 1: Property land use lookup table
files = [f"{dataStore}dataset_1/LU_PropertyLandUseType.csv"]

# Read CSV files
pluDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(pluDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(pluDF.count()))

# Print schema
print(' ')
print('Schema:')
pluDF.printSchema()

# COMMAND ----------

# Dataset 1: Story lookup table
files = [f"{dataStore}dataset_1/LU_StoryType.csv"]

# Read CSV files
storyDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(storyDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(storyDF.count()))

# Print schema
print(' ')
print('Schema:')
storyDF.printSchema()

# COMMAND ----------

# Dataset 1: Type construction lookup table
files = [f"{dataStore}dataset_1/LU_TypeConstructionType.csv"]

# Read CSV files
tcDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(tcDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(tcDF.count()))

# Print schema
print(' ')
print('Schema:')
tcDF.printSchema()

# COMMAND ----------

# Dataset 1: Building quality lookup table
files = [f"{dataStore}dataset_1/LU_BuildingQualityType.csv"]

# Read CSV files
bqDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(bqDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(bqDF.count()))

# Print schema
print(' ')
print('Schema:')
bqDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### State FIPS

# COMMAND ----------

# State FIPS lookup table 
from pyspark.sql.functions import lpad
from pyspark.sql.types import StringType

files = [f"{dataStore}dataset_1/fips_state.csv"]

# Read CSV files
stateDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

stateDF = stateDF.withColumn('state_fips', lpad(stateDF.state_fips.cast(StringType()), 2, '0'))

# Display dataframe
display(stateDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(stateDF.count()))

# Print schema
print(' ')
print('Schema:')
stateDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze Database
# MAGIC
# MAGIC Bronze layer stored in Delta format.
# MAGIC
# MAGIC **Dataset 1**
# MAGIC - Lookup data added

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dataset 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Lookup tables

# COMMAND ----------

# Store air conditioning lookup table
spark.sql('DROP TABLE IF EXISTS bronze_airconditioning_type')

(acDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_airconditioning_type')
)

# COMMAND ----------

# Store architectural style lookup table
spark.sql('DROP TABLE IF EXISTS bronze_architecturalstyle_type')

(asDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('bronze_architecturalstyle_type')
)

# COMMAND ----------

# Store building class lookup table
spark.sql('DROP TABLE IF EXISTS bronze_buildingclass_type')

(bcDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('bronze_buildingclass_type')
)

# COMMAND ----------

# Store heating or system lookup table
spark.sql('DROP TABLE IF EXISTS bronze_heatingorsystem_type')

(hsDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('bronze_heatingorsystem_type')
)

# COMMAND ----------

# Store property land use lookup table
spark.sql('DROP TABLE IF EXISTS bronze_propertylanduse_type')

(pluDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('bronze_propertylanduse_type')
)

# COMMAND ----------

# Store story lookup table
spark.sql('DROP TABLE IF EXISTS bronze_story_type')

(storyDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('bronze_story_type')
)

# COMMAND ----------

# Store type construction lookup table
spark.sql('DROP TABLE IF EXISTS bronze_typeconstruction_type')

(tcDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('bronze_typeconstruction_type')
)

# COMMAND ----------

# Store building quality lookup table
spark.sql('DROP TABLE IF EXISTS bronze_buildingquality_type')

(bqDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('bronze_buildingquality_type')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### State FIPS

# COMMAND ----------

# Store state FIPS lookup table
spark.sql('DROP TABLE IF EXISTS bronze_state_fips')

(stateDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('bronze_state_fips'))