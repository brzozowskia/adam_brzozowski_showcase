# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Group 6
# MAGIC #### Final Project

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
# MAGIC #### Preliminary data analysis

# COMMAND ----------

dbutils.data.summarize(propertiesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dataset 2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Crosswalk data

# COMMAND ----------

# Dataset 2: Cities crosswalk table
files = [f"{dataStore}dataset_2/cities_crosswalk.csv"]

# Read CSV files
ccwDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(ccwDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(ccwDF.count()))

# Print schema
print(' ')
print('Schema:')
ccwDF.printSchema()

# COMMAND ----------

# Dataset 2: County crosswalk table
files = [f"{dataStore}dataset_2/CountyCrossWalk_Zillow.csv"]

# Read CSV files
ccwzDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(ccwzDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(ccwzDF.count()))

# Print schema
print(' ')
print('Schema:')
ccwzDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Time series data

# COMMAND ----------

# Dataset 2: Neighborhood time series table
files = [f"{dataStore}dataset_2/Neighborhood_time_series.csv"]

# Read CSV files
ntsDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(ntsDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(ntsDF.count()))

# Print schema
print(' ')
print('Schema:')
ntsDF.printSchema()

# COMMAND ----------

# Dataset 2: Zip time series table
files = [f"{dataStore}dataset_2/Zip_time_series.csv"]

# Read CSV files
ztsDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(ztsDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(ztsDF.count()))

# Print schema
print(' ')
print('Schema:')
ztsDF.printSchema()

# COMMAND ----------

# Dataset 2: County time series table
files = [f"{dataStore}dataset_2/County_time_series.csv"]

# Read CSV files
ctsDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(ctsDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(ctsDF.count()))

# Print schema
print(' ')
print('Schema:')
ctsDF.printSchema()

# COMMAND ----------

# Dataset 2: City time series table
files = [f"{dataStore}dataset_2/City_time_series.csv"]

# Read CSV files
citytsDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(citytsDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(citytsDF.count()))

# Print schema
print(' ')
print('Schema:')
citytsDF.printSchema()

# COMMAND ----------

# Dataset 2: Metro time series table
files = [f"{dataStore}dataset_2/Metro_time_series.csv"]

# Read CSV files
mtsDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(mtsDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(mtsDF.count()))

# Print schema
print(' ')
print('Schema:')
mtsDF.printSchema()

# COMMAND ----------

# Dataset 2: State time series table
files = [f"{dataStore}dataset_2/State_time_series.csv"]

# Read CSV files
stsDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

# Display dataframe
display(stsDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(stsDF.count()))

# Print schema
print(' ')
print('Schema:')
stsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dataset 3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Prices

# COMMAND ----------

# Dataset 3: Prices table
files = [f"{dataStore}dataset_3/price.csv"]

# Read CSV files
priceDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

priceDF = priceDF.withColumnRenamed('City Code', 'City_Code')
priceDF = priceDF.withColumnRenamed('Population Rank', 'Population_Rank')

priceDF = priceDF.withColumnRenamed('November 2010', 'nov_2010')
priceDF = priceDF.withColumnRenamed('December 2010', 'dec_2010')

priceDF = priceDF.withColumnRenamed('January 2011', 'jan_2011')
priceDF = priceDF.withColumnRenamed('February 2011', 'feb_2011')
priceDF = priceDF.withColumnRenamed('March 2011', 'mar_2011')
priceDF = priceDF.withColumnRenamed('April 2011', 'apr_2011')
priceDF = priceDF.withColumnRenamed('May 2011', 'may_2011')
priceDF = priceDF.withColumnRenamed('June 2011', 'jun_2011')
priceDF = priceDF.withColumnRenamed('July 2011', 'jul_2011')
priceDF = priceDF.withColumnRenamed('August 2011', 'aug_2011')
priceDF = priceDF.withColumnRenamed('September 2011', 'sep_2011')
priceDF = priceDF.withColumnRenamed('October 2011', 'oct_2011')
priceDF = priceDF.withColumnRenamed('November 2011', 'nov_2011')
priceDF = priceDF.withColumnRenamed('December 2011', 'dec_2011')

priceDF = priceDF.withColumnRenamed('January 2012', 'jan_2012')
priceDF = priceDF.withColumnRenamed('February 2012', 'feb_2012')
priceDF = priceDF.withColumnRenamed('March 2012', 'mar_2012')
priceDF = priceDF.withColumnRenamed('April 2012', 'apr_2012')
priceDF = priceDF.withColumnRenamed('May 2012', 'may_2012')
priceDF = priceDF.withColumnRenamed('June 2012', 'jun_2012')
priceDF = priceDF.withColumnRenamed('July 2012', 'jul_2012')
priceDF = priceDF.withColumnRenamed('August 2012', 'aug_2012')
priceDF = priceDF.withColumnRenamed('September 2012', 'sep_2012')
priceDF = priceDF.withColumnRenamed('October 2012', 'oct_2012')
priceDF = priceDF.withColumnRenamed('November 2012', 'nov_2012')
priceDF = priceDF.withColumnRenamed('December 2012', 'dec_2012')

priceDF = priceDF.withColumnRenamed('January 2013', 'jan_2013')
priceDF = priceDF.withColumnRenamed('February 2013', 'feb_2013')
priceDF = priceDF.withColumnRenamed('March 2013', 'mar_2013')
priceDF = priceDF.withColumnRenamed('April 2013', 'apr_2013')
priceDF = priceDF.withColumnRenamed('May 2013', 'may_2013')
priceDF = priceDF.withColumnRenamed('June 2013', 'jun_2013')
priceDF = priceDF.withColumnRenamed('July 2013', 'jul_2013')
priceDF = priceDF.withColumnRenamed('August 2013', 'aug_2013')
priceDF = priceDF.withColumnRenamed('September 2013', 'sep_2013')
priceDF = priceDF.withColumnRenamed('October 2013', 'oct_2013')
priceDF = priceDF.withColumnRenamed('November 2013', 'nov_2013')
priceDF = priceDF.withColumnRenamed('December 2013', 'dec_2013')

priceDF = priceDF.withColumnRenamed('January 2014', 'jan_2014')
priceDF = priceDF.withColumnRenamed('February 2014', 'feb_2014')
priceDF = priceDF.withColumnRenamed('March 2014', 'mar_2014')
priceDF = priceDF.withColumnRenamed('April 2014', 'apr_2014')
priceDF = priceDF.withColumnRenamed('May 2014', 'may_2014')
priceDF = priceDF.withColumnRenamed('June 2014', 'jun_2014')
priceDF = priceDF.withColumnRenamed('July 2014', 'jul_2014')
priceDF = priceDF.withColumnRenamed('August 2014', 'aug_2014')
priceDF = priceDF.withColumnRenamed('September 2014', 'sep_2014')
priceDF = priceDF.withColumnRenamed('October 2014', 'oct_2014')
priceDF = priceDF.withColumnRenamed('November 2014', 'nov_2014')
priceDF = priceDF.withColumnRenamed('December 2014', 'dec_2014')

priceDF = priceDF.withColumnRenamed('January 2015', 'jan_2015')
priceDF = priceDF.withColumnRenamed('February 2015', 'feb_2015')
priceDF = priceDF.withColumnRenamed('March 2015', 'mar_2015')
priceDF = priceDF.withColumnRenamed('April 2015', 'apr_2015')
priceDF = priceDF.withColumnRenamed('May 2015', 'may_2015')
priceDF = priceDF.withColumnRenamed('June 2015', 'jun_2015')
priceDF = priceDF.withColumnRenamed('July 2015', 'jul_2015')
priceDF = priceDF.withColumnRenamed('August 2015', 'aug_2015')
priceDF = priceDF.withColumnRenamed('September 2015', 'sep_2015')
priceDF = priceDF.withColumnRenamed('October 2015', 'oct_2015')
priceDF = priceDF.withColumnRenamed('November 2015', 'nov_2015')
priceDF = priceDF.withColumnRenamed('December 2015', 'dec_2015')

priceDF = priceDF.withColumnRenamed('January 2016', 'jan_2016')
priceDF = priceDF.withColumnRenamed('February 2016', 'feb_2016')
priceDF = priceDF.withColumnRenamed('March 2016', 'mar_2016')
priceDF = priceDF.withColumnRenamed('April 2016', 'apr_2016')
priceDF = priceDF.withColumnRenamed('May 2016', 'may_2016')
priceDF = priceDF.withColumnRenamed('June 2016', 'jun_2016')
priceDF = priceDF.withColumnRenamed('July 2016', 'jul_2016')
priceDF = priceDF.withColumnRenamed('August 2016', 'aug_2016')
priceDF = priceDF.withColumnRenamed('September 2016', 'sep_2016')
priceDF = priceDF.withColumnRenamed('October 2016', 'oct_2016')
priceDF = priceDF.withColumnRenamed('November 2016', 'nov_2016')
priceDF = priceDF.withColumnRenamed('December 2016', 'dec_2016')

priceDF = priceDF.withColumnRenamed('January 2017', 'jan_2017')

# Display dataframe
display(priceDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(priceDF.count()))

# Print schema
print(' ')
print('Schema:')
priceDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Prices per square feet

# COMMAND ----------

# Dataset 3: Prices per square feet table
files = [f"{dataStore}dataset_3/pricepersqft.csv"]

# Read CSV files
ppsqftDF = (spark.read
           .format('csv')
           .option("inferSchema", True)
           .option('header', True)
           .load(files))

ppsqftDF = ppsqftDF.withColumnRenamed('City Code', 'City_Code')
ppsqftDF = ppsqftDF.withColumnRenamed('Population Rank', 'Population_Rank')

ppsqftDF = ppsqftDF.withColumnRenamed('November 2010', 'nov_2010')
ppsqftDF = ppsqftDF.withColumnRenamed('December 2010', 'dec_2010')

ppsqftDF = ppsqftDF.withColumnRenamed('January 2011', 'jan_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('February 2011', 'feb_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('March 2011', 'mar_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('April 2011', 'apr_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('May 2011', 'may_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('June 2011', 'jun_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('July 2011', 'jul_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('August 2011', 'aug_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('September 2011', 'sep_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('October 2011', 'oct_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('November 2011', 'nov_2011')
ppsqftDF = ppsqftDF.withColumnRenamed('December 2011', 'dec_2011')

ppsqftDF = ppsqftDF.withColumnRenamed('January 2012', 'jan_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('February 2012', 'feb_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('March 2012', 'mar_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('April 2012', 'apr_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('May 2012', 'may_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('June 2012', 'jun_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('July 2012', 'jul_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('August 2012', 'aug_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('September 2012', 'sep_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('October 2012', 'oct_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('November 2012', 'nov_2012')
ppsqftDF = ppsqftDF.withColumnRenamed('December 2012', 'dec_2012')

ppsqftDF = ppsqftDF.withColumnRenamed('January 2013', 'jan_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('February 2013', 'feb_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('March 2013', 'mar_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('April 2013', 'apr_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('May 2013', 'may_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('June 2013', 'jun_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('July 2013', 'jul_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('August 2013', 'aug_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('September 2013', 'sep_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('October 2013', 'oct_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('November 2013', 'nov_2013')
ppsqftDF = ppsqftDF.withColumnRenamed('December 2013', 'dec_2013')

ppsqftDF = ppsqftDF.withColumnRenamed('January 2014', 'jan_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('February 2014', 'feb_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('March 2014', 'mar_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('April 2014', 'apr_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('May 2014', 'may_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('June 2014', 'jun_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('July 2014', 'jul_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('August 2014', 'aug_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('September 2014', 'sep_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('October 2014', 'oct_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('November 2014', 'nov_2014')
ppsqftDF = ppsqftDF.withColumnRenamed('December 2014', 'dec_2014')

ppsqftDF = ppsqftDF.withColumnRenamed('January 2015', 'jan_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('February 2015', 'feb_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('March 2015', 'mar_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('April 2015', 'apr_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('May 2015', 'may_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('June 2015', 'jun_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('July 2015', 'jul_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('August 2015', 'aug_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('September 2015', 'sep_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('October 2015', 'oct_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('November 2015', 'nov_2015')
ppsqftDF = ppsqftDF.withColumnRenamed('December 2015', 'dec_2015')

ppsqftDF = ppsqftDF.withColumnRenamed('January 2016', 'jan_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('February 2016', 'feb_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('March 2016', 'mar_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('April 2016', 'apr_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('May 2016', 'may_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('June 2016', 'jun_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('July 2016', 'jul_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('August 2016', 'aug_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('September 2016', 'sep_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('October 2016', 'oct_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('November 2016', 'nov_2016')
ppsqftDF = ppsqftDF.withColumnRenamed('December 2016', 'dec_2016')

ppsqftDF = ppsqftDF.withColumnRenamed('January 2017', 'jan_2017')


# Display dataframe
display(ppsqftDF)

# Display number of rows
print(' ')
print('Total of records: ' + str(ppsqftDF.count()))

# Print schema
print(' ')
print('Schema:')
ppsqftDF.printSchema()

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
# MAGIC - Property data for 2016 and 2017 added
# MAGIC - Lookup data added
# MAGIC
# MAGIC **Dataset 2**
# MAGIC - Crosswalk data added
# MAGIC - Time series data added
# MAGIC
# MAGIC **Dataset 3**
# MAGIC - Prices data added
# MAGIC - Prices per sqft added
# MAGIC
# MAGIC **State FIPS**
# MAGIC - State FIPS added

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dataset 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Properties table

# COMMAND ----------

# Store properties bronze table
(propertiesDF
   .write
   .format("delta")
   .mode("overwrite")
   .partitionBy("file_year")
   .option("overwriteSchema", "true")
   .saveAsTable('bronze_properties')
)

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
# MAGIC ### Dataset 2

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Crosswalk tables

# COMMAND ----------

# Store cities crosswalk table
spark.sql('DROP TABLE IF EXISTS bronze_cities_crosswalk')

(ccwDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_cities_crosswalk')
)

# COMMAND ----------

# Store counties crosswalk table
spark.sql('DROP TABLE IF EXISTS bronze_counties_crosswalk')

(ccwzDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_counties_crosswalk')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Time series tables

# COMMAND ----------

# Store neighborhood time series table
spark.sql('DROP TABLE IF EXISTS bronze_neighborhood_time_series')

(ntsDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_neighborhood_time_series')
)

# COMMAND ----------

# Store zip time series table
spark.sql('DROP TABLE IF EXISTS bronze_zip_time_series')

(ztsDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_zip_time_series')
)

# COMMAND ----------

# Store county time series table
spark.sql('DROP TABLE IF EXISTS bronze_county_time_series')

(ctsDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_county_time_series')
)

# COMMAND ----------

# Store city time series table
spark.sql('DROP TABLE IF EXISTS bronze_city_time_series')

(citytsDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_city_time_series')
)

# COMMAND ----------

# Store metro time series table
spark.sql('DROP TABLE IF EXISTS bronze_metro_time_series')

(mtsDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_metro_time_series')
)

# COMMAND ----------

# Store state time series table
spark.sql('DROP TABLE IF EXISTS bronze_state_time_series')

(stsDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_state_time_series')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dataset 3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Prices

# COMMAND ----------

# Prices table
spark.sql('DROP TABLE IF EXISTS bronze_price')

(priceDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_price')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Prices per square feet

# COMMAND ----------

# Prices per square feet table
spark.sql('DROP TABLE IF EXISTS bronze_price_per_sqft')

(ppsqftDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f'bronze_price_per_sqft')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### State FIPS

# COMMAND ----------

# Store building quality lookup table
spark.sql('DROP TABLE IF EXISTS bronze_state_fips')

(stateDF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('bronze_state_fips'))
   

# COMMAND ----------

display(stateDF)

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
# MAGIC - Partition by year in file name added. ### PENDING ###

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
# MAGIC ### Store silver data

# COMMAND ----------

# Store properties silver table

spark.sql('DROP TABLE IF EXISTS silver_properties')

(properties_2_DF
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('silver_properties')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # This section is for testing purposes only ** To be removed  **#

# COMMAND ----------

### TO BE REMOVED ###
# Tests for data analysis
# display(properties_1_DF.filter(properties_1_DF.typeconstructiontypeid.isNotNull()))

# from pyspark.sql.functions import avg
display(propertiesDF.select())

# COMMAND ----------

propertiesDF.createOrReplaceTempView('alvaro')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(bedroomcnt), bedroomcnt, PropertyLandUseDesc, AVG(taxamount)
# MAGIC FROM silver_properties
# MAGIC GROUP BY bedroomcnt, PropertyLandUseDesc
# MAGIC ORDER BY PropertyLandUseDesc, bedroomcnt
# MAGIC --WHERE bedroomcnt = 0 AND PropertyLandUseDesc = 'Single Family Residential'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PropertyLandUseDesc, 
# MAGIC        landtaxvaluedollarcnt, structuretaxvaluedollarcnt, taxvaluedollarcnt, 
# MAGIC        landtaxvaluedollarcnt / lotsizesquarefeet AS pricepersquarefeet_land,
# MAGIC        structuretaxvaluedollarcnt / lotsizesquarefeet AS pricepersquarefeet_structure, 
# MAGIC        taxvaluedollarcnt / lotsizesquarefeet AS pricepersquarefeet_total
# MAGIC FROM silver_properties

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver_properties

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver_airconditioning_type

# COMMAND ----------

