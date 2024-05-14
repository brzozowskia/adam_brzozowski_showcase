# Databricks notebook source
import re

dataStore = "/FileStore/group_06/"
databaseName = "group_06"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT regionidneighborhood, COUNT(*) AS Properties
# MAGIC FROM bronze_properties
# MAGIC GROUP BY regionidneighborhood

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

propertiestDF_4.write.format("delta").mode("overwrite").partitionBy("propfile_year").option("overwriteSchema", "true").saveAsTable(f"{databaseName}.tempoPart")

# COMMAND ----------

# MAGIC %sql
# MAGIC --- TEST ---
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver_properties

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS test_coordinates;
# MAGIC
# MAGIC CREATE TABLE test_coordinates
# MAGIC AS 
# MAGIC -- SELECT DISTINCT CAST(int(latitude / 1000000) as varchar(10)) AS lat, CAST(int(longitude / 1000000) AS varchar(10)) AS lon
# MAGIC -- FROM silver_properties
# MAGIC -- ORDER BY lat, lon
# MAGIC SELECT DISTINCT latitude / 1000000 AS lat, longitude / 1000000 AS lon
# MAGIC FROM silver_properties
# MAGIC ORDER BY lat, lon

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT latitude / 1000000 AS lat, longitude / 1000000 AS lon
# MAGIC FROM bronze_properties

# COMMAND ----------

# MAGIC %sql
# MAGIC --- TEST ---
# MAGIC
# MAGIC SELECT DISTINCT(state_fips), COUNT(state_fips)
# MAGIC FROM silver_properties
# MAGIC GROUP BY state_fips

# COMMAND ----------

# MAGIC %sql
# MAGIC --- TEST ---
# MAGIC
# MAGIC SELECT DISTINCT(fips), COUNT(fips)
# MAGIC FROM bronze_properties
# MAGIC GROUP BY fips

# COMMAND ----------

# Rename ID column to avoid ambiguities in join
acDF = acDF.withColumnRenamed('AirConditioningTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
asDF = asDF.withColumnRenamed('ArchitecturalStyleTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
bcDF = bcDF.withColumnRenamed('BuildingClassTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
hsDF = hsDF.withColumnRenamed('HeatingOrSystemTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
pluDF = pluDF.withColumnRenamed('PropertyLandUseTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
storyDF = storyDF.withColumnRenamed('StoryTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
tcDF = tcDF.withColumnRenamed('TypeConstructionTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
bqDF = bqDF.withColumnRenamed('BuildingQualityTypeID', 'joinID')

-----

# Rename ID column to avoid ambiguities in join
ac_1_DF = acDF.withColumnRenamed('AirConditioningTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
as_1_DF = asDF.withColumnRenamed('ArchitecturalStyleTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
bc_1_DF = bcDF.withColumnRenamed('BuildingClassTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
hs_1_DF = hsDF.withColumnRenamed('HeatingOrSystemTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
plu_1_DF = pluDF.withColumnRenamed('PropertyLandUseTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
story_1_DF = storyDF.withColumnRenamed('StoryTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
tc_1_DF = tcDF.withColumnRenamed('TypeConstructionTypeID', 'joinID')

# Rename ID column to avoid ambiguities in join
bq_1_DF = bqDF.withColumnRenamed('BuildingQualityTypeID', 'joinID')


# COMMAND ----------

### TEST ###
spark.sql('DROP TABLE IF EXISTS bronze_airconditioning_type')
# display(spark.sql('SELECT * FROM bronze_airconditioning_type'))

# COMMAND ----------

# MAGIC %sql
# MAGIC --- ### TEST ###
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM bronze_buildingquality_type

# COMMAND ----------

### ANALYSIS ###

# Houses with no bedrooms
landUseType = [264]
display(propertiesDF.filter((propertiesDF.propertylandusetypeid.isin(landUseType)) & (propertiesDF.bedroomcnt == 0)))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Streaming

# COMMAND ----------

# SUCCEED

# Read the data from the silver_properties table as a DataFrame
silver_properties = spark.table("silver_properties")

# Create a new DataFrame called streaming_properties by selecting the specified columns from silver_properties
streaming_properties = (
    silver_properties
    .select("parcelid", "latitude", "longitude", "yearbuilt", "taxvaluedollarcnt", "calculatedfinishedsquarefeet")
)

display(streaming_properties)

# COMMAND ----------

# SUCCEED

# Group the rows in the streaming_properties DataFrame by the yearbuilt column and count the number of rows in each group
yearbuilt_counts = (
    streaming_properties
    .groupBy("yearbuilt")
    .count()
)

# Show the data in the yearbuilt_counts DataFrame in a scatter graph format
yearbuilt_counts.display()

# COMMAND ----------

# COPIED

# Write the data from the bronze_properties DataFrame to the streaming_properties table as a streaming query
query = (
    bronze_properties
    .writeStream
    .outputMode("append")
    .format("memory")
    .queryName("streaming_properties")
    .start()
)

# Wait for the streaming query to be terminated
# query.awaitTermination()

# Show the data in the silver_properties table in a graphical format
spark.table("streaming_properties").display()

# COMMAND ----------

# COPIED

from pyspark.sql.types import *
from pyspark.sql.functions import *

streamingInputDF = (
  spark
  .readStream
  .format("delta")
  .table("bronze_properties")
)

streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.yearbuilt,
      streamingInputDF.architecturalstyletypeid
    )
    .count()
)

streamingCountsDF.isStreaming

# COMMAND ----------

# COPIED

streamingInputDF.isStreaming

# COMMAND ----------

# COPIED

# query = (streamingCountsDF
#    .writeStream
#    .format("memory")
#    .queryName('counts')
#    .outputMode("complete")
#    .option("ignoreChanges", "true")
#    .option("checkpointLocation", "/tmp/_checkpoints2/")
#    .start()
# )

# Works but do not stream
# query = (streamingCountsDF.writeStream
#    .format("delta")
#    .queryName('counts')
#    .outputMode("complete")
#    .option("checkpointLocation", "/tmp/_checkpoints3/")
#    .toTable("events2")
# )

query = (streamingCountsDF.writeStream
   .trigger(processingTime = '5 seconds')
   .format("delta")
   .queryName('counts')
   .outputMode("complete")
   .option("checkpointLocation", "/tmp/_checkpoints4/")
   .toTable("events2")
)

# COMMAND ----------

# COPIED

# Store as delta tabl with processing time = 30 seconds
writer = streamingInputDF.writeStream.trigger(processingTime = '2 seconds').format('delta').outputMode('append').option('checkPointLocation', 'tmp/_checkpoints5/').toTable("streaming_2")

# COMMAND ----------

# COPIED

streamingCountsDF.isStreaming

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- COPIED
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM streaming_2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- COPIED
# MAGIC
# MAGIC SELECT *
# MAGIC from events2
# MAGIC order by yearbuilt desc

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE gold_properties (name VARCHAR(64), address VARCHAR(64), student_id INT)
# MAGIC     USING PARQUET PARTITIONED BY (student_id);
# MAGIC
# MAGIC INSERT INTO gold_properties
# MAGIC     VALUES ('Amy Smith', '123 Park Ave, San Jose', 111111);
# MAGIC
# MAGIC CREATE TABLE silver_streaming (name VARCHAR(64), address VARCHAR(64), student_id INT)
# MAGIC     USING PARQUET PARTITIONED BY (student_id);
# MAGIC
# MAGIC INSERT INTO silver_streaming
# MAGIC     VALUES ('Amy Smith', '123 Park Ave, San Jose', 111111);

# COMMAND ----------

display(gold_properties_1.distinct(""))

# COMMAND ----------

#Replace 0 for null on only population column 
df.na.fill(value=0,subset=["hasbasement"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Streaming

# COMMAND ----------

# Delete checkpoint before start streaming
dbutils.fs.rm("/tmp/_checkpoint", True)

# Read the data from the silver_properties Delta table as a streaming DataFrame
silver_properties = spark.readStream.format("delta").table("silver_properties")

# Start running the query that prints the data to the console
# query = silver_properties.writeStream.outputMode("append").format("console").start()

query = (
    silver_properties
    .writeStream
    .outputMode("append")
    .format("delta")
    .option("path", "silver_streaming")
    .option("checkpointLocation", "/tmp/_checkpoint")
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Gold Database Experimentation

# COMMAND ----------

import re

dataStore = "/FileStore/group_06/"
databaseName = "group_06"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")
spark.sql(f"use {databaseName}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM gold_properties

# COMMAND ----------


# Read the data from the silver_properties table as a DataFrame
silver_properties = spark.read.format("delta").table("silver_properties")

columns = ["parcelid", "bathroomcnt", "bedroomcnt", "decktypeid", "fips", "fireplaceflag", "fullbathcnt", "hashottuborspa", "poolcnt", "roomcnt", "unitcnt", "yardbuildingsqft17", "yearbuilt"]

# Select All columns from List
gold_properties_1 = silver_properties.select(*columns)
# gold_properties_1 = (silver_properties.select("parcelid", "bathroomcnt", "bedroomcnt", "decktypeid", "fips", "fireplaceflag", "fullbathcnt", "hashottuborspa", "poolcnt", "basementsqft", "yearbuilt"))

# Full table
# rawDF = spark.read.format("delta").table("silver_properties")

# gold_properties_1.na.drop("any")
gold_properties_1 = gold_properties_1.na.drop(subset=["unitcnt"]) 

gold_properties_1 = gold_properties_1.limit(100)
display(gold_properties_1)

# COMMAND ----------

from pyspark.sql.functions import when, lit, col, datediff, current_date, year

#goldDF = goldDF.withColumn("haspool", when(col("poolcnt").isNull(), 0).otherwise(col("poolcnt")))

# years_difference = (year(current_date()) - col("yearbuilt"))
# goldDF = goldDF.withColumn("Age_Group", when(years_difference <= 10, "0 - 10 years old"))
# goldDF = goldDF.withColumn("Age_Group", when((years_difference > 10) & (years_difference <= 60), "10 - 60 years old").otherwise(col("Age_Group")))
# goldDF = goldDF.withColumn("Age_Group", when(years_difference > 60, "60 or more years old").otherwise(col("Age_Group")))

# gold_properties_1 = gold_properties_1.withColumn("patiosize", when(col("yardbuildingsqft17").isNull(), "none"))
# gold_properties_1 = gold_properties_1.withColumn("patiosize", when(col("yardbuildingsqft17") == 0, "none").otherwise(col("patiosize")))
# gold_properties_1 = gold_properties_1.withColumn("patiosize", when((col("yardbuildingsqft17") > 0) & (col("yardbuildingsqft17") < 200), "small").otherwise(col("patiosize")))
# gold_properties_1 = gold_properties_1.withColumn("patiosize", when((col("yardbuildingsqft17") >= 200) & (col("yardbuildingsqft17") < 300), "medium").otherwise(col("patiosize")))
# gold_properties_1 = gold_properties_1.withColumn("patiosize", when((col("yardbuildingsqft17") >= 300) & (col("yardbuildingsqft17") < 400), "large").otherwise(col("patiosize")))
# gold_properties_1 = gold_properties_1.withColumn("patiosize", when(col("yardbuildingsqft17") >= 400, "extra_large").otherwise(col("patiosize")))

# gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when(col("roomcnt").isNull(), "none"))
# gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when(col("roomcnt") == 0, "none").otherwise(col("roomcntcat")))
# gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when((col("roomcnt") > 0) & (col("roomcnt") <= 4), "small").otherwise(col("roomcntcat")))
# gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when((col("roomcnt") > 4) & (col("roomcnt") <= 8), "medium").otherwise(col("roomcntcat")))
# gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when(col("roomcnt") > 8, "large").otherwise(col("roomcntcat")))

gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt").isNull(), 0))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 0, 0).otherwise(col("unitcntcat")))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 1, 1).otherwise(col("unitcntcat")))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 2, 2).otherwise(col("unitcntcat")))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 3, 3).otherwise(col("unitcntcat")))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 4, 4).otherwise(col("unitcntcat")))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") >= 5, 5).otherwise(col("unitcntcat")))

display(gold_properties_1)

# COMMAND ----------

(years_difference)

# COMMAND ----------

# Drop rows with Nulls in given columns

goldDF = goldDF.na.drop(subset=["bathroomcnt", "bedroomcnt", "fips", "latitude", "longitude", "propertycountylandusecode", "propertylandusetypeid", "PropertyLandUseDesc", "regionidzip", "censustractandblock", "state_code", "roomcnt", "taxvaluedollarcnt"])

goldDF.createOrReplaceTempView('gold_experiments')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM gold_experiments