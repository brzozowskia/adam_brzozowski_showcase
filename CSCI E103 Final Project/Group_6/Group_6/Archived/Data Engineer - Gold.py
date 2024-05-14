# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Group 6
# MAGIC #### Final Project - Gold Layer

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
# MAGIC ## Gold Database
# MAGIC
# MAGIC Gold layer stored in Delta format.
# MAGIC
# MAGIC **Transformations:**
# MAGIC - Unnecessary columns removed
# MAGIC - Unnecessary null rows removed
# MAGIC - Nulls in numerical columns converted to zeroes
# MAGIC - Nulls in boolean columns converted to False
# MAGIC - Categorical columns added

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read columns from silver

# COMMAND ----------

# Read the data from the silver_properties table as a DataFrame
silver_properties = spark.read.format("delta").table("silver_properties")

columns = ["bathroomcnt", "bedroomcnt", "BuildingQualityTypeDesc", "decktypeid", "calculatedfinishedsquarefeet", "fips", "fireplaceflag", "fullbathcnt", "hashottuborspa", "latitude", "longitude", "lotsizesquarefeet", "parcelid", "poolcnt", "pooltypeid2", "pooltypeid10", "propertycountylandusecode", "propertylandusetypeid", "PropertyLandUseDesc", "censustractandblock", "regionidzip", "roomcnt", "unitcnt", "yardbuildingsqft17", "yardbuildingsqft26", "taxvaluedollarcnt", "basementsqft", "state_code", "state_fips", "yearbuilt", "landtaxvaluedollarcnt"]

# Select All columns from List
gold_properties = silver_properties.select(*columns)

# Display gold_properties information
display(gold_properties)
gold_properties.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add calculated columns, replace nulls

# COMMAND ----------

from pyspark.sql.functions import when, lit, col, datediff, current_date, year

# New columns
# ===========

# Age_Group
years_difference = (year(current_date()) - col("yearbuilt"))
gold_properties_1 = gold_properties.withColumn("Age_Group", when(years_difference <= 10, "0 - 10 years old"))
gold_properties_1 = gold_properties_1.withColumn("Age_Group", when((years_difference > 10) & (years_difference <= 60), "10 - 60 years old").otherwise(col("Age_Group")))
gold_properties_1 = gold_properties_1.withColumn("Age_Group", when(years_difference > 60, "60 or more years old").otherwise(col("Age_Group")))

# decktypeid
gold_properties_1 = gold_properties_1.withColumn("hasdeck", when(col("decktypeid").isNull(), 0).otherwise(1))

# hasbasement
gold_properties_1 = gold_properties_1.withColumn("hasbasement", when(col("basementsqft").isNull(), 0).otherwise(1))

# hashottub
gold_properties_1 = gold_properties_1.withColumn("hashottub", when(col("pooltypeid10").isNull() & col("pooltypeid2").isNull(), 0).otherwise(1))

# hasyardstorage
gold_properties_1 = gold_properties_1.withColumn("hasyardstorage", when(col("yardbuildingsqft26").isNull(), 0).otherwise(1))


# landPricePerSqft
gold_properties_1 = gold_properties_1.withColumn("landpricepersqft", col("lotsizesquarefeet") / col("landtaxvaluedollarcnt"))

# lotexists
gold_properties_1 = gold_properties_1.withColumn("lotexists", when(col("lotsizesquarefeet").isNull(), 0).otherwise(1))

# patiosize
gold_properties_1 = gold_properties_1.withColumn("patiosize", when(col("yardbuildingsqft17").isNull(), "none"))
gold_properties_1 = gold_properties_1.withColumn("patiosize", when(col("yardbuildingsqft17") == 0, "none").otherwise(col("patiosize")))
gold_properties_1 = gold_properties_1.withColumn("patiosize", when((col("yardbuildingsqft17") > 0) & (col("yardbuildingsqft17") < 200), "small").otherwise(col("patiosize")))
gold_properties_1 = gold_properties_1.withColumn("patiosize", when((col("yardbuildingsqft17") >= 200) & (col("yardbuildingsqft17") < 300), "medium").otherwise(col("patiosize")))
gold_properties_1 = gold_properties_1.withColumn("patiosize", when((col("yardbuildingsqft17") >= 300) & (col("yardbuildingsqft17") < 400), "large").otherwise(col("patiosize")))
gold_properties_1 = gold_properties_1.withColumn("patiosize", when(col("yardbuildingsqft17") >= 400, "extra_large").otherwise(col("patiosize")))

# pool
gold_properties_1 = gold_properties_1.withColumn("pool", when(col("poolcnt").isNull(), "No").otherwise("Yes"))

# roomcntcat
gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when(col("roomcnt").isNull(), "none"))
gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when(col("roomcnt") == 0, "none").otherwise(col("roomcntcat")))
gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when((col("roomcnt") > 0) & (col("roomcnt") <= 4), "small").otherwise(col("roomcntcat")))
gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when((col("roomcnt") > 4) & (col("roomcnt") <= 8), "medium").otherwise(col("roomcntcat")))
gold_properties_1 = gold_properties_1.withColumn("roomcntcat", when(col("roomcnt") > 8, "large").otherwise(col("roomcntcat")))

# unitcntcat
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt").isNull(), 0))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 0, 0))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 1, 1))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 2, 2))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 3, 3))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") == 4, 4))
gold_properties_1 = gold_properties_1.withColumn("unitcntcat", when(col("unitcnt") >= 5, 5))



# Updated columns
# ===============

# BuildingQualityTypeDesc
gold_properties_1 = gold_properties_1.withColumn("BuildingQualityTypeDesc", when(col("BuildingQualityTypeDesc").isNull(), "No information").otherwise(col("BuildingQualityTypeDesc")))

# calculatedfinishedsquarefeet
gold_properties_1 = gold_properties.withColumn("calculatedfinishedsquarefeet", when(col("calculatedfinishedsquarefeet").isNull(), 0).otherwise(col("calculatedfinishedsquarefeet")))

# fireplacecnt
gold_properties_1 = gold_properties.withColumn("fireplaceflag", when(col("fireplaceflag").isNull(), False).otherwise(col("fireplaceflag")))

# fullbathcnt
gold_properties_1 = gold_properties.withColumn("fullbathcnt", when(col("fullbathcnt").isNull(), 0).otherwise(col("fullbathcnt")))

# hashottuborspa
gold_properties_1 = gold_properties.withColumn("hashottuborspa", when(col("hashottuborspa").isNull(), False).otherwise(col("hashottuborspa")))

# lotsizesquarefeet
gold_properties_1 = gold_properties.withColumn("lotsizesquarefeet", when(col("lotsizesquarefeet").isNull(), 0).otherwise(col("lotsizesquarefeet")))

# unitcnt
gold_properties_1 = gold_properties.withColumn("unitcnt", when(col("unitcnt").isNull(), 0).otherwise(col("unitcnt")))


# Display gold table
display(gold_properties_1)
gold_properties_1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Drop rows with nulls in given columns

# COMMAND ----------

# Drop rows with Nulls in given columns
gold_properties_1 = gold_properties_1.na.drop(subset=["bathroomcnt", "bedroomcnt", "fips", "latitude", "longitude", "propertycountylandusecode", "propertylandusetypeid", "PropertyLandUseDesc", "regionidzip", "censustractandblock", "state_code", "roomcnt", "taxvaluedollarcnt"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Store gold data

# COMMAND ----------

# Store properties gold table

spark.sql('DROP TABLE IF EXISTS gold_properties')

(gold_properties_1
   .write
   .format("delta")
   .mode("overwrite")
  #  .partitionBy("regionidneighborhood")
   .saveAsTable('gold_properties')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM gold_properties