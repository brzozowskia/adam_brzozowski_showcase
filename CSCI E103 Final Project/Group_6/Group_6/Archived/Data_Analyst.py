# Databricks notebook source
# MAGIC %md 
# MAGIC ## Zillow’s Home Value
# MAGIC This dashboard shows the statistics and KPIs of real estate properties on Zillow platform. Data for this dashboard was distributed on Kaggle website https://www.kaggle.com/competitions/zillow-prize-1/data.

# COMMAND ----------

# MAGIC %md
# MAGIC ![my_test_image](files/zillow.png)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from `hive_metastore`.`group_06`.`silver_properties`

# COMMAND ----------

bronze_df = spark.read.format("csv").option("header","true").load(f"dbfs:/FileStore/group_06/dataset_1/properties_2016.csv")
display(bronze_df)

# COMMAND ----------

bronze_df_2017 = spark.read.format("csv").option("header","true").load(f"dbfs:/FileStore/group_06/dataset_1/properties_2017.csv")
display(bronze_df_2017)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select state_code, round(avg(taxvaluedollarcnt),0) as Avg_Zestimate
# MAGIC from `hive_metastore`.`group_06`.`silver_properties`
# MAGIC where taxvaluedollarcnt<>0
# MAGIC group by state_code
# MAGIC

# COMMAND ----------

location = spark.sql("Select a.latitude/1000000, a.longitude/1000000, a.bedroomcnt as Bedroom_Count, round(a.landtaxvaluedollarcnt/(a.lotsizesquarefeet*100),0) as landestimate_tens, b.BuildingQualityTypeDesc from `hive_metastore`.`group_06`.`silver_properties` as a left join `hive_metastore`.`group_06`.`silver_buildingquality_type` as b on a.buildingqualitytypeid = b.joinID where a.taxvaluedollarcnt is not null and bedroomcnt <> 0 and a.buildingqualitytypeid is not null")

display(location)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select a.latitude/1000000, a.longitude/1000000, a.landtaxvaluedollarcnt, a.lotsizesquarefeet, round(a.landtaxvaluedollarcnt/(a.lotsizesquarefeet*100),0) as landestimate_group
# MAGIC from `hive_metastore`.`group_06`.`silver_properties` as a 
# MAGIC left join `hive_metastore`.`group_06`.`silver_buildingquality_type` as b 
# MAGIC on a.buildingqualitytypeid = b.joinID 
# MAGIC where a.landtaxvaluedollarcnt is not null and a.lotsizesquarefeet is not null and a.landtaxvaluedollarcnt< 500000 and a.lotsizesquarefeet < 12000 and a.lotsizesquarefeet > 1600

# COMMAND ----------

# MAGIC %sql
# MAGIC Select latitude/1000000, longitude/1000000, round(yearbuilt/10,0) as yeabuilt_group, yearbuilt
# MAGIC from `hive_metastore`.`group_06`.`silver_properties` 
# MAGIC where lotsizesquarefeet is not null and yearbuilt is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC Select parcelid, structuretaxvaluedollarcnt
# MAGIC from `hive_metastore`.`group_06`.`silver_properties`
# MAGIC where structuretaxvaluedollarcnt<300000

# COMMAND ----------

# MAGIC %sql
# MAGIC Select b.propertylandusedesc, round(avg(a.yearbuilt),0) as Avg_Yeabuilt, format_number(round(avg(a.structuretaxvaluedollarcnt),0), " ,###") as Avg_Value 
# MAGIC from `hive_metastore`.`group_06`.`silver_properties` as a 
# MAGIC left join `hive_metastore`.`group_06`.`silver_propertylanduse_type` as b
# MAGIC on a.propertylandusetypeid = b.joinID
# MAGIC where b.propertylandusedesc is not null and a.yearbuilt is not null and a.yearbuilt>1900 and a.structuretaxvaluedollarcnt<300000
# MAGIC group by b.propertylandusedesc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Properties current condition reported

# COMMAND ----------

# MAGIC %sql
# MAGIC Select format_number(count(*)/1000, ',### K') as Count, format_number(c.Total/1000, ',### K') as Total, format_number((count(*)/c.Total), ',### %') as Quality_Percent, round(count(*)/c.Total,2) as Property_Current_Condition_Percent, b.BuildingQualityTypeDesc as Property_Current_Condition
# MAGIC from `hive_metastore`.`group_06`.`silver_properties` a
# MAGIC   left join `hive_metastore`.`group_06`.`silver_buildingquality_type` b
# MAGIC   on a.buildingqualitytypeid = b.joinID
# MAGIC   left join (Select count(*) as total from `hive_metastore`.`group_06`.`silver_properties` where buildingqualitytypeid is not null) c
# MAGIC where b.BuildingQualityTypeDesc is not null
# MAGIC group by b.BuildingQualityTypeDesc, c.total
# MAGIC order by count(*) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC Select a.Yearbuilt , b.HeatingOrSystemDesc, count(*) as Count
# MAGIC from `hive_metastore`.`group_06`.`silver_properties` as a 
# MAGIC left join `hive_metastore`.`group_06`.`silver_heatingorsystem_type` as b
# MAGIC on a.heatingorsystemtypeid = b.joinID
# MAGIC where a.yearbuilt is not null
# MAGIC group by a.yearbuilt, b.HeatingOrSystemDesc

# COMMAND ----------

# MAGIC %sql
# MAGIC Select case when a.yearbuilt> YEAR(DATEADD (YEAR, -10, current_date())) then '0-10 years old'
# MAGIC             when a.yearbuilt< YEAR(DATEADD (YEAR, -60, current_date())) then '10-60 years old'
# MAGIC             else '60 or more years old'
# MAGIC        end as Age_Group, b.HeatingOrSystemDesc , 
# MAGIC        count(*) as Count, c.Total, round(count(*)/c.Total,4)*100 as HeatingOrSystemPercent
# MAGIC from `hive_metastore`.`group_06`.`silver_properties` as a 
# MAGIC left join `hive_metastore`.`group_06`.`silver_heatingorsystem_type` as b
# MAGIC on a.heatingorsystemtypeid = b.joinID
# MAGIC left join (Select count(*) as Total from `hive_metastore`.`group_06`.`silver_properties` where heatingorsystemtypeid is not null) c
# MAGIC --where a.heatingorsystemtypeid is not null
# MAGIC group by Age_Group, b.HeatingOrSystemDesc, c.Total
# MAGIC order by round(count(*)/c.Total,4) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Yearbuilt , HeatingOrSystemDesc, count(*) as Count
# MAGIC from `hive_metastore`.`group_06`.`silver_properties`
# MAGIC where yearbuilt is not null and yearbuilt>1900 and HeatingOrSystemDesc in ('Solar', 'Geo Thermal')
# MAGIC group by yearbuilt, HeatingOrSystemDesc

# COMMAND ----------

spark.sql(f"use group_06")
fake_zestimate = spark.sql("Select parcelid, case when MOD(taxvaluedollarcnt,2)=0 then taxvaluedollarcnt*1.03 else taxvaluedollarcnt*0.98 end as zestimate from silver_properties where file_year = '2016'")

# COMMAND ----------

fake_zestimate.show()

# COMMAND ----------

(fake_zestimate
   .write
   .format("delta")
   .mode("overwrite")
   .saveAsTable('fake_zestimate')
)