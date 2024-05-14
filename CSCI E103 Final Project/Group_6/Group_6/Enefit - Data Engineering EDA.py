# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### <img style="float: left; padding-right: 10px; width: 45px" src="https://upload.wikimedia.org/wikipedia/en/8/89/ExtensionFlag.png"> CSCI E-103 Data Engineering for Analytics to Solve Business Challenges
# MAGIC
# MAGIC # Final Project | Group 6
# MAGIC
# MAGIC **Student Name: Adam Brzozowski, Sam Ippisch, Stefan Mcneil, Gordon Hew, Brandon Hong, Noura Almansoori**
# MAGIC
# MAGIC **Harvard University**<br/>
# MAGIC **Fall 2023**<br/>
# MAGIC **Instructors**: Eric Gieseke ALM, Chief Executive Officer and Founder, Pago Capital | Anindita Mahapatra ALM, Solutions Architect, Databricks\
# MAGIC **Teaching Fellows** Ramdas Murali | Sriharsha Tikkireddy
# MAGIC
# MAGIC
# MAGIC <hr style="height:2pt">
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Group 6
# MAGIC #### Final Project - Bronze: Energy Prediction Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Introduction:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This workbook will provide the data modeling for use in the silver layer. Everything up until the end is exploratory and shows how we worked out what the joins should be.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##2 Data Engineer (20 pts):
# MAGIC
# MAGIC Ingesting the various datasets into delta lake tables.
# MAGIC Making the pipelines robust enough to be run “daily,” assume that you may get data refreshes daily
# MAGIC Stream from one delta table to another in an incremental fashion using trigger once.
# MAGIC Use delta and demonstrate upserts and merges in the gold layer to contain aggregates for reporting in Databricks SQL.
# MAGIC
# MAGIC Project requirements are located here:
# MAGIC https://docs.google.com/document/d/13W9iI0nbUPN36LWF2_OA_LHn7Ui7vJHDPhxSYb9KH08/edit

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Variables definition

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The first step in the process is to inventory raw source files - starting at the parent directory for the assignment. We can see that we only need to pay attention to one subdirectory - the energy-prediction directory for the scope of this project since that is the dataset chosen by the team:

# COMMAND ----------

# MAGIC %fs ls /mnt/data/2023-kaggle-final

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Next, we need to examine the contents of the energy-prediction subdirectory to review the files and variables of interest pursuant to building useful data tables:

# COMMAND ----------

# MAGIC %fs ls mnt/data/2023-kaggle-final/energy-prediction/

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Overview
# MAGIC
# MAGIC Adam's clean energy expertise was leveraged heavily for this part of the exercise. The dataset consists of 1.19 gb of files describing the movements of energy prices for behind the meter pv installations for businesses and consumers in estonia. **The prediction task per kaggle is to predict the amount of energy produced and consumed by these Estonian consumers and businesses - which the data dictionary calls "prosumers" (presumably because they produce and consume).**
# MAGIC
# MAGIC
# MAGIC ## Data review - Client.csv
# MAGIC
# MAGIC The first table that will be examined is the "client" table. The client table appears to correspond to the site where the onsite electric power generation is located. The data is coded for location via county. Geography can very useful in solar power generation because incident solar radiation is partially a function of latitude. Over large service territories this matters. In addition, we can see that date is provided - which is also critical because incident solar radiation is a function of the time of year as well - with peak incident solar radiation corresponding typically to July. During summer months, production could be plentiful and the market oversupplied, so we might paradoxically think that lower prices means greater supply vs a dumping type of behavior because the site is generating more power than it can use. In the winter, production is lower by design and so we could see price insensitivity in the reverse as well - whereby higher prices does not translate into different behavior because all energy is being consumed onsite.
# MAGIC
# MAGIC Product_type is undefined - but it could be that this is the brand of solar panel. There are differences in the photosynthetic conversion efficiency of different panels. It should be the case that the rated capacity - reported under installed_capacity of instantaneous energy production (in kilowatts - i.e. 1000 joules / second). However, rated capacity is also self reported by manufacturers and is not entirely objective. It will be interesting to see how important product_type is during the modeling process. Is_business is a boolean that indicates whether the site is a business or not. This is typically important from a behavioral standpoint because households are managed differently than businesses. It's unclear whether households would be more or less responsive to prince signals, but it's a good candidate variable as a behavioral factor. 
# MAGIC
# MAGIC Eic_count are European Union energy consumption points. This seems like it could be a rather different market incentive structure than in the United States and that this variable may be providing proxy signals to the end user. It would be good to explore this variable with more extensive EDA. 
# MAGIC
# MAGIC Data block id is when each block of data became available during the normal course of the workings of the energy market.
# MAGIC

# COMMAND ----------

client_temp = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/client.csv',header=True, inferSchema=True)

# COMMAND ----------

client_temp.show(5)

# COMMAND ----------

client_temp.createOrReplaceTempView("client_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There are sixteen counties in the dataset. Estonia is a relatively small geography, and so production differences may not vary much between counties as a function of latitude / incident solar radiation. However, there could be county level differences worth exploring. 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select county, count(*)
# MAGIC from client_temp
# MAGIC Group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Notably, the sample is balanced roughly evenly between businesses and residences. As such, there should not be substantial class imbalance.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select is_business, count(*)
# MAGIC from client_temp
# MAGIC Group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Next, let's explore the county mapping file. It's a json file that maps county ID to map names. We can see that there are names in Estonian for each of the counties. Utilizing these may be beneficial for plotting data on a map or with parsing variable importances if the county should prove to be important (which does not seem likely).
# MAGIC
# MAGIC

# COMMAND ----------

county_mapping = spark.read.format("json").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/county_id_to_name_map.json').createOrReplaceTempView("county_mapping")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from county_mapping

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Next up is the critical electricity prices file. We can see that this file contains reference data for hourly electricity prices on a euros per megawatt-hour basis. The price field is the only field defined in the data dictionary. However, it is reasonable to assume that the forecast date is the date of a forecast. Subtracting the origin_date from the forecast_date points to the origin_date being the day before the forecast in all cases. Most likely, the grid is operating on day ahead forecasting. We will not need to include the origin_date since it should not contain any information not already embedded into forecast_date. Data_block_id , according to the data dictionary from the Kaggle website, corresponds to windows of time in which data becomes available - which makes sense in the context of day ahead settling and clearing processes within energy futures markets.

# COMMAND ----------

electricity_prices = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/electricity_prices.csv', header=True, inferSchema=True).createOrReplaceTempView("electricity_prices")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *, datediff(a.forecast_date, a.origin_date) as days_diff
# MAGIC from electricity_prices as a limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Select datediff(a.forecast_date, a.origin_date) as days_diff, count(*) as count_forecast_datediff_length,
# MAGIC count(distinct a.data_block_id) as undup_data_blocks,
# MAGIC count(distinct origin_date) as undup_origin_date,
# MAGIC count(distinct forecast_date) as series_length_time
# MAGIC from electricity_prices as a 
# MAGIC Group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Forecast_weather.csv
# MAGIC The next file that looks interesting is the forecast_weather.csv file. Weather is important for solar production. Overcast or rainy days can decrease production. Excessive heat can result in greater onsite cooling loads that curtail exports to the grid. 

# COMMAND ----------

forecast_weather = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/forecast_weather.csv', header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Direct solar radiation, cloud cover, precipitation type measures predictably have a preponderance of zero values. These are not mistakes because of night time and times when it is not raining or cloudy. Furthermore, there is no missing data in this file. As such, it may be expedient to remove from the forecasting process all of the data for night time since solar production does not occur at night. As noted previously, the latitude and longitude does not vary substantially either. We will likely be able to drop these from the dataset for modeling and analytical purposes.

# COMMAND ----------

dbutils.data.summarize(forecast_weather)

# COMMAND ----------

forecast_weather.createOrReplaceTempView("forecast_weather")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from forecast_weather limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # gas_prices.csv
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Notably, the prices in gas_prices are in terms of $ / MWh equivalent and undoubtedly refer to the price of natural gas that would otherwise be used as input fuel into a natural gas fired electric power plant. Higher gas prices relative to electricity prices is referred to in the industry as a lower "spark spread" and decreases the attractiveness of using natural gas compared to other fuel sources. Other things being equal, I would expect higher natural gas prices to translate to higher electricity prices, and consequently more exports of photovoltaic power to the grid instead of consumption behind the meter.
# MAGIC
# MAGIC From the below, we can see the date of each energy forecast contains a low and high forecast price for power that comes one day ahead of the forecast date. There is substantial variability in the forecast range. However, as noted above, there is an open question regarding how functional the market is in terms of the ability of price to induce changes in supply response vis a vis production vs consumption. Nonetheless, these variables do appear to be worth examining within the context of this study. 

# COMMAND ----------

gas_prices = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/gas_prices.csv', header=True, inferSchema=True)

# COMMAND ----------

dbutils.data.summarize(gas_prices)

# COMMAND ----------

gas_prices.createOrReplaceTempView("gas_prices")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select a.*,datediff(a.forecast_date, a.origin_date) as days_diff
# MAGIC from gas_prices a 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select datediff(a.forecast_date, a.origin_date) as days_diff, count(*)
# MAGIC from gas_prices a 
# MAGIC Group by 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Historical_weather.csv
# MAGIC
# MAGIC Historical weather is just past weather data with the same fields. The same considerations apply.

# COMMAND ----------

historical_weather = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/historical_weather.csv', header=True, inferSchema=True)

# COMMAND ----------

dbutils.data.summarize(historical_weather)

# COMMAND ----------

historical_weather.createOrReplaceTempView("historical_weather")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Train.csv
# MAGIC
# MAGIC Next, we'll examine the train.csv file. The prediction target is called "target" in this file. Notably, we also have to predict whether it is consumption or production. The prediction_unit_id is a unique identifier for the site. Some sites are not in the test set. There's an assumption there that we will need to make site specific predictions rather than making predictions as a function of other attributes regardless of the site. 

# COMMAND ----------

train_csv = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/train.csv', header=True, inferSchema=True)

# COMMAND ----------

dbutils.data.summarize(train_csv)

# COMMAND ----------

train_csv.createOrReplaceTempView("train_csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(distinct date(datetime)), count(distinct data_block_id)
# MAGIC from train_csv

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Weather station to county mapping csv
# MAGIC
# MAGIC The last file we'll consider is the weather_station_to_county_mapping.csv file. It's unclear that this file will provide much value due to the high ratio of missing data. Moreover, it's only providing spatial lat-lon coordinates for the elements in the dataset to indicate which county they are in. 

# COMMAND ----------

weather_station_county_mapping = spark.read.format("csv").load('dbfs:/mnt/data/2023-kaggle-final/energy-prediction/weather_station_to_county_mapping.csv', header=True, inferSchema=True)

# COMMAND ----------

dbutils.data.summarize(weather_station_county_mapping)

# COMMAND ----------

weather_station_county_mapping.createOrReplaceTempView("weather_station_county_mapping")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from weather_station_county_mapping limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Next steps - data modeling
# MAGIC
# MAGIC The files above indicate that we should load all of the tables. It would then be expedient to develop an OLAP table with the following columns:
# MAGIC
# MAGIC train.csv: all columns\
# MAGIC gas_prices.csv: all columns\
# MAGIC client.csv: eic_count, installed_capacity\
# MAGIC electricity_prices.csv: all \
# MAGIC
# MAGIC Data_block_id corresponds to data that becomes available all at once on a given day as part of the workings of the energy markets. A key question in initially exploring the data was identifying the correct join conditions - which presumably should revolve around the data_block_id field. After examining this, we concluded that joining the date together using dates was more correct and would reduce interpretation complexity with modeling. It is not relevant when a foreast became available - only whether it matched well with the actuals. 
# MAGIC
# MAGIC # Join logic
# MAGIC
# MAGIC A key criterion for join guaranteeing join validity is to establish a baseline number of fact rows off of which predictions are made. Joins to other tables from train_csv should not create additional rows.  

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*)
# MAGIC from train_csv as a

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC From the below, we can see that joining on the date of the train_csv.datetime and forecast_date seems to meet this basic test. 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*)
# MAGIC from train_csv as a
# MAGIC left join gas_prices as b 
# MAGIC on date(a.datetime) = b.forecast_date

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Next, a visual inspection of the result is made to understand validity more concretely.

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *
# MAGIC from train_csv as a
# MAGIC left join gas_prices as b 
# MAGIC on date(a.datetime) = b.forecast_date

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Next, electricity prices will be merged on the forecast_date of the forecasted price and the datetime of the training data. The below record count proves that there is no multiplicity when adding this join:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*)
# MAGIC from train_csv as a
# MAGIC left join gas_prices as b
# MAGIC  on date(a.datetime) = b.forecast_date
# MAGIC left join electricity_prices as c on a.datetime = c.forecast_date

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select *
# MAGIC from train_csv as a
# MAGIC left join gas_prices as b 
# MAGIC on date(a.datetime) = b.forecast_date
# MAGIC left join electricity_prices as c on a.datetime = c.forecast_date

# COMMAND ----------

# MAGIC %md
# MAGIC Next, client will be joined into the OLAP dataframe:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(distinct concat(product_type, data_block_id, county))
# MAGIC from client_temp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Next, the client table is joined to the training data using the information contained in the data dictionary. We can see no multiplicity in the joins, and so this must be correct:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select count(*)
# MAGIC from train_csv as a
# MAGIC left join gas_prices as b
# MAGIC on date(a.datetime) = b.forecast_date
# MAGIC left join electricity_prices as c on a.datetime = c.forecast_date
# MAGIC left join client_temp d on d.product_type = a.product_type and d.county = a.county and d.is_business = a.is_business and d.date = date(a.datetime)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC After reviewing the data documentation more carefully, it seems that the data_block_ids do indeed need to line up. The client table is reference data for installed capacity. Over the short date window that we're talking about, that should be stable and no information leakage is expected. Joining just on dates alone does not work for the other fields because these fields are completely date bound and related to prices for that day. 
# MAGIC
# MAGIC # Finalized Joins!
# MAGIC
# MAGIC Below, the finalized joins are worked out to create a flattened table. 

# COMMAND ----------

# MAGIC %sql
# MAGIC
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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Decision
# MAGIC
# MAGIC We decided that the weather data was redundant with the electricity and natural gas price data for this dataset. Half of the series for renewable energy could be produced based only on the time of data local time - since solar power does not generate during the evening and the dataset reflected that. In addition, these markets are settled in advance of the day the weather is reported - so there doesn't seem to be much point to including this data. Lastly, any autoregressive lagged signal in this dataset is going to be reflected in the moving price signal for the gas and electricity series because energy supply and price varies inversely.

# COMMAND ----------

historical_weather.printSchema()

# COMMAND ----------

forecast_weather.printSchema()