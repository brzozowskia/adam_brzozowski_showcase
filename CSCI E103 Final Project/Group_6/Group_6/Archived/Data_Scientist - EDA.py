# Databricks notebook source
df_silver = spark.sql(f"Select * from `hive_metastore`.`group_06`.`silver_properties`")
df_silver.createOrReplaceTempView("sl_matt")
display(df_silver)

# COMMAND ----------

numvals = dict()
for col in df_silver.columns:
  query = f"SELECT COUNT({col}) FROM `hive_metastore`.`group_06`.`silver_properties` WHERE {col} IS NOT NULL"
  x = spark.sql(query)
  cn = f"count({col})"
  numvals[col] = x.head(1)[0]
  
print(numvals)

# COMMAND ----------

for key, val in numvals.items():
  print(f"Column {key} has {val[0]/5970434} percent non-null values.")

# COMMAND ----------

# MAGIC %md
# MAGIC Airconditioningtypeid:
# MAGIC   - "None" is an option
# MAGIC   - Since only 27% of properties have air conditioning information available, this might be a dropped column.
# MAGIC   
# MAGIC architecturalstyletypeid:
# MAGIC   - .2% populated
# MAGIC   - Drop the column
# MAGIC   
# MAGIC basementsqft:
# MAGIC   - .05% populated
# MAGIC   - We're working with california data so likely this is because basements don't exist
# MAGIC   - https://housegrail.com/why-are-there-no-basements-in-california/
# MAGIC   - Safe to assume NULL means no basement??  Probably - lowest value in this column is 20sqft
# MAGIC   - Calculate - Null == 0 AND/OR new column, has_basement -> Null = 0 (no), populated = 1 (yes)
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT basementsqft FROM sl_matt;

# COMMAND ----------

# MAGIC %md
# MAGIC Column bathroomcnt has 0.9975849326866355 percent non-null values.<br>
# MAGIC Column bedroomcnt has 0.9975889524949108 percent non-null values.<br>
# MAGIC Column calculatedbathnbr has 0.958785575721966 percent non-null values.
# MAGIC   - See if there are zeroes... yes.  Could a null value mean there's no building?\
# MAGIC   - Possibly - However, many of the rows where these are null, are only null values.
# MAGIC   - Drop null rows (we still have 99.7% of the data)
# MAGIC   - bathroomcnt and calculatedbathnbr are the same; drop calculatedbathnbr

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sl_matt
# MAGIC WHERE bathroomcnt IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT calculatedbathnbr, bathroomcnt FROM sl_matt
# MAGIC WHERE calculatedbathnbr != bathroomcnt

# COMMAND ----------

# MAGIC %md
# MAGIC Column buildingclasstypeid has 0.00424759741084149 percent non-null values.
# MAGIC   - Drop this
# MAGIC   
# MAGIC Column buildingqualitytypeid has 0.6498494079324887 percent non-null values.<br>
# MAGIC Column buildingqualitytype has 0.6498494079324887 percent non-null values.
# MAGIC   - Only 65% populated; from 1 to 12
# MAGIC   - Given the scarcity in some quality values, drop buildingqualitytypeid
# MAGIC   - Keep buildingqualitytype (not in silver? -> can calculate) & create dummy flags:
# MAGIC     1. Bad (9-12)
# MAGIC     2. Good (5-8)
# MAGIC     3. Very Good (1-4)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT buildingqualitytypeid, COUNT(*) FROM sl_matt
# MAGIC WHERE buildingqualitytypeid IS NOT NULL
# MAGIC GROUP BY buildingqualitytypeid
# MAGIC ORDER BY buildingqualitytypeid ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column decktypeid has 0.005774287095376986 percent non-null values.
# MAGIC - I wonder if this is the same as with basements?
# MAGIC - Given only .5% has a value, and the value is either 66 or NULL...
# MAGIC - DROP or Calculate to 0 (null - no deck) or 1 (66 - deck)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT decktypeid, COUNT(*) FROM sl_matt
# MAGIC GROUP BY decktypeid
# MAGIC ORDER BY decktypeid ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column finishedfloor1squarefeet has 0.06808131536166383 percent non-null values.<br>
# MAGIC Column calculatedfinishedsquarefeet has 0.9831399191415565 percent non-null values.<br>
# MAGIC Column finishedsquarefeet12 has 0.9094765975136816 percent non-null values.<br>
# MAGIC Column finishedsquarefeet13 has 0.0025698299319613952 percent non-null values.<br>
# MAGIC Column finishedsquarefeet15 has 0.0638116090053085 percent non-null values.<br>
# MAGIC Column finishedsquarefeet50 has 0.06808131536166383 percent non-null values.<br>
# MAGIC Column finishedsquarefeet6 has 0.0072830551346853515 percent non-null values.<br>
# MAGIC   - All of these are measuring a similar thing...
# MAGIC   - Calculated can replace all the others, most likely - I don't see what extra value these add?
# MAGIC   - Most useful might be 50, which has finished living area on the entry level of the home... but it is missing values for almost all rows, so not useful...
# MAGIC   - DROP all which are not calculated.
# MAGIC   - Nulls in calculated? Change to zero (assume no finished living space - revisit this)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT finishedfloor1squarefeet, finishedsquarefeet6, finishedsquarefeet12, finishedsquarefeet13, finishedsquarefeet15, finishedsquarefeet50, calculatedfinishedsquarefeet FROM sl_matt
# MAGIC WHERE calculatedfinishedsquarefeet IS NOT NULL AND
# MAGIC calculatedfinishedsquarefeet != finishedfloor1squarefeet OR
# MAGIC calculatedfinishedsquarefeet != finishedsquarefeet6 OR
# MAGIC calculatedfinishedsquarefeet != finishedsquarefeet12 OR
# MAGIC calculatedfinishedsquarefeet != finishedsquarefeet13 OR
# MAGIC calculatedfinishedsquarefeet != finishedsquarefeet15 OR
# MAGIC calculatedfinishedsquarefeet != finishedsquarefeet50;

# COMMAND ----------

# MAGIC %md
# MAGIC Column fips has 0.997593307287209 percent non-null values.
# MAGIC   - Keep
# MAGIC   - It appears nulls => no data, since all other columns in the row are null.
# MAGIC   - Drop the null rows.
# MAGIC
# MAGIC Column fireplacecnt has 0.10480996858854817 percent non-null values.<br>
# MAGIC Column fullbathcnt has 0.958785575721966 percent non-null values.
# MAGIC   - Keep
# MAGIC   - Nulls => No fireplace, or no full bath
# MAGIC
# MAGIC Column garagecarcnt has 0.29717688864829594 percent non-null values.
# MAGIC Column garagetotalsqft has 0.29717688864829594 percent non-null values.
# MAGIC   - Null => No data collected; what to do?  Probably drop

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT fips, COUNT(*) FROM sl_matt
# MAGIC GROUP BY fips
# MAGIC ORDER BY fips ASC;
# MAGIC
# MAGIC SELECT * FROM sl_matt
# MAGIC WHERE fips IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT fireplacecnt, COUNT(*) FROM sl_matt
# MAGIC GROUP BY fireplacecnt
# MAGIC ORDER BY fireplacecnt ASC;
# MAGIC
# MAGIC SELECT DISTINCT fullbathcnt, COUNT(*) FROM sl_matt
# MAGIC GROUP BY fullbathcnt
# MAGIC ORDER BY fullbathcnt ASC;
# MAGIC
# MAGIC SELECT DISTINCT garagecarcnt, COUNT(*) FROM sl_matt
# MAGIC GROUP BY garagecarcnt
# MAGIC ORDER BY garagecarcnt ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column hashottuborspa has 0.019944278757624657 percent non-null values.
# MAGIC   - Values are NULL or TRUE -> we can keep & assume null => false

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT hashottuborspa, COUNT(*) FROM sl_matt
# MAGIC GROUP BY hashottuborspa
# MAGIC ORDER BY hashottuborspa ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column heatingorsystemtypeid has 0.615627775133265 percent non-null values.
# MAGIC   - Drop this; many nulls, but also useless data
# MAGIC   - For instance, Yes and None exist, but so do a number of other values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT heatingorsystemtypeid, COUNT(*) FROM sl_matt
# MAGIC GROUP BY heatingorsystemtypeid
# MAGIC ORDER BY heatingorsystemtypeid ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column latitude has 0.997593307287209 percent non-null values.<br>
# MAGIC Column longitude has 0.997593307287209 percent non-null values.<br>
# MAGIC Column lotsizesquarefeet has 0.9080795466460228 percent non-null values.<br>
# MAGIC   - .9975849326866355 is the "bathroom count" not null % which is very similar.
# MAGIC   - Why would a property not have a lot size?  Probably if it's an apartment building
# MAGIC   - Drop null rows from lat/lon, lot size null -> "not applicable"
# MAGIC   - Maybe add a flag which is "has its own lot"?  And nulls => 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT lotsizesquarefeet, COUNT(*) FROM sl_matt
# MAGIC GROUP BY lotsizesquarefeet
# MAGIC ORDER BY lotsizesquarefeet ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column poolcnt has 0.17706685979612202 percent non-null values.<br>
# MAGIC Column poolsizesum has 0.00936581829729631 percent non-null values.<br>
# MAGIC Column pooltypeid10 has 0.009035356558668934 percent non-null values.<br>
# MAGIC Column pooltypeid2 has 0.010908922198955721 percent non-null values.<br>
# MAGIC Column pooltypeid7 has 0.16604387553735625 percent non-null values.<br>
# MAGIC   - poolcnt -> Null => No, 1 => Yes.  So, Null -> 0
# MAGIC   - pool sizenum is total sqft of pools on property.  The fact that it is only .9% populated (compared to 17% for pool cnt) means it's under-counted (drop)
# MAGIC   - pool type id is a category of what kind of pool we have.  It should sum to poolcnt.
# MAGIC   - Drop typeids.  New column:  "has hottub", True if data is not null in typeid10 or typeid2, False if otherwise.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT poolcnt, COUNT(*) FROM sl_matt
# MAGIC GROUP BY poolcnt
# MAGIC ORDER BY poolcnt ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column propertycountylandusecode has 0.997441392032807 percent non-null values.<br>
# MAGIC Column propertylandusetypeid has 0.997593307287209 percent non-null values.
# MAGIC   - Drop null rows for these two
# MAGIC   
# MAGIC Column propertyzoningdesc has 0.6634526066279269 percent non-null values.
# MAGIC   - Drop; very granular, with many nulls
# MAGIC
# MAGIC Column rawcensustractandblock has 0.997593307287209 percent non-null values.<br>
# MAGIC   - Drop nulls.  However, what does this data mean?
# MAGIC   - "Census tract and block ID combined - also contains blockgroup assignment by extension"
# MAGIC   - Check what this means

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT rawcensustractandblock, COUNT(*) FROM sl_matt
# MAGIC GROUP BY rawcensustractandblock
# MAGIC ORDER BY rawcensustractandblock ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column regionidcity has 0.9790680208507455 percent non-null values.
# MAGIC   - City in which the property is located (if any)
# MAGIC
# MAGIC Column regionidcounty has 0.997593307287209 percent non-null values.
# MAGIC   - County in which the property is located
# MAGIC
# MAGIC Column regionidneighborhood has 0.3874329738843106 percent non-null values.
# MAGIC   - Neighborhood in which the property is located
# MAGIC
# MAGIC Column regionidzip has 0.9955289682458596 percent non-null values.
# MAGIC   - Zip code in which the property is located
# MAGIC   
# MAGIC All of these tell us general location.  City & County & zip code have some really useful information.  Neighborhood is most useful in big cities.
# MAGIC   - Keep Zip Code, city & county will likely be extra since zip codes tend to be city-level or even more specific.
# MAGIC   - There is a little bit of city/town overlap in some cases, but ultimately this is more specific / useful (& better populated than neighborhood)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT regionidneighborhood, COUNT(*) FROM sl_matt
# MAGIC GROUP BY regionidneighborhood
# MAGIC ORDER BY regionidneighborhood ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column roomcnt has 0.9975807453863488 percent non-null values.
# MAGIC   - Drop null rows
# MAGIC   - Calculate into "brackets" - is there really a difference between 4 and 5 rooms?  Probably not.  But 5 and 10 rooms, certainly.
# MAGIC   - This one might also have multicollinearity issues with square footage
# MAGIC   - Brackets:
# MAGIC     1. 0 rooms
# MAGIC     2. 1 - 4 rooms
# MAGIC     3. 5 - 8 rooms
# MAGIC     4. 9+ rooms

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT roomcnt, COUNT(*) FROM sl_matt
# MAGIC GROUP BY roomcnt
# MAGIC ORDER BY roomcnt ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column storytypeid has 0.0005438465612382618 percent non-null values.
# MAGIC   - interestingly, the only category which exists is "basement", with 3247 records.  Compare to "basementsqft", which has 3255 records of not null.
# MAGIC   - Drop
# MAGIC
# MAGIC Column threequarterbathnbr has 0.10518297329808855 percent non-null values.
# MAGIC   - Null -> 0
# MAGIC   - Could change to a flag - has 3/4 bathroom (value = 1, null = 0)
# MAGIC
# MAGIC Column typeconstructiontypeid has 0.002259969710744646 percent non-null values.
# MAGIC   - drop - far too undercounted
# MAGIC
# MAGIC Column unitcnt has 0.6630224871424757 percent non-null values.
# MAGIC   - Nulls -> 0
# MAGIC   - Keep, 1, 2, 3, 4, and new value of 5+

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT storytypeid, COUNT(*) FROM sl_matt
# MAGIC GROUP BY storytypeid
# MAGIC ORDER BY storytypeid ASC;
# MAGIC
# MAGIC SELECT count(basementsqft) FROM sl_matt WHERE basementsqft IS NOT NULL;
# MAGIC
# MAGIC SELECT DISTINCT threequarterbathnbr, COUNT(*) FROM sl_matt
# MAGIC GROUP BY threequarterbathnbr
# MAGIC ORDER BY threequarterbathnbr ASC;
# MAGIC
# MAGIC SELECT DISTINCT unitcnt, COUNT(*) FROM sl_matt
# MAGIC GROUP BY unitcnt
# MAGIC ORDER BY unitcnt ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column yardbuildingsqft17 has 0.027124158813245404 percent non-null values.
# MAGIC   - Patio in yard
# MAGIC   - Create categories, 0, less than 200, 200-300, 300-400, 400+
# MAGIC   
# MAGIC Column yardbuildingsqft26 has 0.0008865352167028394 percent non-null values.
# MAGIC   - storage / shed in yard
# MAGIC   - Yes or No (Null -> 0, Value -> 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC CASE WHEN yardbuildingsqft17 < 200 then "<200"
# MAGIC WHEN yardbuildingsqft17 >= 200 AND yardbuildingsqft17 < 300 THEN "200-300"
# MAGIC WHEN yardbuildingsqft17 >= 300 AND yardbuildingsqft17 < 400 THEN "300-400"
# MAGIC WHEN yardbuildingsqft17 >= 400 THEN ">400"
# MAGIC END patiorange, count(*) as total_within_range
# MAGIC FROM (SELECT DISTINCT parcelid, yardbuildingsqft17 FROM sl_matt WHERE fips IS NOT NULL)
# MAGIC GROUP BY 1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Column yearbuilt has 0.9819508933521416 percent non-null values.
# MAGIC   - Nulls are probably "no structure / year built relevant"
# MAGIC   - Surprising to see some built before 1900!
# MAGIC   - Categorize to "Age_Group" variable listed on the sheet

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT yearbuilt, COUNT(*) FROM sl_matt
# MAGIC GROUP BY yearbuilt
# MAGIC ORDER BY yearbuilt ASC;
# MAGIC
# MAGIC SELECT
# MAGIC CASE WHEN yearbuilt < 1960 then "<1960"
# MAGIC WHEN yearbuilt >= 1960 AND yearbuilt < 2010 THEN "1960-2010"
# MAGIC WHEN yearbuilt >= 2010 THEN ">2010"
# MAGIC END year_range, count(*) as total_within_range
# MAGIC FROM (SELECT yearbuilt FROM sl_matt WHERE yearbuilt IS NOT NULL)
# MAGIC GROUP BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Column numberofstories has 0.22908636122600132 percent non-null values.
# MAGIC   - Possibly drop this; many houses don't have this data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT numberofstories, COUNT(*) FROM sl_matt
# MAGIC GROUP BY numberofstories
# MAGIC ORDER BY numberofstories ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC Column assessmentyear has 0.9975928048111745 percent non-null values.<br>
# MAGIC Column landtaxvaluedollarcnt has 0.9786181373079411 percent non-null values.<br>
# MAGIC Column taxamount has 0.9909550963966773 percent non-null values.<br>
# MAGIC Column taxdelinquencyflag has 0.018922744979678194 percent non-null values.<br>
# MAGIC Column taxdelinquencyyear has 0.01892341494772407 percent non-null values.<br>
# MAGIC   - In terms of modeling, it's likely these will cause multicollinearity issues.
# MAGIC   - It might be interesting to have a field which is "tax %", which would be something like tax / assessment
# MAGIC   - drop all of these for now
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT assessmentyear, COUNT(*) FROM sl_matt
# MAGIC GROUP BY assessmentyear
# MAGIC ORDER BY assessmentyear ASC;
# MAGIC
# MAGIC SELECT DISTINCT landtaxvaluedollarcnt, COUNT(*) FROM sl_matt
# MAGIC GROUP BY landtaxvaluedollarcnt
# MAGIC ORDER BY landtaxvaluedollarcnt ASC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finally, review all data in the GOLD Layer:
# MAGIC
# MAGIC ##### Property Structure Information
# MAGIC
# MAGIC - parcelid:  The ID of the parcel listed on Zillow (PK?)
# MAGIC - basementsqft:  Square Footage of the basement.  This can be turned into "Has_Basement", null -> 0, values -> 1
# MAGIC - bathroomcnt:  Number of bathrooms.  Drop Null Rows
# MAGIC - fullbathcnt:  Number of full bathroms.  Nulls -> 0
# MAGIC - hashottuborspa:  Does the home have a hottub or spa.  Might overlap with "has_hottub"?  Nulls -> 0, true -> 1
# MAGIC - bedroomcnt:  Number of bedrooms.  Drop null rows
# MAGIC - roomcnt:  Number of rooms.  Calculate a new field; none for 0 rooms, small for 1-4 rooms, medium for 5-8 rooms, large for 9+ rooms)
# MAGIC - calculatedfinishedsquarefeet:   Nulls -> 0?
# MAGIC - fireplacecnt:  Number of fireplaces.  Nulls -> 0
# MAGIC - fireplaceflag:  Has a fireplace or not. Nulls -> 0 for false, otherwise 1 for true.
# MAGIC - yearbuilt -> Age Group.  0-10 years old, 10-60 years old, then 60+ years old
# MAGIC - decktypeid:  Calculate "has_deck".  Nulls -> 0, values -> 1
# MAGIC - unitcnt:  Number of units in the property.  nulls -> 0, will use this categorically (with categories of 0, 1, 2, 3, 4, and 5+)
# MAGIC
# MAGIC ##### Yard Information
# MAGIC - lotsizesquarefeet:  Size of the lot in square feet.  Nulls -> 0
# MAGIC - poolcnt:  Number of pools on the property.  Calculate to "pool".  Null -> 0, 1 can stay 1 for true. (only had 1 and null)
# MAGIC - "has_hottub":  Calculated from pooltypeid10 & pooltypeid2; if value -> 1.  Otherwise, 0.
# MAGIC - yardbuildingsqft17:  Patio size.  nulls -> 0, then ordinal categories (<200, 200-300, 300-400, 400+)
# MAGIC - yardbuildingsqft26:  Yard storage.  Nulls -> 0, values -> 1
# MAGIC
# MAGIC ##### Location Information
# MAGIC - fips:  Federal Information Processing Standard Code.  Drop null rows.
# MAGIC - latitude:  Drop null rows
# MAGIC - longitude:  Drop null rows
# MAGIC - censustractandblock:  Drop null rows
# MAGIC - fips_str:  Drop null rows
# MAGIC - state_code:  Drop null rows
# MAGIC - propertycountylandusecode:  Property's zoning at the county level.  Drop null rows.
# MAGIC - propertylandusetypeid:  Drop null rows
# MAGIC - PropertyLandUseDesc:  Drop null rows
# MAGIC - regionidzip:  Drop null rows
# MAGIC
# MAGIC ##### Target:  What are we trying to evaluate?
# MAGIC - taxvaluedollarcnt
# MAGIC   - Target for our ML, we will predict the tax value
# MAGIC   - More ideally, we could predict the difference between tax value & sales value to get a Z-Estimate

# COMMAND ----------

for key in numvals.keys():
  
  print(key)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT taxvaluedollarcnt FROM sl_matt;
# MAGIC
# MAGIC SELECT
# MAGIC CASE WHEN taxvaluedollarcnt IS NULL then "Null"
# MAGIC WHEN taxvaluedollarcnt < 1000 THEN "<1000"
# MAGIC WHEN taxvaluedollarcnt >= 1000 AND taxvaluedollarcnt < 10000 THEN "1000-10000"
# MAGIC WHEN taxvaluedollarcnt >= 10000 AND taxvaluedollarcnt < 100000 THEN "10000-100000"
# MAGIC WHEN taxvaluedollarcnt >= 100000 AND taxvaluedollarcnt < 1000000 THEN "100000-1000000"
# MAGIC WHEN taxvaluedollarcnt >= 1000000 THEN "Over_1000000"
# MAGIC END taxes, count(*) as total_within_range
# MAGIC FROM sl_matt
# MAGIC GROUP BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(parcelid) FROM sl_matt
# MAGIC WHERE bathroomcnt IS NOT NULL AND
# MAGIC bedroomcnt IS NOT NULL AND
# MAGIC fips IS NOT NULL AND
# MAGIC latitude IS NOT NULL AND
# MAGIC longitude IS NOT NULL AND
# MAGIC censustractandblock IS NOT NULL AND
# MAGIC state_code IS NOT NULL AND
# MAGIC propertycountylandusecode IS NOT NULL AND
# MAGIC propertylandusetypeid IS NOT NULL AND
# MAGIC propertylandusedesc IS NOT NULL AND
# MAGIC regionidzip IS NOT NULL AND
# MAGIC taxvaluedollarcnt IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(fireplaceflag) FROM sl_matt WHERE fireplaceflag IS NOT NULL;
# MAGIC
# MAGIC SELECT DISTINCT fireplaceflag, COUNT(*) FROM sl_matt
# MAGIC GROUP BY fireplaceflag
# MAGIC ORDER BY fireplaceflag ASC;