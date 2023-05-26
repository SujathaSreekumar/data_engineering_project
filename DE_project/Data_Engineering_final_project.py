# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Analysis of Airbnb data - Singapore

# COMMAND ----------

# MAGIC %md
# MAGIC Data Engineering Final Project
# MAGIC
# MAGIC Name : Sujatha S<br>
# MAGIC Roll : AA.SC.P2MCA2107441<br>
# MAGIC Sem  : 3

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook is for data analysis of Airbnb data.
# MAGIC
# MAGIC Data is taken from http://insideairbnb.com/get-the-data<br><br>
# MAGIC The country chosen for the Airbnb data is Singapore

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Importing the necessary libraries

# COMMAND ----------

import pyspark
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Extraction

# COMMAND ----------

df_cal_1 = spark.sql("SELECT * FROM calendar_1")
df_cal_2 = spark.sql("SELECT * FROM calendar_2")
df_cal_3 = spark.sql("SELECT * FROM calendar_3")
df_cal_4 = spark.sql("SELECT * FROM calendar_4")

df_list_1 = spark.sql("SELECT * FROM listings_1")
df_list_2 = spark.sql("SELECT * FROM listings_2")
df_list_3 = spark.sql("SELECT * FROM listings_3")
df_list_4 = spark.sql("SELECT * FROM listings_4")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Dictionary

# COMMAND ----------

# MAGIC %md
# MAGIC - #### listing_1<br>
# MAGIC Data describes the listing details of Airbnb home listings in one quarter (April 2022 - June 2022)
# MAGIC
# MAGIC    Volume of data : 640kb

# COMMAND ----------

display(df_list_1)

# COMMAND ----------

df_list_1.describe

# COMMAND ----------

df_list_1.dtypes

# COMMAND ----------

df_list_1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - ####listing_2<br>
# MAGIC Data describes the listing details of Airbnb home listings in one quarter (July 2022 - September 2022)
# MAGIC
# MAGIC    Volume of data : 664kb

# COMMAND ----------

display(df_list_2)

# COMMAND ----------

df_list_2.describe

# COMMAND ----------

df_list_2.dtypes

# COMMAND ----------

df_list_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - #### listing_3<br>
# MAGIC Data describes the listing details of Airbnb home listings in one quarter (October 2022 - December 2022)
# MAGIC
# MAGIC    Volume of data : 474kb

# COMMAND ----------

display(df_list_3)

# COMMAND ----------

df_list_3.describe

# COMMAND ----------

df_list_3.dtypes

# COMMAND ----------

df_list_3.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - #### listing_4<br>
# MAGIC Data describes the listing details of Airbnb home listings in one quarter (January 2023 - March 2023)
# MAGIC
# MAGIC    Volume of data : 513kb

# COMMAND ----------

display(df_list_4)

# COMMAND ----------

df_list_4.describe

# COMMAND ----------

df_list_4.dtypes

# COMMAND ----------

df_list_4.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - #### calendar_1<br>
# MAGIC Data describes the calender details of Airbnb home listings in one quarter (April 2022 - June 2022)
# MAGIC
# MAGIC    Volume of data : 65.2MB

# COMMAND ----------

display(df_cal_1)

# COMMAND ----------

df_cal_1.describe

# COMMAND ----------

df_cal_1.dtypes

# COMMAND ----------

df_cal_1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - #### calendar_2<br>
# MAGIC Data describes the calender details of Airbnb home listings in one quarter (July 2022 - September 2022)
# MAGIC
# MAGIC    Volume of data : 67.3MB

# COMMAND ----------

display(df_cal_2)

# COMMAND ----------

df_cal_2.describe

# COMMAND ----------

df_cal_2.dtypes

# COMMAND ----------

df_cal_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - #### calendar_3<br>
# MAGIC Data describes the calender details of Airbnb home listings in one quarter (October 2022 - December 2022)
# MAGIC
# MAGIC    Volume of data : 49.5MB

# COMMAND ----------

display(df_cal_3)

# COMMAND ----------

df_cal_3.describe

# COMMAND ----------

df_cal_3.dtypes

# COMMAND ----------

df_cal_3.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - #### calendar_4<br>
# MAGIC Data describes the calender details of Airbnb home listings in one quarter (January 2023 - March 2023)
# MAGIC
# MAGIC    Volume of data : 53.5MB

# COMMAND ----------

display(df_cal_4)

# COMMAND ----------

df_cal_4.describe

# COMMAND ----------

df_cal_4.dtypes

# COMMAND ----------

df_cal_4.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Combining the four tables containing listing details into one dataframe

# COMMAND ----------

df_listing_full = df_list_1.union(df_list_2)

# COMMAND ----------

df_listing_full = df_listing_full.union(df_list_3)

# COMMAND ----------

df_listing_full = df_listing_full.union(df_list_4)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Here we can see 14586 rows of data after combining all four tables of data

# COMMAND ----------

df_listing_full.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Lets check for duplicates and if any, we will remove the duplicate rows

# COMMAND ----------

df_listing_full.distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Removing duplicate rows

# COMMAND ----------

df_listing_full = df_listing_full.dropDuplicates()

# COMMAND ----------

df_listing_full.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Table for listing has 13312 rows of data

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Combining the four tables containing calender details into one dataframe

# COMMAND ----------

df_calender_full = df_cal_1.union(df_cal_2)

# COMMAND ----------

df_calender_full = df_calender_full.union(df_cal_3)

# COMMAND ----------

df_calender_full = df_calender_full.union(df_cal_4)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Here we can see 5305261(5.3M) rows of data after combining all four tables of data

# COMMAND ----------

df_calender_full.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Checking for duplicates 

# COMMAND ----------

df_calender_full.distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Removing duplicate rows

# COMMAND ----------

df_calender_full = df_calender_full.dropDuplicates()

# COMMAND ----------

df_calender_full.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Table for calender has 3902795(3.9M) rows of data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking for null values

# COMMAND ----------

df = df_listing_full.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull(), c 
                           )).alias(c)
                    for c in df_listing_full.columns])
df.show()

# COMMAND ----------

df2 = df_calender_full.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull(), c 
                           )).alias(c)
                    for c in df_calender_full.columns])
df2.show()

# COMMAND ----------

df_listing_full.write.mode('overwrite').saveAsTable('sg_listing')
df_calender_full.write.mode('overwrite').saveAsTable('sg_calender')

# COMMAND ----------

# MAGIC %md
# MAGIC - ##### Listing table 
# MAGIC ###### Removing null values 
# MAGIC ###### Filtering out extreme values for a clean data

# COMMAND ----------

df_listing_full = df_listing_full.dropna(subset=['id','neighbourhood_group','neighbourhood','room_type','price'])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name,host_name,price,neighbourhood_group,neighbourhood,room_type,minimum_nights 
# MAGIC FROM default.sg_listing
# MAGIC where price in (
# MAGIC SELECT max(price) AS max_price
# MAGIC FROM default.sg_listing)

# COMMAND ----------

df_listing_full = df_listing_full.filter(col('price').between(10,10000))

# COMMAND ----------

df_listing_full.count()

# COMMAND ----------

df = df_listing_full.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull(), c 
                           )).alias(c)
                    for c in df_listing_full.columns])
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM default.sg_listing
# MAGIC where price in (100,200,300,400,500)

# COMMAND ----------

# MAGIC %md
# MAGIC - ##### Calender table 
# MAGIC ###### Removing null values 
# MAGIC ###### Filtering out extreme values for a clean data

# COMMAND ----------

df_calender_full = df_calender_full.dropna(subset=['price','adjusted_price'])

# COMMAND ----------

df2 = df_calender_full.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull(), c 
                           )).alias(c)
                    for c in df_calender_full.columns])
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exporting the clean data for analysis

# COMMAND ----------

df_listing_full.write.mode('overwrite').saveAsTable('sg_listing')
df_calender_full.write.mode('overwrite').saveAsTable('sg_calender')

# COMMAND ----------

# MAGIC %md 
# MAGIC - ##### What are the various types of rooms available? Which room type is popular?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT room_type,COUNT(room_type) AS total_count
# MAGIC FROM default.sg_listing
# MAGIC GROUP BY room_type
# MAGIC ORDER BY total_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Entire home/apartments is the popular choice of accommodation with 6942 listings and is closely followed by Private Rooms with 5396 listings

# COMMAND ----------

# MAGIC %md
# MAGIC - ##### Which neighbourhood is the preferred choice of accomodation? Why is it so?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT neighbourhood_group, COUNT(id) as total_count
# MAGIC FROM default.sg_listing
# MAGIC GROUP BY neighbourhood_group
# MAGIC ORDER BY total_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Central Region emerges the clear winner here with a total count of 9372 listings in the area.<br>
# MAGIC This could be due to the proximity to the city centre.<br>
# MAGIC Moreover in Singapore, Airbnb doesn't cater to normal tourists as it is illegal, but they are legal to be rent out with a minimum rental period of 3 months<br>
# MAGIC Since Central Region is the preferred area of choice, it most likely seems to be catering to business employees who are here for short-term work commitments.
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT neighbourhood, COUNT(id) as total_count
# MAGIC FROM default.sg_listing
# MAGIC WHERE neighbourhood_group = 'Central Region'
# MAGIC GROUP BY neighbourhood
# MAGIC ORDER BY total_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Top three areas(Kallang, Downtown Core, Outram) together comprises of 1/3 of the listings<br>
# MAGIC Top five areas(Kallang, Downtown Core, Outram,Rochor,Queenstown) together comprises 1/2 of the listings

# COMMAND ----------

# MAGIC %md
# MAGIC - ##### Find out any listing of 'Rooftop' and 'near MRT' which are available

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT cal.listing_id,cal.available,lis.name, lis.neighbourhood_group,lis.neighbourhood,cal.price
# MAGIC FROM default.sg_calender cal
# MAGIC JOIN default.sg_listing lis
# MAGIC ON cal.listing_id = lis.id
# MAGIC WHERE lis.name LIKE '%Rooftop% near MRT%' and cal.available = 't';

# COMMAND ----------

# MAGIC %md
# MAGIC We can see one listing 'Rooftop deck serviced studio in CBD near MRT' in the Central Region.<br>The price varies from $200 - $203  

# COMMAND ----------

# MAGIC %md
# MAGIC - ##### Find out any listing of 'pool' and 'near MRT' which are available.<br>List the dates of availability

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT cal.listing_id,cal.`date`,cal.available,lis.name, lis.neighbourhood_group,lis.neighbourhood,cal.price
# MAGIC FROM default.sg_calender cal
# MAGIC JOIN default.sg_listing lis
# MAGIC ON cal.listing_id = lis.id
# MAGIC WHERE lis.name LIKE '%pool%near MRT%' and cal.available = 't';
