# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Analysis of Airbnb data

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

df_cal_4.printSchema

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
# MAGIC ###### Lets check for duplicates and if any we will remove the duplicate rows

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
# MAGIC ###### Final table for listing has 13312 rows of data

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
# MAGIC ###### Final table for calender has 3902795(3.9M) rows of data

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Checking for null values

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


