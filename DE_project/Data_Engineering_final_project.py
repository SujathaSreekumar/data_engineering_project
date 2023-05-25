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



# COMMAND ----------

# MAGIC %md
# MAGIC calendar_3<br>
# MAGIC Data describes the calender details of Airbnb home listings in one quarter (April 2022 - June 2022)
# MAGIC
# MAGIC Volume of data : 640kb

# COMMAND ----------

display(df_cal_3)

# COMMAND ----------

df_cal_3.describe

# COMMAND ----------

df_cal_3.dtypes

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC calendar_4<br>
# MAGIC Data describes the calender details of Airbnb home listings in one quarter (April 2022 - June 2022)
# MAGIC
# MAGIC Volume of data : 640kb

# COMMAND ----------

display(df_cal_4)

# COMMAND ----------

df_cal_4.describe

# COMMAND ----------

df_cal_4.dtypes
