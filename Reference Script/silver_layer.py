# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC DATA ACCESS USING APP~

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awstoragedatalaket.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awstoragedatalaket.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awstoragedatalaket.dfs.core.windows.net", "c1c9d6c6-983d-46da-9828-edddb7c04309")
spark.conf.set("fs.azure.account.oauth2.client.secret.awstoragedatalaket.dfs.core.windows.net", "wGd8Q~6o3OVoErqc3giwyXiLtELwf4zEixyhXb.I")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awstoragedatalaket.dfs.core.windows.net", "https://login.microsoftonline.com/76875695-dc0a-41c6-93d0-5ecdff172057/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read calendar Data

# COMMAND ----------

df_cal = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cust = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_pro_cat = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_pro = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_ret = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_ter = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_sub_cat = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@awstoragedatalaket.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC TRANSFORMATION

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn("Month",month(col("Date")))\
                .withColumn("Year",year(col("Date")))\
                .withColumn("Week",weekofyear(col("Date")))\
                .withColumn("Day",dayofmonth(col("Date")))\
                .withColumn("DayofWeek",dayofweek(col("Date")))\
                .withColumn("Quarter",quarter(col("Date"))) 
df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Calendar

# COMMAND ----------

df_cal.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Calendar")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Customers

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust = df_cust.withColumn("fullname",concat(col("prefix"),lit(' '),col("FirstName"),lit(' '),col("LastName")))
df_cust.display()

# COMMAND ----------

df_cust.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Customers")\
    .save()

# COMMAND ----------

df_pro_cat.display()


# COMMAND ----------

df_sub_cat.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_SubCategories")\
    .save()

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn("ProductSKU",split(col('ProductSKU'),'-')[0])\
               .withColumn("ProductName",split(col('ProductName'),'')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Products")\
    .save()

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Returns")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Territories")\
    .save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn('Multiply', col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format("parquet")\
    .mode("append")\
    .option("path","abfss://silver@awstoragedatalaket.dfs.core.windows.net/AdventureWorks_Sales")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Sales analysis 

# COMMAND ----------

#I want to create how many order did we recived 
df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('Total_order')).display()

# COMMAND ----------

df_pro_cat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

