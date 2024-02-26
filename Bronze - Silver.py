# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@jcsdatalakegen2.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://silver@jcsdatalakegen2.dfs.core.windows.net/",
  mount_point = "/mnt/silver",
  extra_configs = configs)

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://gold@jcsdatalakegen2.dfs.core.windows.net/",
  mount_point = "/mnt/gold",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver")

# COMMAND ----------

product_lookup = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Product Lookup.csv')
product_category_lookup = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Product Categories Lookup.csv')
product_subcategory_lookup = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Product Subcategories Lookup.csv')
customer_lookup = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Customer Lookup.csv')
territory_lookup = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Territory Lookup.csv')
calendar_lookup = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Calendar Lookup.csv')
salesdata_2020 = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Sales Data 2020.csv')
salesdata_2021 = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Sales Data 2021.csv')
salesdata_2022 = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Sales Data 2022.csv')
return_data = spark.read.format('csv').option("header","true").load('/mnt/bronze/dbo_AdventureWorks Returns Data.csv')


# COMMAND ----------

display(product_lookup)
product_lookup.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
product_lookup = product_lookup.withColumn("ProductKey", col("ProductKey").cast(IntegerType()))\
    .withColumn("ProductSubcategoryKey",col("ProductSubcategoryKey").cast(IntegerType()))

# COMMAND ----------

display(product_lookup)
product_lookup.printSchema()

# COMMAND ----------

display(product_category_lookup)
product_category_lookup.printSchema()

# COMMAND ----------

product_category_lookup = product_category_lookup.withColumn("ProductCategoryKey", col("ProductCategoryKey").cast(IntegerType()))
product_category_lookup = product_category_lookup.drop("ProductCatgoryKey")
display(product_category_lookup)
product_category_lookup.printSchema() 


# COMMAND ----------

display(customer_lookup)
customer_lookup.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType

customer_lookup = customer_lookup.withColumn("CustomerKey", customer_lookup["CustomerKey"].cast(IntegerType()))
customer_lookup = customer_lookup.withColumn("BirthDate",
 to_date(customer_lookup["Birthdate"], "yyyy-MM-dd"))
customer_lookup.printSchema()

# COMMAND ----------

calendar_lookup = calendar_lookup.withColumn("Date",
 to_date(calendar_lookup["Date"], "yyyy-MM-dd"))
calendar_lookup.printSchema()

# COMMAND ----------

return_data = return_data.withColumn("ReturnDate",
 to_date(return_data["ReturnDate"], "yyyy-MM-dd"))\
     .withColumn("TerritoryKey",col("TerritoryKey").cast(IntegerType()))\
         .withColumn("ProductKey",col("ProductKey").cast(IntegerType()))\
             .withColumn("ReturnQuantity",col("ReturnQuantity").cast(IntegerType()))
return_data=return_data.drop("ReturnData")
return_data.printSchema()


# COMMAND ----------

salesdata_2020 = salesdata_2020.withColumn("OrderDate",
 to_date(salesdata_2020["OrderDate"], "yyyy-MM-dd"))\
    .withColumn("StockDate",to_date(salesdata_2020["StockDate"], "yyyy-MM-dd"))\
     .withColumn("OrderNumber",col("OrderNumber").cast(IntegerType()))\
         .withColumn("ProductKey",col("ProductKey").cast(IntegerType()))\
             .withColumn("OrderQuantity",col("OrderQuantity").cast(IntegerType()))\
                 .withColumn("TerritoryKey",col("TerritoryKey").cast(IntegerType()))\
                     .withColumn("OrderLineItem",col("OrderLineItem").cast(IntegerType()))\
                         .withColumn("CustomerKey",col("CustomerKey").cast(IntegerType()))
             
             
salesdata_2020.printSchema()

# COMMAND ----------

salesdata_2021 = salesdata_2021.withColumn("OrderDate",
 to_date(salesdata_2021["OrderDate"], "yyyy-MM-dd"))\
    .withColumn("StockDate",to_date(salesdata_2021["StockDate"], "yyyy-MM-dd"))\
     .withColumn("OrderNumber",col("OrderNumber").cast(IntegerType()))\
         .withColumn("ProductKey",col("ProductKey").cast(IntegerType()))\
             .withColumn("OrderQuantity",col("OrderQuantity").cast(IntegerType()))\
                 .withColumn("TerritoryKey",col("TerritoryKey").cast(IntegerType()))\
                     .withColumn("OrderLineItem",col("OrderLineItem").cast(IntegerType()))\
                         .withColumn("CustomerKey",col("CustomerKey").cast(IntegerType()))
             
             
salesdata_2021.printSchema()

# COMMAND ----------

salesdata_2022 = salesdata_2022.withColumn("OrderDate",
 to_date(salesdata_2022["OrderDate"], "yyyy-MM-dd"))\
    .withColumn("StockDate",to_date(salesdata_2022["StockDate"], "yyyy-MM-dd"))\
     .withColumn("OrderNumber",col("OrderNumber").cast(IntegerType()))\
         .withColumn("ProductKey",col("ProductKey").cast(IntegerType()))\
             .withColumn("OrderQuantity",col("OrderQuantity").cast(IntegerType()))\
                 .withColumn("TerritoryKey",col("TerritoryKey").cast(IntegerType()))\
                     .withColumn("OrderLineItem",col("OrderLineItem").cast(IntegerType()))\
                         .withColumn("CustomerKey",col("CustomerKey").cast(IntegerType()))
             
             
salesdata_2022.printSchema()

# COMMAND ----------

territory_lookup= territory_lookup.withColumn("SalesTerritoryKey",col("SalesTerritoryKey").cast(IntegerType()))
territory_lookup.printSchema()

# COMMAND ----------

from pyspark.sql.functions import concat_ws
customer_lookup=customer_lookup.na.drop(how="any")
df = customer_lookup.count()
print(df)
customer_lookup=customer_lookup.withColumn("Full Name",concat_ws(" ",customer_lookup.Prefix,customer_lookup.FirstName,customer_lookup.LastName))
customer_lookup.printSchema()
display(customer_lookup)

# COMMAND ----------



# COMMAND ----------

# CALENDAR LOOKUP

from pyspark.sql.functions import dayofweek, month, date_format, to_date, date_trunc, trunc, year

calendar_lookup = calendar_lookup.withColumn(
    "Day Name",
    date_format(calendar_lookup.Date, "EEEE")
)

calendar_lookup = calendar_lookup.withColumn(
    "Month Name",
    date_format(calendar_lookup.Date, "MMMM")
)

# Add column with start of the week
calendar_lookup = calendar_lookup.withColumn(
    "Start of Week",
    date_trunc('week', calendar_lookup.Date)  # Truncate to the start of the week
)

# Add column with start of the month
calendar_lookup = calendar_lookup.withColumn(
    "Start of Month",
    date_trunc('month', calendar_lookup.Date)  # Truncate to the start of the month
)

# Add column with start of the quarter
calendar_lookup = calendar_lookup.withColumn(
    "Start of Quarter",
    date_trunc('quarter', calendar_lookup.Date)  # Truncate to the start of the quarter
)

# Add column with start of the year
calendar_lookup = calendar_lookup.withColumn(
    "Start of Year",
    year(calendar_lookup.Date),  # Truncate to the start of the year
)

calendar_lookup = calendar_lookup.withColumn(
    "Start of Week",
    to_date(calendar_lookup["Start of Week"]))\
    .withColumn(
    "Start of Month",
    to_date(calendar_lookup["Start of Month"]))\
        .withColumn(
    "Start of Quarter",
    to_date(calendar_lookup["Start of Quarter"]))  

display(calendar_lookup)
calendar_lookup.printSchema()

# COMMAND ----------

salesdata_2022.printSchema()

# COMMAND ----------

from pyspark.sql.types import LongType
customer_lookup=customer_lookup.withColumn("AnnualIncome",col("AnnualIncome").cast(LongType()))
customer_lookup.printSchema()

# COMMAND ----------

from pyspark.sql.types import DecimalType
product_lookup=product_lookup.withColumn("ProductCost",col("ProductCost").cast(DecimalType(10,2)))\
    .withColumn("ProductPrice",col("ProductPrice").cast(DecimalType(10,2)))
product_lookup.printSchema()

# COMMAND ----------

import pyspark.sql.functions as F
Sales_Data = salesdata_2020.unionAll(salesdata_2021).unionAll(salesdata_2022)
Sales_Data.printSchema()


# COMMAND ----------

product_lookup.printSchema()

# COMMAND ----------

product_lookup.write.option("header",'true').csv("/mnt/silver/Product_Lookup")
product_category_lookup.write.option("header",'true').csv("/mnt/silver/Product_Category_Lookup")
product_subcategory_lookup.write.option("header",'true').csv("/mnt/silver/Product_Subcategory_Lookup")
calendar_lookup.write.option("header",'true').csv("/mnt/silver/Calendar_Lookup")
customer_lookup.write.option("header",'true').csv("/mnt/silver/Customer_Lookup")
return_data.write.option("header",'true').csv("/mnt/silver/Returns_Data")
territory_lookup.write.option("header",'true').csv("/mnt/silver/Territory_Lookup")
Sales_Data.write.option("header",'true').csv("/mnt/silver/Sales_Data")

# COMMAND ----------


