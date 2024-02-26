# Databricks notebook source
dbutils.fs.ls('mnt/silver')

# COMMAND ----------

product_lookup= spark.read.format('delta').option("header","true").load('/mnt/silver/Product_Lookup/')
customer_lookup=spark.read.format('delta').option("header","true").load('/mnt/silver/Customer_Lookup/')
calender_lookup=spark.read.format('delta').option("header","true").load('/mnt/silver/Calendar_Lookup/')
territory_lookup=spark.read.format('delta').option("header","true").load('/mnt/silver/Territory_Lookup/')
product_category_lookup=spark.read.format('delta').option("header","true").load('/mnt/silver/Product_Category_Lookup/')
product_subcategory_lookup=spark.read.format('delta').option("header","true").load('/mnt/silver/Product_Subcategory_Lookup/')
sales_data=spark.read.format('delta').option("header","true").load('/mnt/silver/Sales_Data/')
returns_data=spark.read.format('delta').option("header","true").load('/mnt/silver/Returns_Data/')

# COMMAND ----------

from pyspark.sql.functions import countDistinct,count
total_orders = sales_data.agg(countDistinct("OrderNumber").alias("totalorders"))
display(total_orders)
total_returns = returns_data.agg(count("ReturnQuantity").alias("totalreturns"))
display(total_returns)


# COMMAND ----------

sales_data.printSchema()

# COMMAND ----------

joined_table_1= sales_data.join(product_lookup,"ProductKey")
specific_columns=joined_table_1.select("OrderQuantity","ProductPrice")
specific_columns.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import sum,col
specific_columns=specific_columns.withColumn("Product of both", col("OrderQuantity")*col("ProductPrice"))
specific_columns.show()
total_revenue=specific_columns.agg(sum(col("Product of both")).alias("totalrevenue"))
display(total_revenue)

# COMMAND ----------

quantity_sold = sales_data.agg(sum("OrderQuantity").alias("quantitysold"))
quantity_sold.show()


# COMMAND ----------

quantity_returned = returns_data.agg(sum("ReturnQuantity").alias("quantityreturned"))
quantity_returned.show()


# COMMAND ----------

joined_table_2= quantity_sold.join(quantity_returned)
joined_table_2.show()

# COMMAND ----------

from pyspark.sql.types import DecimalType
return_rate=joined_table_2.select(col("quantityreturned")/col("quantitysold"))
return_rate=return_rate.withColumnRenamed("(quantityreturned / quantitysold)","returnrate")
return_rate=return_rate.withColumn("returnrate",col("returnrate")*100)
return_rate=return_rate.withColumn("returnrate",col("returnrate").cast(DecimalType(10,2)))
return_rate.show()

# COMMAND ----------

joined_table_3= sales_data.join(product_lookup,"ProductKey")
specific_columns_3=joined_table_3.select("OrderQuantity","ProductCost")
specific_columns_3=specific_columns_3.withColumn("totalcost", col("OrderQuantity")*col("ProductCost"))
specific_columns_3.show(truncate=False)
total_cost=specific_columns_3.agg(sum(col("totalcost")))
total_cost=total_cost.withColumnRenamed("sum(totalcost)","totalcost")
display(total_cost)

# COMMAND ----------

joined_table_4=total_cost.join(total_revenue)
joined_table_4=joined_table_4.withColumn("totalprofit",col("totalrevenue") - col("totalcost"))
total_profit=joined_table_4.select("totalprofit")
total_profit.show()


# COMMAND ----------

total_customers = sales_data.agg(countDistinct("CustomerKey").alias("totalcustomers"))
display(total_customers)

# COMMAND ----------

dbutils.fs.ls('mnt/gold')

# COMMAND ----------

# Define a function to replace invalid characters in column names
def clean_column_names(df):
    new_columns = [col.replace(' ', '_').replace('-', '_').replace('/', '_') for col in df.columns]
    return df.toDF(*new_columns)

# Clean the column names of each DataFrame
product_lookup = clean_column_names(product_lookup)
product_category_lookup= clean_column_names(product_category_lookup)
product_subcategory_lookup = clean_column_names(product_subcategory_lookup)
calender_lookup = clean_column_names(calender_lookup)
customer_lookup = clean_column_names(customer_lookup)
returns_data = clean_column_names(returns_data)
territory_lookup = clean_column_names(territory_lookup)
sales_data = clean_column_names(sales_data)
total_orders = clean_column_names(total_orders)
total_returns = clean_column_names(total_returns)
total_revenue = clean_column_names(total_revenue)
total_customers = clean_column_names(total_customers)
total_cost = clean_column_names(total_cost)
total_profit = clean_column_names(total_profit)
quantity_returned = clean_column_names(quantity_returned)
quantity_sold = clean_column_names(quantity_sold)
return_rate = clean_column_names(return_rate)

# Write the cleaned DataFrames to Delta tables
product_lookup.write.format('delta').save("/mnt/gold/Product_Lookup")
product_category_lookup.write.format('delta').save("/mnt/gold/Product_Category_Lookup")
product_subcategory_lookup.write.format('delta').save("/mnt/gold/Product_Subcategory_Lookup")
calender_lookup.write.format('delta').save("/mnt/gold/Calendar_Lookup")
customer_lookup.write.format('delta').save("/mnt/gold/Customer_Lookup")
returns_data.write.format('delta').save("/mnt/gold/Returns_Data")
territory_lookup.write.format('delta').save("/mnt/gold/Territory_Lookup")
sales_data.write.format('delta').save("/mnt/gold/Sales_Data")
total_orders.write.format('delta').save("/mnt/gold/Total_Orders")
total_returns.write.format('delta').save("/mnt/gold/Total_Returns")


# COMMAND ----------

total_revenue.write.format('delta').save("/mnt/gold/Total_Revenue")
total_customers.write.format('delta').save("/mnt/gold/Total_Customers")
total_cost.write.format('delta').save("/mnt/gold/Total_Cost")
total_profit.write.format('delta').save("/mnt/gold/Total_Profit")
quantity_returned.write.format('delta').save("/mnt/gold/Quantity_Returned")
quantity_sold.write.format('delta').save("/mnt/gold/Quantity_Sold")
return_rate.write.format('delta').save("/mnt/gold/Return_Rate")

# COMMAND ----------


