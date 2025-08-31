# Databricks notebook source
# 1. Write a UDF that categorizes products (Production.Product.ListPrice) into “Low”, “Medium”, “High” price ranges. Apply it to create a new column.

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

df = spark.read.table("workspace.production.product")

def princerange(price):
    if price <= 100:
        return "Low"
    
    elif (price >=100 and price<= 500):
        return "“Medium”"
    
    else:
        return "High"

price_udf = udf(princerange, StringType())

broz = (df.withColumn("PrinceRange", price_udf(df["Listprice"])))
gold = broz.select("ProductID", "Name", "Listprice", "PrinceRange")

display(gold)


# COMMAND ----------

# UDF (User Defined Function) Practice
# Name Formatter UDF
# Create a UDF that formats customer names as "LastName, FirstName" and apply it to the Person.Person table.

from pyspark.sql.functions import * 
from pyspark.sql.window import *
from pyspark.sql.types import *

raw = spark.read.table("workspace.Person.Person")

def NameFormatter(firstname, lastname):
    return f"{lastname}, {firstname}"

NameFormatter_func = udf(NameFormatter, StringType())

silver = raw.withColumn("Formatted Name", NameFormatter_func ( raw["FirstName"] , raw["LastName"] ) )
gold = silver.select("FirstName", "LastName", "Formatted Name")

display(gold)

# COMMAND ----------

# 3. Email Domain Extractor
# Write a UDF to extract the domain from email addresses in the EmailAddress column of the Person.EmailAddress table.

from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

bronze = spark.read.table("workspace.Person.Email_Address")

def DomainExtractor(EmailAddress):
    if EmailAddress and "@" in EmailAddress:
        return  EmailAddress.split("@")[1]

extract_domain_udf = udf(DomainExtractor, StringType())

bronze_with_domain = bronze.withColumn("Domain", extract_domain_udf(col("EmailAddress")))


display(bronze_with_domain.limit(5))


# COMMAND ----------

from pyspark.sql.functions import (
    sum,
    rank,
    col,
    desc
)
from pyspark.sql.window import Window

bronze = spark.read.table("workspace.sales.sales_order_header")


silver = (
    bronze
    .groupBy("SalesPersonID")
    .agg(
        sum("SubTotal").alias("Total_sales")
    )
)


window_val = Window.orderBy(desc("Total_sales"))


silver = silver.withColumn(
    "Rank",
    rank().over(window_val)
)

gold = silver.select(
    "SalesPersonID",
    "Total_sales",
    "Rank"
)

display(gold)

# COMMAND ----------

# Create a function that calculates profit margin for each product using ListPrice and StandardCost from Production.Product.

from pyspark.sql.functions import(sum,rank,col,desc)
from pyspark.sql.window import Window

def ProfitMargin(ListPrice, StandardCost):
    profit = (StandardCost - ListPrice) / 100
    return profit

bronze = spark.read.table("workspace.production.product")


udf_val = udf(ProfitMargin, IntegerType())

silver = (bronze
          .withColumn("Profit", udf_val(col("ListPrice"), col("StandardCost"))))

gold = (silver.select("ProductID", "Name", "ListPrice", "StandardCost", "Profit"))
display(gold)


# COMMAND ----------

# Build a function that segments customers based on their total purchase amount from Sales.Customer and Sales.SalesOrderHeader.

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

bronze1 = spark.read.table("workspace.sales.customer")
bronze2 = spark.read.table("workspace.sales.Sales_Order_Header")

silver = (bronze1
          .join(bronze2, "CustomerID")
          .select("CustomerID", bronze1.AccountNumber.alias("AccountNumber"), "SubTotal", "OrderDate"))

display(silver)


