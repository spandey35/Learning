# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import * 
from pyspark.sql.window import *
from random import *
from datetime import datetime, timedelta


# COMMAND ----------

# Customer Data

df_customer = spark.read.format("delta").load(
  "/Volumes/workspace/default/adventureworks2022/DeltaTables/Ingestion/Customer")

def load_customer ():
    return(
       df_customer.withColumn("ModifiedDate", expr("try_to_timestamp(ModifiedDate, 'MM/dd/yyyy HH:mm:ss')"))
        .select("CustomerID", "StoreID", "TerritoryID", "AccountNumber", "ModifiedDate")
          )

# COMMAND ----------

from pyspark.sql.functions import udf, expr, col
from pyspark.sql.types import StringType, DoubleType
import random

df_product = spark.read.format("delta").load(
    "/Volumes/workspace/default/adventureworks2022/DeltaTables/Ingestion/product"
)

randomcolors = [
    "Cyan", "Magenta", "Teal", "Lavender", 
    "Maroon", "Olive", "Navy", "Turquoise", 
    "Beige", "Coral", "Indigo", "Mint"
]

choice_size = ["XS", "S", "M", "L", "XL", "XXL"]

def InputColor(Color):
    return random.choice(randomcolors)

udf_Inputcolor = udf(InputColor, StringType())

def Standcost(StandardCost):
    return float(random.randint(1000, 2000))

udf_Standcost = udf(Standcost, DoubleType())

def ListPrice(ListPrice):
    return float(random.randint(1000, 2000))

udf_ListPrice = udf(ListPrice, DoubleType())

def Sizes(Size):
    return random.choice(choice_size)

udf_Sizes = udf(Sizes, StringType())

def Weights(Weight):
    return float(random.randint(200, 250))

udf_Weights = udf(Weights, DoubleType())


def random_sell_end_date_str():
    start_date = datetime(2025, 9, 1)
    random_days = random.randint(30, 365)
    end_date = start_date + timedelta(days=random_days)
    return end_date.strftime("%m/%d/%Y %H:%M:%S")

udf_SellEndDateStr = udf(random_sell_end_date_str, StringType())


def load_product():
    return (
        df_product
        .withColumn(
            "ModifiedDate", 
            expr("try_to_timestamp(ModifiedDate, 'MM/dd/yyyy HH:mm:ss')")
        )
        .withColumn("Color", udf_Inputcolor(col("Color")))
        .withColumn("StandardCost", udf_Standcost(col("StandardCost")))
        .withColumn("ListPrice", udf_ListPrice(col("ListPrice")))
        .withColumn("Size", udf_Sizes(col("Size")))
        .withColumn("Weight", udf_Weights(col("Weight")))
        .withColumn("SellEndDate",
                    concat(
                         date_format(add_months(to_timestamp(lit("2025-08-23")),24), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                         lit("+00:00")
                    ))
        .select("ProductID", "Name", "ProductNumber", "Color", "StandardCost", "ListPrice" ,"Size", "Weight", "SellStartDate", "SellEndDate", "ModifiedDate")
    )


# COMMAND ----------

df_sales_order_detail = spark.read.format("delta").load("/Volumes/workspace/default/adventureworks2022/DeltaTables/Ingestion/sales_order_detail")

def load_sales_order_detail():
    return(
        df_sales_order_detail.withColumn("ModifiedDate", expr(
            "try_to_timestamp(ModifiedDate, 'MM/DD/YYYY HH:MM:SS')"
        ))
    )


# COMMAND ----------

df_sales_order_header = spark.read.format("delta").load("/Volumes/workspace/default/adventureworks2022/DeltaTables/Ingestion/sales_order_header/")

def load_sales_order_header():
    return(
        df_sales_order_header.withColumn("ModifiedDate", expr(
            "try_to_timestamp(ModifiedDate, 'MM/DD/YYYY HH:MM:SS')"
        ))
        .na.drop(subset=["CurrencyRateID"])
    )

