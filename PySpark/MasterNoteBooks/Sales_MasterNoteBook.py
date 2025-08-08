# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pytz
from datetime import *

# COMMAND ----------

def load_trans():
    return (spark.read
            .format("csv")
            .option("path", "/Volumes/workspace/default/pyspark_source/SalesDataset/ecommerce_transactions.csv")
            .option("header", True)
            .option("inferSchema", True)
            .load())
    
def load_customer():
    return (spark.read
            .format("csv")
            .option("path", "/Volumes/workspace/default/pyspark_source/SalesDataset/customers.csv")
            .option("header", True)
            .option("inferSchema", True)
            .load()) 

def load_product():
    return (spark.read
            .format("csv")
            .option("path", "/Volumes/workspace/default/pyspark_source/SalesDataset/products.csv")
            .option("header", True)
            .option("inferSchema", True)
            .load())           
