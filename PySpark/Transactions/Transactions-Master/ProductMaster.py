# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import *
import pytz

# COMMAND ----------

def ReturnProductMaster():
    return (
        spark.read
        .format("csv")
        .option("header", True)
        .load("/Volumes/workspace/default/pyspark_source/Transactions/products_master.csv")
    )
