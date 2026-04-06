# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pytz

# COMMAND ----------

def ReturnReturnsFact():
    return( spark.read
        .format("csv")
        .option("header", True)
        .load("/Volumes/workspace/default/pyspark_source/Transactions/returns_fact.csv"))
