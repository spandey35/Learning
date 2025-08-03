# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.ml.feature import *


# COMMAND ----------

def load_INC():
    return (spark.read
            .format("csv")
            .option("path","/Volumes/workspace/default/pyspark_source/BRReport/incidentdata-final.csv")
            .option("header",True)
            .option("inferSchema",True)
            .load()
            )
    

