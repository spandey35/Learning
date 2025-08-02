# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *


# COMMAND ----------

def load_Promotion():
    return (spark.read
            .format("csv")
            .option("path","/Volumes/workspace/default/pyspark_source/Promo/Promotion.csv")
            .option("inferSchema",True)
            .option("header",True)
            .load())

def load_PromotionInvestment():
    return (spark.read
            .format("csv")
            .option("path","/Volumes/workspace/default/pyspark_source/Promo/PromotionInvestment.csv")
            .option("inferSchema",True)
            .option("header",True)
            .load()
            )
    
def load_InvestmentType():
    return (spark.read
            .format("csv")
            .option("path","/Volumes/workspace/default/pyspark_source/Promo/InvestmentType.csv")
            .option("inferSchema",True)
            .option("header",True)
            .load())

def load_PromotionBudgetAllocation():
    return (spark.read
            .format("csv")
            .option("path","/Volumes/workspace/default/pyspark_source/Promo/PromotionBudgetAllocation.csv")
            .option("inferSchema",True)
            .option("header",True)
            .load())     


def load_PromotionStatus():
    return (spark.read
            .format("csv")
            .option("path","/Volumes/workspace/default/pyspark_source/Promo/Promotionstatus.csv")
            .option("inferSchema",True)
            .option("header",True)
            .load())  


def load_PromotionInvestmentdetails():
    return (spark.read
            .format("csv")
            .option("path","/Volumes/workspace/default/pyspark_source/Promo/PromotionInvestmentdetails.csv")
            .option("inferSchema",True)
            .option("header",True)
            .load())  

def load_UsersDetail():
    return (spark.read
            .format("csv")
            .option("path","/Volumes/workspace/default/pyspark_source/Promo/usersdetail.csv")
            .option("inferSchema",True)
            .option("header",True)
            .load())            

        
    
