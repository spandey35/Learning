# Databricks notebook source
# MAGIC %run ./MasterNoteBooks/Promo_mnb

# COMMAND ----------

promotion = load_Promotion()
promotioninvestment = load_PromotionInvestment()
promotionstatus = load_PromotionStatus()
userdetail = load_UsersDetail()
promotionbudgetallocation = load_PromotionBudgetAllocation()
promotioninvestmentdetail = load_PromotionInvestmentdetails()
investmenttype = load_InvestmentType()

# COMMAND ----------

#User Impact Analysis --> For each user, calculate:

# 4. Number of promotions that reached "Approved".
#  Skills: joins, aggregations, filtering, datetime operations.


# 1. Total number of promotions created.
ans_1 = (promotion.join(userdetail,promotion.CreatedBy == userdetail.UserId)
        .groupBy("CreatedBy").count())

display(ans_1)       

# 2. Total investment handled.
ans_2 = (promotioninvestment
         .join(userdetail,promotioninvestment.CreatedBy == userdetail.UserId)
         .groupBy("Email").agg(round(sum("InvestmentAmount"),2).alias("Total_Sum_InvestmentAmount"))
         .orderBy(desc("Total_Sum_InvestmentAmount"))
         .select("Email","Total_Sum_InvestmentAmount")
         )

display(ans_2) 


# 3. Average time between creation and modification.

ava_creation = (promotion
                .groupBy("CreatedDate","ModifiedDate").agg(avg("CreatedDate").)
                )

