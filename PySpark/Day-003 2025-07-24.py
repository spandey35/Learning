# Databricks notebook source
# MAGIC %run ./MasterNoteBooks/MNB_Sales_Emp_Dep

# COMMAND ----------

emp = load_employee()
dep = load_department()
sal = load_salary()


# COMMAND ----------

## Use a UDF to generate a new column only if certain conditions across multiple columns are met (e.g., salary > average AND age < 30). Multi-Row Comparison UDF

joindf = (sal
        .join(dep,"EmployeeID")
        .join(emp,"EmployeeID")
        .select("EmployeeID","AnnualSalary","Age")
        )

average = joindf.agg(avg("AnnualSalary").alias("AverageAnnualSalary"))
datafame= joindf.crossJoin(average)

def CertainConditions(AnnualSalary,AverageAnnualSalary,Age):
        if (AnnualSalary > AverageAnnualSalary) and Age < 30 :
                return "Eligible"
        else:
               return "Not-Eligible"

udf_val = udf(CertainConditions,StringType())

final = (datafame.withColumn("CertainConditions",udf_val("AnnualSalary","AverageAnnualSalary","Age"))
         .where("CertainConditions = 'Eligible'")
         .orderBy("Age"))

display(final)



# COMMAND ----------

# Optimize way
# Join the DataFrames
joindf = (sal
          .join(dep, "EmployeeID")
          .join(emp, "EmployeeID")
          .select("EmployeeID", "AnnualSalary", "Age"))

# Calculate the average salary
average = joindf.agg(avg("AnnualSalary").alias("AverageAnnualSalary")).collect()[0][0]

# Filter the DataFrame based on the conditions
final = (joindf
         .withColumn("CertainConditions", when((col("AnnualSalary") > average) & (col("Age") < 30), "Eligible").otherwise("Not-Eligible"))
         .where(col("CertainConditions") == "Eligible")
         .orderBy("Age"))

display(final)

# COMMAND ----------

## Build a UDF that converts selected columns into a JSON string per row, useful for exporting or logging.
## Regex-Based UDF

## Build a UDF that converts selected columns into a JSON string per row, useful for exporting or logging.
## Regex-Based UDF

selected_columns = ["EmployeeID", "Name", "Gender", "Age", "Department", "AnnualSalary", "Tax"]

join = (emp
        .join(sal, "EmployeeID")
        .join(dep, "EmployeeID")
        .select("EmployeeID", "Name", "Gender", "Age", "Department", "AnnualSalary", "Tax"))

def extract_columns(*cols):
    keys = selected_columns
    return {k: str(v) for k, v in zip(keys, cols)}

extract_udf = udf(extract_columns, MapType(StringType(), StringType()))

with_joined_df = join.withColumn("SelectedData", extract_udf(*[join[col] for col in selected_columns]))

display(with_joined_df.select("EmployeeID", "SelectedData"))






# COMMAND ----------


