# Databricks notebook source
# MAGIC %run ./MasterNoteBooks/MNB_Sales_Emp_Dep

# COMMAND ----------

## Dynamic Column Selector UDF
## Create a UDF that takes a list of column names and returns a dictionary of their values for each row. Useful for flexible data extraction. Conditional Column Generator

emp = load_employee()
dep = load_department()
sal = load_salary()

join = (emp
        .join(sal, "EmployeeID")
        .join(dep, "EmployeeID")
        .select("EmployeeID","Name","Gender","City","Department","Location","Manager","AnnualSalary","AnnualBonus","Tax","NetSalary")
        )

      




# COMMAND ----------

from pyspark.sql.functions import udf, struct
from pyspark.sql.types import MapType, StringType

# List of columns you want to extract dynamically
selected_columns = ["AnnualSalary", "AnnualBonus", "Tax", "Location"]

# Define the UDF
def extract_columns(*cols):
    keys = selected_columns
    return {k: str(v) for k, v in zip(keys, cols)}

# Register the UDF
extract_udf = udf(extract_columns, MapType(StringType(), StringType()))

# Apply the UDF using struct to pass multiple columns
join_with_dict = join.withColumn(
    "SelectedData",
    extract_udf(*[join[col] for col in selected_columns])
)

display(join_with_dict.select("EmployeeID", "SelectedData"))


# COMMAND ----------


