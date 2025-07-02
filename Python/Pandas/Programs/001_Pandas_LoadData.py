# Date: 02-07-2025

import pandas as pd 
import openpyxl

df = pd.read_csv(r'C:\Users\Suraj Pandey\OneDrive\Desktop\Learning\Python\Pandas\Excel Files\pokemon_data.csv')

df1 = pd.read_excel(r'C:\Users\Suraj Pandey\OneDrive\Desktop\Learning\Python\Pandas\Excel Files\pokemon_data.xlsx')

print("\n from CSV Date  Head\n",  df.head(3), "\n,")
print("\n  from CSV Date Tail \n", df.tail(3), "\n,")


print("\n from xlsx Date  Head\n",  df1.head(3), "\n,")
print("\n  from xlsx Date Tail \n", df1.tail(3), "\n,")