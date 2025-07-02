# Date: 02-07-2025

## By Using where()

print("Solution:1")
import numpy as np
x = np.array([1, 2, 3, 4, 5, 6])
condition = np.where(x % 2 == 0)
print(condition,"\n")


print("Solution:2")
import numpy as np
x = np.array([1, 2, 3, 4, 5, 6])
condition = np.where(x == 4)
print(condition,"\n")



## searchsorted()  --> Perform binary search in array and retrun indexs
print("Solution:3")
import numpy as np
x = np.array([1, 2, 3, 4, 5, 6])
condition = np.searchsorted(x,5,side='right')
print(condition,"\n")



## How to insert the values in array, and it will retrun at which index it can be insertred 
print("Solution:4")
import numpy as np
x = np.array([1, 2, 3, 4, 5, 6])
condition = np.searchsorted(x,[7,2.6,1.1])
print(condition,"\n")


