## Date: 30-06-2025
## Array indexing is same as accessing the array elements:
## start with 0 and end 

## For 1D Array

print("Solution:1")
import numpy as np
x = np.array([1,2,3,4,5,6])
print(f"The first index of array {x} is: {x[0]}\n")


## 2D Array (Take is as row and columns)

print("Solution:2")
import numpy as np
x = np.array([[1,2,3,4,5,6],[7,8,9,10,11,12]])
print(f"{x}\n The 2nd element of 1st rows for array is: {x[0,1]}\n")

print("Solution:3")
import numpy as np
x = np.array([[1,2,3,4,5,6],[7,8,9,10,11,12]])
print(f"{x}\n The 4th element of 2nd rows for array is: {x[1,3]}\n")



## 3D Array

print("Solution:4")
import numpy as np
x = np.array([[[1,2,3],[4,5,6],[7,8,9],[10,11,12]]])
print(f"{x}\n The 3rd row, last element for array is: {x[0,2,2]}\n")

