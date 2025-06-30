# Date: 30-06-2025
# Creating numpy nd Array
# array object in numpy called as ndarray
#array() , ndim () to check the daimension of each array

print("Solution 1:")
import numpy as np
x = np.array([1,2,3,5,6])

print(f"The 1-D Array is:{x} and type is {type(x)} and dimension is:  {np.ndim(x)}\n")


### We can also pass as list , tuple or any array like object with array mothod i.e. array()
## And it will be converted to Ndarray

print("Solution 2:")
import numpy as np 
y = np.array((1,2,3,4,5,6))

print(f"The 1-D Array is:{y} and type is {type(y)} and dimension is:  {np.ndim(x)}\n")



## Dimension in array
## A dimension in array is one level of array depth (nested array)
## 0-d Arrays - scalars are the elements in array, each value in array is 0-D array
## Now we will Create 0_d Array with the value 42


print("Solution 3:")
import numpy as np 
x = np.array(42)
print(f"The 0-D Array is {x} and type is {type(x)} and dimension is:  {np.ndim(x)}\n")


## 1-D arrays  - an array that has 0-d arrays to its element are called 1-D array. 

print("Solution 4:")
import numpy as np 
x = np.array((42,21,45,67))
print(f"The 1-D Array is {x} and type is {type(x)} and dimension is:  {np.ndim(x)}\n")


## 2-D arrays - An array which cantain 2 array. 

print("Solution 5:")
import numpy as np 
x = np.array([(42,21,45,67),(42,21,45,67)])
print(f"The 2-D Array is:\n {x} \n and type is {type(x)} and dimension is:  {np.ndim(x)}\n")


## 3-D arrays - An array which cantain 3 array. 

print("Solution 6:")
import numpy as np 
x = np.array([[[1,2],[3,4],[5,6]]])
print(f"The 3-D Array is:\n {x} \n and type is {type(x)} and dimension is:  {np.ndim(x)}\n")



## Create an array with 5 daimension and verify if it have 5 daimension (we can use mdim=5) which will allow to change the daimensions of array. 

print("Solution 7:")
import numpy as np
x = np.array([1,2,3,4,5], ndmin=5 )
print (f"The 5-D Array is:\n {x} \n and type is {type(x)} and dimension is:  {np.ndim(x)}\n")




