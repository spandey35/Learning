# Date: 01-07-2025

## Reshaping means changing the shape of the array like adding or removing.


print("Solution:1")
import numpy as np
x= np.array([1, 2, 3, 4, 5, 6])
print(f"The Rshaped array {x} \n {x.reshape(2,3)}\n")


print("Solution:2")
import numpy as np
x= np.array([1, 2, 3, 4, 5, 6])
print(f"The Rshaped array {x} \n {x.reshape(2,3).base}\n")



## Uknow dimensions --> We are allowed to have one unknow dimension and we can pass it as -1. 

print("Solution:3")
import numpy as np
x= np.array([1, 2, 3, 4, 5, 6, 7, 8])
print(f"The Rshaped array {x} \n {x.reshape(2,2,-1)}\n")


## Flattening the array by the multidimensional into 1D array


print("Solution:4")
import numpy as np
x= np.array([[1, 2, 3], [4, 5, 6]])
x1 = x.reshape(-1)
print(f"The Rshaped array :\n {x}   {x1}\n")



## There are alot of functions for changing the shape of the array in nump.
## flatten , reval and rearranging the element rot90 , flip , fliplr, filped.
## They are also actualy comes under advanced numpy 



 

 