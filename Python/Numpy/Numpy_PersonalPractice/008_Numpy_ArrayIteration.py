# Date: 01-07-2025

## Intration array means giving to the elements one by one using loops. 

# Iterate the element of 1D

print("Solution:1")
import numpy as np
x = np.random.randint(1,9,2)
print(x,"\n")

## 2D aaray 
print("Solution:2")
import numpy as np 
x= np.array([[1,2,3],[4,5,6]])
for i in x:
    print(i,"\n")


## Intrate on each element Scalar elements
print("Solution:3")
import numpy as np 
x= np.array([[1,2,3],[4,5,6]])
for i in x:
    for j in i :
        print (j,"\n")
print("\n")




## 3d#
print("Solution:3")
import numpy as np 
x= np.array([[1,2,3],[4,5,6],[7,8,9]])
for i in x:
    for j in i :
        print (j)
print("\n")




## Using nditer()
print("Solution:5")
import numpy as np
x = np.array([[[1,2],[3,4],[5,6],[7,8]]])
for i in np.nditer(x[:,::2]):
    print(i)
print("\n")
 

