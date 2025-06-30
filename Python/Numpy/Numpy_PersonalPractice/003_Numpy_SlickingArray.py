## Date: 30-06-2025
## slickinng means taking element from point  A to point B 
## [start:end,steps]

# Now we will slice the element from 1 to from from range of 10 values

print("Solution:1")
import numpy as np
x = np.array([1,2,3,4,5,6,7,8,9,10])
print(x[1:5],"\n")

print("Solution:2")
import numpy as np
x = np.array([1,2,3,4,5,6,7,8,9,10])
print(x[4:],"\n")

print("Solution:3")
import numpy as np
x = np.array([1,2,3,4,5,6,7,8,9,10])
print(x[:4],"\n")


print("Solution:4")
import numpy as np
x = np.array([1,2,3,4,5,6,7,8,9,10])
print(x[:-4],"\n")


print("Solution:5")
import numpy as np
x = np.array([1,2,3,4,5,6,7,8,9,10])
print(x[-8:-4],"\n")



## With steps : we can use step value to skip that many values while slcing the array

print("Solution:6")
import numpy as np
x = np.array([1,2,3,4,5,6,7,8,9,10])
print(x[-8:-4:2],"\n")

print("Solution:7")
import numpy as np
x = np.array([1,2,3,4,5,6,7,8,9,10])
print(x[::2],"\n")



## Slicing 2D array : Print 7 8 9
print("Solution:8")
import numpy as np
x = np.array([[1,2,3,4,5],[6,7,8,9,10]])
print(x[1,1:4],"\n")


## Another Example 
print("Solution:9")
import numpy as np
x = np.array([[1,2,3,4,5],[6,7,8,9,10]])
print("First Row",x[0,-4:-1])
print("2nd Row",x[1,[1,3,4]],"\n")