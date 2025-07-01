# Date: 01-07-2025

## Shape of the Array is the number of elements in each daimensions


print("Solution:1")
import numpy as np
x = np.array([[1, 2, 3, 4],[5, 6, 7, 8]])
print(f"The Shape of array \n{x} is: {np.shape(x)}\n")



print("Solution:2")
import numpy as np
x = np.array( [[1, 2, 3, 4], [5, 6, 7, 8]], ndmin=5 )
print(f"The Shape of array \n{x} is: {np.shape(x)}\n")


