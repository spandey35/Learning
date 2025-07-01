# Date: 01-07-2025

## Diff between Numpy array copy and view
## We will make a copy first , change in orginal array and display both

print("Solution:1")
import numpy as np
x = np.array([1, 2, 3, 4, 5])
x1 = x.copy()
x1[0] = 11
print(f'Aarry is {x}')
print(f'Aarry is {x1}\n')



## Now we will make it as view and dsiplay both

print("Solution:2")
import numpy as np
x = np.array([1, 2, 3, 4, 5])
x1 = x.view()
x[0]= 11
print(f'Aarry is {x}')
print(f'Aarry is {x1}\n')


