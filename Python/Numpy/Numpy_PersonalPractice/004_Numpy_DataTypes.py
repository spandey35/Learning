# Date: 30-06-2025
# Data Type in python (normal python--> String, Integer, Float, boolen and Complex)
# Data type in numpy (Interger -->i, Boolen ---> b, unsigned interger--> u, float --> f, complex float -->c, timedalta-->m,
#                     datetime --> M , object---> o , String-->s , Unicode string--> U, memory-->V )


# Checking the datatype of any array -- dtype()

print("Solution:1")
import numpy as np
x = np.array([1,2,3,4])
print(f"The Data type of {x} is: {x.dtype}\n")



print("Solution:2")
import numpy as np 
x =np.array(['apple','banana','cherry'])
print(f"The Data type of {x} is: {x.dtype}\n")


# Creating array with defined data type: 

print("Solution:3")
import numpy as np 
x =np.array([1,2,3,4],dtype='S')
print(f"The Data type of {x} is: {x.dtype}\n")


# Now we will Create an array with datatype 4 byte interger
print("Solution:4")
import numpy as np 
x =np.array([1,2,3,4],dtype='i4')
print(f"The Data type of {x} is: {x.dtype}\n")



## Lets say if we pass 2 datatypes in array

print("Solution:5")
import numpy as np 
try:
    x =np.array(['a','2','3','4'],dtype='i4')
    print(f"The Data type of {x} is: {x.dtype}\n")          ## it will give ValueError: invalid literal for int() with base 10: 'a'

except ValueError :
    print("Value Error\n")                                  ## Try expect used just for normal output





## Coverting Data type in existing array

print("Solution:4")
import numpy as np 
x =np.array([1.1,2.2,3.3,4.4],dtype='i4')
x1 = x.astype('i') 
x2 = x.astype(int)
print(f"The Data type of {x1} is: {x1.dtype}")
print(f"The Data type of {x2} is: {x2.dtype}\n")

