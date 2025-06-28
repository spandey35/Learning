from typing import List
class Solution():
    def Dictionarylists (self, a:List , b:List)->dict:
        values=dict(zip(a,b))
        return values
    
a=list(input("Enter the Values: "))
b=list(input("Enter the Values: "))


e=Solution()
result=e.Dictionarylists(a,b)
print(result)
