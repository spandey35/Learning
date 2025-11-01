class Solution():
    def mergedict( self , a:dict , b:dict )->dict:
        marge = a | b 
        return marge
    
a=eval(input("Enter the values: "))
b=eval(input("Enter the values: "))

e=Solution()
result=e.mergedict(a,b)
print(result)
        