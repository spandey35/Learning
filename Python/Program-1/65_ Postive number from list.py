class Solution ():
    def FilterPostive (self , a:list)->list:
        result = []
        for i in a:
            if i >=0:
                result.append(i)
        
        return result
    
try:
    user_input=list(map(int,input("Enter the Numbers: ").strip().split()))

    e= Solution()
    result=e.FilterPostive(user_input)
    print(result)

except TypeError as E:
    print("Error: ",E)    

except Exception as A:
    print( "Error" , A)

