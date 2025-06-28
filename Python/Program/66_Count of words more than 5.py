class Solution ():
    def FilterPostive (self , a:list)->list:
        result = []
        for i in a:
            if len(i)>5:
                result.append(i)
        
        return result
    
try:
    user_input=list(map(str,input("Enter the Numbers: ").strip().split()))

    e= Solution()
    result=e.FilterPostive(user_input)
    print(result)

except TypeError as E:
    print("Error: ",E)    

except Exception as A:
    print( "Error" , A)

