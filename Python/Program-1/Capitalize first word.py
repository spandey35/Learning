class Solution():
    def CapitalizeWords (self, a:str)->str:
        words=a.title()
        return words
    
a=str(input("Enter the Values: "))

e=Solution()
result=e.CapitalizeWords(a)
print(result)
