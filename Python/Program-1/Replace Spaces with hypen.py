class Solution():
    def EachWord (self , a:str)->str:
        replaced= ' '
        for i in a:
            if i != ' ':
                replaced += i
            else:
                replaced += '-'
        
        return replaced
    

a=str(input("Enter the Values: "))

e=Solution()
result=e.EachWord(a)
print(result)


