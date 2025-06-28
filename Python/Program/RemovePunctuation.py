class Solution():
    def Removepunctuation(self, a:str)->str:
        check= "!"
        final=''
        for i in a:
            if i != check:
                final = final + i
            
            else:
                continue

        return final

a=str(input("Enter anything in String: "))

e=Solution()
result = e.Removepunctuation(a)
print(result)
