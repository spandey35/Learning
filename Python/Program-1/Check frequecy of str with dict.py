class Solution():
    def checkwithdict (self, a:str)->dict:
        dict={}
        for i in a :
            if i not in dict:
                dict[i] = 1
            else:
                dict[i] += 1
            
        return dict
    
a=input("Enter the values: ")

e=Solution()
result=e.checkwithdict(a)
print(result)