class Solution():
    def frequentWords (self , a:str )->dict:
        check = {}
        for i in a.split() :
            if i not in check:
                check[i] = 1

            else:
                check[i] += 1
            
            
        return check
    
a= str(input("Enter the words to check frequent words used: "))

e=Solution()
result=e.frequentWords(a)
print(result)