class Solution():
    def NestedList (self , a:list , b:list)->list:
        empty_list = []
        for i , j in zip(a ,b) :
            empty_list.append ([i , j])

        return empty_list
    
try: 
    a = list (map(int,input("Enter the Numbers: ").split()))
    b = list (map(int,input("Enter the Numbers: ").split()))
    e=Solution()
    result = e.NestedList (a , b)
    print(result)

except Exception as E:
    print (E)