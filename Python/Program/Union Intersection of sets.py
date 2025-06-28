import ast
class Solution():
    def UnionInterscet (self, choose:str, a:set , b:set)->set:
        try:
            if choose=="Union":
                uni = a.union(b)
                return uni
            
            elif choose=="Intersection":
                 inter= a.intersection(b)
                 return inter
        
            else :
                return "Go Away !! "
            
        except Exception as e:
            return f"An error occurred: {e}"
        
a=str(input("Enter the Values: "))
b = ast.literal_eval(input("Enter the 1st Set (e.g., {1, 2, 3}): "))
c = ast.literal_eval(input("Enter the 2nd Set (e.g., {3, 4, 5}): "))


e=Solution()
result=e.UnionInterscet(a,b,c)
print(result)



    