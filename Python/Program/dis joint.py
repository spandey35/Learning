import ast
class solution():
    def differnce (self, a:set , b:set)->set:
        try:
            value=a.isdisjoint(b)
            return value
        
        except Exception as e:
            return e
        

a=ast.literal_eval(input("Enter the 1st Values: "))   
b=ast.literal_eval(input("Enter the 2nd values: "))

e=solution()
result=e.differnce(a,b)
print(result)