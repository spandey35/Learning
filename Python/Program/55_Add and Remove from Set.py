import ast
class Solution():
    def AddRemoveElement (self, choose:str , input:set, Opration:str)->set:
        trans=Opration
        try:
            if choose =="Add":
               input.add(trans)
               return input
        
            elif choose=="Remove":
                input.remove(trans)
                return input
        
            else:
                return "No No !"
            
        except SyntaxError as e:
            return (f"You have entered an invalid set format. Please use correct Python set syntax. Error: {e}")
        
        
        
a=str(input("Choose the option between Add and Remove: "))
b=ast.literal_eval(input("Enter the Set values: "))
c=ast.literal_eval(input("Enter the values: "))

e=Solution()
result=e.AddRemoveElement(a,b,c)
print(result)

      
