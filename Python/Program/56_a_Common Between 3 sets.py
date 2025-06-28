import ast
import traceback
class Solution():
        try:
          def CommanElement (self , a:set , b:set , c:set)->set:
                value = a.union(b,c)
                return value
          
        except Exception as e:
            print (f"You have entered an invalid set format. Please use correct Python set syntax. Error: {e}")

try:

    a=ast.literal_eval(input("Enter the 1st Set values: "))
    b=ast.literal_eval(input("Enter the 2nd Set values: "))
    c=ast.literal_eval(input("Enter the 3rd Set values: "))

    e=Solution()
    result=e.CommanElement(a,b,c)
    print(result)

except SyntaxError as e:
     print (f"You have entered an invalid set format. Please use correct Python set syntax. Error: {e}")
     traceback.extract_tb()


except Exception as e:
     print(f"An unexpected error occurred: {e}")
     traceback.print_exc()

    
