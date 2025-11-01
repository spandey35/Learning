'Type - 1'
class Reversed1:
    def reversed(self, a: str) -> str:
        try:
                value = ''
                for i in range(len(a) - 1, -1, -1):
                    value += a[i]
                return value
        
        except Exception as E:
             print(f'Exception: {E}')

a = input("Enter the values:")

e=Reversed1()
result=e.reversed(a)
print(result)