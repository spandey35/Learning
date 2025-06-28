class solution():
    def Sequare (self , a:int):
        square = a * a
        return square

input=eval(input("Enter the Number : "))

e=solution()
result = e.Sequare(input)
print(result)