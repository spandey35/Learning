class Solution():
    def __init__(self, iteration:int, values:list)->None:
        self.values = values
        self.iteration = iteration

    def InputSum(self):
        if self.iteration < 0 :
            print( "Value should be greater then Zero" )
            exit()
        
        if len(self.values) != self.iteration :
            print( "Values given is More or less then the itration values , Please give extact values")
            exit()
        
        for v in self.values:
            if v < 0:
                return "Negative values are not allowed"

        sum=0

        for i in range(len(self.values)):
            sum += self.values[i]
        
        return sum
    

Iteration = int(input("Enter the number of values you want to check: "))

Values = list(map(int, input("Enter the values separated by space: ").strip().split()))

e = Solution(Iteration, Values)

print("Sum of values is:", e.InputSum())







