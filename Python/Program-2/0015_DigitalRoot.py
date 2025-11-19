class Solution():
    def __init__(self, values: list) -> int:
        self.values = values

    def DigitalRoot(self):
        if self.values == []:
            print("No Values are given in the list")
            exit()

        
        while True:
            
            checker = 0
            for i in range(len(self.values)):
                checker += self.values[i]

            
            if checker < 10:
                return checker

            self.values = [int(d) for d in str(checker)]



num = input("Enter a number: ")

values_list = [int(d) for d in num]

obj = Solution(values_list)
print("Digital Root is:", obj.DigitalRoot())
