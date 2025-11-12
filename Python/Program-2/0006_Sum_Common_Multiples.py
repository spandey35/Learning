## Date: 11-11-2025
## CreatedBy: surajss.3110@gmail.com
## Print the sum of all numbers which are divisible by both 3 and 5.

class Solution:
    def __init__(self, numbers):
        self.numbers = numbers                

    def sum_divisible_by_3_and_5(self):
        divisible_numbers = []                 
        
        for num in self.numbers:
            if num % 3 == 0 and num % 5 == 0:
                divisible_numbers.append(num)

        return sum(divisible_numbers)          



raw_input = input("Enter the Values: ").replace(',', ' ').split()
raw_input = list(map(int, raw_input))          

solution = Solution(raw_input)                 
result = solution.sum_divisible_by_3_and_5()   

print("Output:", result)
