## Date: 11-11-2025
## CreatedBy: surajss.3110@gmail.com
## Print the sum of all numbers which are divisible by both 3 and 5.

from pydantic import BaseModel, validator
from typing import List

## Input Model using Pydantic
class NumberList(BaseModel):
    numbers: List[int]

    @validator('numbers')
    def check_empty(cls, v):
        if len(v) == 0:
            raise ValueError("List cannot be empty")
        return v


class Solution:
    def __init__(self, number_list: NumberList):
        self.number_list = number_list.numbers   

    def sum_divisible_by_3_and_5(self) -> int:
        divisible_numbers = []

        for num in self.number_list:
            if num % 3 == 0 and num % 5 == 0:
                divisible_numbers.append(num)

        return sum(divisible_numbers)


## -------- Program Execution --------
raw_input = input("Enter the Values: ").replace(',', ' ').split()
raw_input = list(map(int, raw_input))  # Convert to int list

# Validate input & create model
number_data = NumberList(numbers=raw_input)

# Pass validated data into solution object
solution = Solution(number_data)

# Get output
result = solution.sum_divisible_by_3_and_5()
print("Output:", result)
