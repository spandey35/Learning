# Databricks notebook source
class Solution():
    def MissingNumbers(self, numbers):
        try:
            missing = [x for x in range(min(numbers), max(numbers)+1) if x not in numbers]
            return missing
        
        except Exception as E:
            print(f"Expection Occured at: {E}")   


user_numbers = list(input("Enter the Values"))

e= Solution()
result = e.MissingNumbers(user_numbers)
print(result)
                
