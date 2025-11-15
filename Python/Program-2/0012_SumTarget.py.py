# Databricks notebook source
class Solution():
    def __init__(self, number_list, Target):
        self.number_list = number_list
        self.Target = Target

    def SumTarget(self):
        if self.number_list == []:
            return "Empty List"
        
        elif len(self.number_list) < 2: 
            return "With One Number we can not Find the target"
        
        res = []

        for i in range(1,len(self.number_list)):
            for j in range(i + 1, len(self.number_list)):
                if self.number_list[i] + self.number_list[j] == self.Target:
                    res.append((self.number_list[i], self.number_list[j]))

        return res if res else "No Pairs found"
    
user_input_number= input("Enter the Numbers (space-separated): ")
user_input_Target= input("Enter the Target: ")

number_list = list(map(int, user_input_number.split()))
target = int(user_input_Target)

e = Solution(number_list, target)
res = e.SumTarget()
print(res)

