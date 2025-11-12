# Databricks notebook source
## Date: 12-11-2025
## CreatedBy: surajss.3110@gmail.com
## You have a list of integers. You need to find the longest contiguous subsequence where each consecutive element differs by exactly 1.
## “Contiguous” means the numbers appear next to each other in the original list (no skipping).
## The difference between consecutive elements should be +1 or -1, but since the example uses increasing order, we assume +1.
## Example Input: [1, 2, 3, 5, 6, 7, 8, 10, 11, 12, 13, 14]
## Expected Output: [10, 11, 12, 13, 14]


class Solution():
    def __init__(self, number_list:list)->list:
        self.number_list = number_list

    def subsequence(self):
        try:
            if not self.number_list:
                return []
            
            current_seq = [self.number_list[0]]
            longest_seq = []

            for i in range (1, len(self.number_list)):
                if self.number_list[i] - self.number_list[i-1] == 1:
                    current_seq.append(self.number_list[i])
                
                else:
                    if len(current_seq) > len(longest_seq):
                        longest_seq =  current_seq
                    current_seq = [self.number_list[i]]

            if len(current_seq) > len(longest_seq):
                    longest_seq =  current_seq

            return longest_seq
            
        except Exception as E:
            return f"Expected at: {E}"
        

try:
    raw_input = input("Enter the values separated by spaces: ")
    number_list = list(map(int, raw_input.strip().split()))
    e = Solution(number_list)
    res = e.subsequence()
    print("Longest contiguous subsequence with difference = 1:", res)


except Exception as e:
    print(f"Invalid input! Error: {e}")




            




