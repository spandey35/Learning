## Date: 15-11-2025
## CreatedBy: surajss.3110@gmail.com
## You are given a sorted list of unique integers.
## Your task is to identify all the ranges (intervals) that are missing between the smallest and largest number.
## Example Input: [2, 3, 7, 10, 11, 15]
## Expected Output: [(4, 6), (8, 9), (12, 14)]

class Solution():
    def __init__(self, number_list: list) -> None:
        self.number_list = number_list

    def MissingIntervalDetector(self):
        if not self.number_list:
            return "List is empty"
        
        if len(self.number_list) < 2:
            return "Too short to check intervals"

        sorted_list = sorted(self.number_list)
        res = []

        for i in range(1, len(sorted_list)):
            prev_num = sorted_list[i - 1]
            curr_num = sorted_list[i]

            if curr_num - prev_num > 1:
                res.append((prev_num + 1, curr_num - 1))

        return res
try:
    raw = input("Enter the Number by giving Space: ").strip().split()
    clean = [x for x in raw if x.strip() != ""]
    
    user_input = list(map(int, clean))
 
    e = Solution(user_input)
    res = e.MissingIntervalDetector()
    print(f"Missing Intervals in the given List are: {res}")

except Exception as E:
    print(f"Exception at: {E}")




           

