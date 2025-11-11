## Date: 11-11-2025
## CreatedBy: surajss.3110@gmail.com
## Print the character which appears the highest number of times along with its count.
## If two characters have the same frequency, print the one that comes first in the string.

class Solution():
    def frequencyChecker(self, words:str)->str:
        try:
            if words == '':
                return "Not Words are given for checking, Try giving some words"
            
            counter_words = {}

            for i in words:
                if i in counter_words:
                    counter_words[i] = counter_words[i] + 1
                else:
                    counter_words[i] = 1  

            max_count = max(counter_words.values())  
            
            for i in words:
                if counter_words[i] == max_count:
                    return i , max_count          
 
        except Exception as E:
            return f"Expection at: {E}"
        

raw_input = input("Enter the words: ")

e = Solution()
res = e.frequencyChecker(raw_input)
print(res)
