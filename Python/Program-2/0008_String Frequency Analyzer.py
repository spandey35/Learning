## Date: 12-11-2025
## CreatedBy: surajss.3110@gmail.com
## Given a paragraph of text, find the most frequent word(s) and display both the word(s) and their frequency count.
## Example Input: Python is great, and Python is fun. Fun things are great!
## Expected Output: Most frequent words: ['python', 'great', 'fun'] , Frequency: 2

class Solution():
    def __init__(self, words:str)->None:
        self.words= words

    def frequency(self):
        try:
            if self.words == "":
                return "Empty String"
            
            word = self.words.split()
            check = {}
            
            for i in word:
                if i in check:
                    check[i] = check[i] + 1
                else:
                    check[i] = 1

            max_list = []
            max_count = max(check.values())

            for i in word:
                if check[i] == max_count:
                     if i not in max_list:
                        max_list.append(i)

            return f"Most frequent words: {max_list}\nFrequency: {max_count}"
   
        
        except Exception as E:
            return(f"Expection at: {E}")
        

raw_input = input("Enter the Words you need to check: ")

e = Solution(raw_input)
res = e.frequency()
print(res)
