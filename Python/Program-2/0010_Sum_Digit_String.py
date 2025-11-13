class Solution():
    def __init__(self, words:str)->None:
        self.words = words
    
    def SumDigit(self):
        if self.words == "":
            print("Empty String")
        
        addition= 0

        for i in self.words:
            if i.isdigit(): 
                    addition += int(i)          
            else: 
                  continue
        
        return addition
    
raw_input= input("Enter the words and digit: ")

try:
    e= Solution(raw_input)
    res = e.SumDigit()
    print(f"Sum of digit in {raw_input} is {res}")

except Exception as E:
     print("Exceptoion at: ",E)
