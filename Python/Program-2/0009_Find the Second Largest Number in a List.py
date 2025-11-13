class Solution():
    def __init__(self, number:list)->None:
        self.number = number

    def SecondLargestNumber(self):
             if self.number == []:
                 return "List is empty"

             largest = self.number[0]
             SecondLargest = self.number[0]

             for i in range(1, len(self.number)):
                  if self.number[i] > largest:
                    SecondLargest = largest
                    largest = self.number[i]
            
                  elif self.number[i] < largest and self.number[i] > SecondLargest:
                    SecondLargest = self.number[i]
                
             return SecondLargest
        
    
try:
  user_input = list(map(int, input("Enter the values separated by spaces: ").split()))

  e= Solution(user_input)
  res = e.SecondLargestNumber()
  print(res)

except Exception as E:
        print(f"Expected at: {E}")
    

