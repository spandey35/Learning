class Solution:
    def CheckAmranStrong(self, a: int) -> str:
        num_str = str(a)
        num_digits = len(num_str)
        result = 0
        for i in  num_str :
            result += int(i) ** num_digits
            
        if a == result:
            return "The given Number is an AmranStrong Number"
        else:
            return "The given Number is not an AmranStrong Number"
        

try:
    a = int(input ("Enter the Number: "))
    e=Solution()
    result=e.CheckAmranStrong(a)
    print(result)



except Exception as E:
    print (E)
