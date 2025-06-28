class Solution ():
    def Recursivefactorial ( self , a:int  )->int:
        if  a == 0 or a==1:
            return 1
        
        else:
            return a * self.Recursivefactorial(a-1)
        
try: 
    a = int (input("Enter the Numbers: "))
    e=Solution()
    result = e.Recursivefactorial (a)
    print(result)

except Exception as E:
    print (E)