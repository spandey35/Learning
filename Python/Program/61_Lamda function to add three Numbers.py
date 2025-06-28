class solution ():
    def addThree_lamda (self , a:int , b:int , c:int)->int:
        return (lambda x , y ,z : x + y + z) (a , b ,c)
        
    
try: 
    a= int(input("Enter the numbers: "))
    b= int(input("Enter the numbers: "))
    c= int(input("Enter the numbers: "))

    e= solution()
    result= e.addThree_lamda(a,b,c)
    print(result)

    
except Exception as E:
    print(E)  
