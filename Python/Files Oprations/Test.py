import csv , os 
with open ("l",'w',newline='') as f:
    w = csv.writer(f)
    w.writerow(["ENO","ENAME","ESAL","EADDR"])
    n=int(input("Enter numbers of emoployee: "))

    for i in range(n):
        eno=int(input("Enter Employee Number: "))
        enane=input("Enter Employee Name: ")
        esal=float(input("Enter Employee Sale: "))
        eadd=input("Enter Employee Sale:")

        w.writerow([eno,enane,esal,eadd])

print("Totol emp data written CSV file successfully")       



