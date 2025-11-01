class Stack:
    def __init__(self):
        self.stack = []

    def push (self , item):
        self.stack.append(item)
        print (f"Pushed {item} to the Stack")

    def pop (self):
        if self.is_empty():
            return "Stack is empty"
        else:
            print(f" The poped element from stack is:{self.stack.pop()}")

    def peek (self):
        if self.is_empty():
            return "Stack is empty"
        else:
            print(f"The peek element in stack is: {self.stack[-1]}")


    def is_empty (self):
        if self.is_empty  == 0:
            return "Stack is empty"      


    def display(self):
        print(f"The stack in reversed oder",list(reversed(self.stack)))



initial_input = input("Enter initial stack elements (space-separated): ")

user_input = list(map(str ,initial_input.strip().split()))  
stack = Stack()
for item in user_input:
    stack.push(item)


while True:
     print("\nChoose an operation: push <value>, pop, peek, display, exit")   
     input1= input("Enter option: ").strip().lower()

     if input1.startswith ("push"):
         try:
             _, value = input1.split()
             stack.push(str(value))

         except ValueError:
            print("Invalid push command. Use: push <value>")

     elif input1 == "pop":
         stack.pop()

     elif input1 == "peek":
         stack.peek()

     elif input1 == "display":
         stack.display()
    
     elif input1 == "exit":
         print("Exiting..")
         break
     else:
         print("Unknown command.")

         



