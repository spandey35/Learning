class Stack:
    def __init__(self):
        self.stack = []  ## checking

    def push(self, item):
        self.stack.append(item)
        print(f"Pushed {item} to stack.")

    def pop(self):
        if self.is_empty():
            print("Stack is empty. Cannot pop.")
        else:
            print(f"Popped {self.stack.pop()} from stack.")

    def peek(self):
        if self.is_empty():
            print("Stack is empty.")
        else:
            print(f"Top element is: {self.stack[-1]}")

    def is_empty(self):
        return len(self.stack) == 0

    def display(self):
        print("Stack (top to bottom):", list(reversed(self.stack)))



# Initialize stack with user input
initial_input = input("Enter initial stack elements (space-separated): ")

elements = list(map(int, initial_input.strip().split()))
stack = Stack()
for item in elements:
    stack.push(item)

    

# Interactive operations
while True:
    print("\nChoose an operation: push <value>, pop, peek, display, exit")
    command = input("Enter command: ").strip().lower()

    if command.startswith("push"):
        try:
            _, value = command.split()
            stack.push(int(value))

        except ValueError:
            print("Invalid push command. Use: push <value>")

    elif command == "pop":
        stack.pop()

    elif command == "peek":
        stack.peek()

    elif command == "display":
        stack.display()

    elif command == "exit":
        print("Exiting...")
        break
    
    else:
        print("Unknown command.")

