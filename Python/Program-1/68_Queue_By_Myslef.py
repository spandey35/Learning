class Queue:
    def __init__(self):
        self.queue = []

    def enqueue(self, item):
        self.queue.append(item)
        print(f"Enqueued: {item}")

    def dequeue(self):
        if self.is_empty():
            print("Queue is empty. Cannot dequeue.")
        else:
            print(f"Dequeued: {self.queue.pop(0)}")

    def peek(self):
        if self.is_empty():
            print("Queue is empty.")
        else:
            print(f"Front of queue: {self.queue[0]}")

    def is_empty(self):
        return len(self.queue) == 0

    def display(self):
        print("Queue (front to rear):", self.queue)


# Initialize queue with user input
initial_input = input("Enter initial queue elements (space-separated): ")
user_input = list(map(str, initial_input.strip().split()))

queue = Queue()
for item in user_input:
    queue.enqueue(item)

# Interactive loop
while True:
    print("\nChoose an operation: enqueue <value>, dequeue, peek, display, exit")
    command = input("Enter option: ").strip().lower()

    if command.startswith("enqueue"):
        try:
            _, value = command.split()
            queue.enqueue(value)
        except ValueError:
            print("Invalid enqueue command. Use: enqueue <value>")

    elif command == "dequeue":
        queue.dequeue()

    elif command == "peek":
        queue.peek()

    elif command == "display":
        queue.display()

    elif command == "exit":
        print("Exiting...")
        break

    else:
        print("Unknown command.")
