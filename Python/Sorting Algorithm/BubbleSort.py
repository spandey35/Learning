class BubbleSort():
    def __init__(self):
        self.numbers = []  

    def get_input(self):
        try:
            size = int(input("Enter how many numbers you want to sort: "))
            print(f"You will enter {size} numbers.")
            for i in range(size):
                element = int(input(f"Element {i+1}: ")) 
                self.numbers.append(element)


        except ValueError:
            print("Please enter valid integers only.")

    def bubblesort(self):
        n = len(self.numbers)
        for i in range(n):
            for j in range(0, n - 1 - i):
                if self.numbers[j] > self.numbers[j + 1]:
                    self.numbers[j], self.numbers[j + 1] = self.numbers[j + 1], self.numbers[j]

    def display(self):
        print("Sorted array:", self.numbers)

# Main execution
if __name__ == "__main__":
    sorter = BubbleSort()
    sorter.get_input()
    sorter.bubblesort()
    sorter.display()
