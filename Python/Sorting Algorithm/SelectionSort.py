class SelectionSort:
    def __init__(self):
        self.numbers = []

    def get_input(self):
        try:
            size = int(input("Enter the number of elements: "))
            print(f"Enter {size} integers:")
            for i in range(size):
                element=int(input(f"Element {i+1}: "))
                self.numbers.append(element)
        except ValueError:
            print("Please enter valid integers only.")

    def sort(self):
        n = len(self.numbers)
       
        for i in range(n):
            min_idx = i
            for j in range(i+1, n):
                if self.numbers[j] < self.numbers[min_idx]:
                    min_idx = j
           
            self.numbers[i], self.numbers[min_idx] = self.numbers[min_idx], self.numbers[i]

        

    def display(self):
        print("Sorted array:", self.numbers)


# Main execution
if __name__ == "__main__":
    sorter = SelectionSort()
    sorter.get_input()
    sorter.sort()
    sorter.display()
