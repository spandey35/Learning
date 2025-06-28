class InsertionSortClas():
    def __init__(self):
        self.numbers = []

    def get_input(self):
        size = int(input("Enter the Size :"))
        print("Entred Size is :", size)

        try: 
             for i in range (size):
                 elements = int(input(f"Enter the element for {i+1}: "))
                 self.numbers.append(elements)

        except ValueError:
            print("Value Error, Please give proper Input.")


    def Insertionsort(self):
        n = len(self.numbers)

        for i in range (1,n):
            key = self.numbers[i]
            j  = i -  1

            while j >= 0 and key < self.numbers [j]:
                self.numbers[j + 1] = self.numbers [j]
                j -= 1
            self.numbers [j +1] = key  


    def display(self):
        print("Soreted Array",self.numbers) 

# Main execution
if __name__ == "__main__":
    sorter = InsertionSortClas()
    sorter.get_input()
    sorter.Insertionsort()
    sorter.display()
