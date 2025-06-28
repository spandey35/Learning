import ast

class SetOperations:
    def __init__(self):
        self.sets_list = []

    def input_sets(self, count: int):
        for i in range(count):
            user_input = input(f"Enter set {i+1} (e.g., {{1, 2, 3}}): ")
            
            try:
               
                current_set = ast.literal_eval(user_input)
                if not isinstance(current_set, set):
                    raise ValueError("Input is not a valid set.")
                self.sets_list.append(current_set)
            
            except Exception as e:
                print(f"Invalid input for set {i+1}. Error: {e}")
                exit()

    
    def compute_union(self) -> set:
        result_union = set()
        for s in self.sets_list:
            result_union = result_union.union(s)
        return result_union

# Main program
try:
    num_sets = int(input("How many sets do you want to enter? "))
    operation = SetOperations()
    operation.input_sets(num_sets)
    result = operation.compute_union()
    print(f"\nUnion of all sets: {result}")

except ValueError:
    print("Please enter a valid number.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
