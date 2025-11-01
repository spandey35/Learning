import ast

class Solution:
    def flatten_nested_list(self, nested_list: list) -> list:
        flat_list = []
        for i in nested_list:
            for j in i:
                flat_list.append(j)
        return flat_list

try:
    
    user_input = input("Enter a nested list (e.g., [[12, 2], [2, 3], [22, 4]]): ")

    nested_list = ast.literal_eval(user_input)

    solver = Solution()
    result = solver.flatten_nested_list(nested_list)

    print("Flattened list:", result)

except Exception as e:
    print("Error:", e)
