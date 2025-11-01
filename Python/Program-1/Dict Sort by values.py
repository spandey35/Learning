class Solution():
    def SortByValue(self, a: dict) -> dict:
        sorted_items = sorted(a.items(), key=lambda item: item[1])
        return dict(sorted_items)

input_str = input("Enter the dictionary (e.g., {'a': 3, 'b': 1}): ")
a = eval(input_str)

e = Solution()
result = e.SortByValue(a)
print(result)
