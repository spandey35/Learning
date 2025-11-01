class Solution:
    def RotateList(self, a: list, option: str, by: int) -> list:
        if not a:
            return []

        by = by % len(a)  # Normalize rotation count

        if option == "Right":
            return a[-by:] + a[:-by]
        elif option == "Left":
            return a[by:] + a[:by]
        else:
            return "Give Correct option"

# Taking user input
input_list = list(map(int, input("Enter list elements separated by space: ").split()))
direction = input("Enter rotation direction (Left/Right): ")
positions = int(input("Enter number of positions to rotate: "))

# Using the class
sol = Solution()
result = sol.RotateList(input_list, direction, positions)
print("Rotated List:", result)
