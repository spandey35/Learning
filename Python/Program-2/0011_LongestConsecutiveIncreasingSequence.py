class Solution():
    def __init__(self, sequence:list)->None:
        self.seq_list = sequence

    def ConsecutiveSeq(self):
        if self.seq_list == []:
            return "Empty list"
        

        sorted_list = sorted(self.seq_list)
        current_sq = [sorted_list[0]]
        longest_sq = []

        for i in range(1, len(sorted_list)):
            if sorted_list[i] == sorted_list[i-1] + 1:
                current_sq.append(sorted_list[i])
            
            else:
                if len(current_sq) > len(longest_sq):
                    longest_sq = current_sq.copy()
                    current_sq = [sorted_list[i]]  

        if len(current_sq) > len(longest_sq):
            longest_sq = current_sq.copy()
        return  longest_sq 
try:
    raw_input = input("Enter the values separated by spaces: ")
    number_list = list(map(int, raw_input.strip().split()))
    e = Solution(number_list)
    res = e.ConsecutiveSeq()
    print(f"Sequence: {res}\nLenght: {len(res)}")


except Exception as e:
    print(f"Invalid input! Error: {e}")    
    
                    

