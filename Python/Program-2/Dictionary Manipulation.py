# Databricks notebook source
class Manipulation():
    def avarage_score(self, student_scores: dict)-> dict:
        try:
            all_score = list(student_scores.values())

            sum_score = sum(all_score)
            avarage_score = sum_score / len (all_score) 

            students_above_avg = {}
            for student in student_scores:
                if student_scores[student] > avarage_score:
                    students_above_avg[student] = student_scores[student]        

            return students_above_avg

        except Exception as E:
            return f"Excpetion at: {E}"    

raw_input = input("Enter students and scores (e.g., suraj1:20,suraj2:30,suraj3:30): ")

# Split and build dictionary
student_scores = {}
pairs = raw_input.split(",")
for pair in pairs:
    name, score = pair.strip().split(":")
    student_scores[name] = int(score)


e = Manipulation()
result = e.avarage_score(student_scores)
print("Students who scored above average:", result)

