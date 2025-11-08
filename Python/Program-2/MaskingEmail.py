# Databricks notebook source
class Solution():
    def maskedemail (self, email):
        try: 
            if "@" not in email:
                return "Invaild email Entered"
        
            username , domain = email.split("@", 1)

            if len(email)>2:
                masked_email = username[:2] + "*" * (len(username)-2)

            else: 
                masked_email = "*" * len(username)
                
            return masked_email + "@" + domain 

        except Exception as E:
            print(f"Expection Occured at: {E}")   


user_name = input("Enter the Email").strip()

e= Solution()
result = e.maskedemail(user_name)
print(result)
        
