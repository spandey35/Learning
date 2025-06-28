from pydantic import BaseModel , Field
from typing import Optional

# Todo: Create a Emp mode
# Feilds:
# id: int
# name:str , (min 2 char)
# department: optional st (defult, "General")
# salary: float (must be >= 10000)

class Employee(BaseModel):
    id: int
    name: str = Field(..., min_length=3 , max_length=50 , description= "Employee Name", examples="Suraj Pandey")
    department :  Optional[str] = "General"
    salary: float =Field (..., ge=10000 )

