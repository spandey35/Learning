from pydantic import BaseModel

class User (BaseModel):
    id : int
    name : str
    is_active : bool

input_date = {'id': 101, 'name':"Suraj Pandey", "is_active": "True"}

User = User(**input_date)
print(User)

