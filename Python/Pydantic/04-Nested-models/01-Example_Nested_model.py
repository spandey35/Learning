from pydantic import BaseModel
from typing import List , Optional

class Address(BaseModel):
    street: str
    city: str
    postal_code: str

class user(BaseModel):
    id: int
    name: str
    address: Address

class Comment(BaseModel):
    id: int
    content: str
    replies : Optional[List["Comment"]]= None


Comment.model_rebuild()  ## for reusing purpose. (metadate)--> Type Safty

address = Address(
    street= "123 Somthing",
    city= "Mumbai",
    postal_code= "10001"
)

User = user(
    id= 1,
    name= "Suraj",
    address= address
)

comment= Comment (
   id=  1,
   content= "First Content",
   replies= [
       Comment(id=2 , content= "Reply1"),
       Comment(id=3 , content= "Reply2")
   ]
)







