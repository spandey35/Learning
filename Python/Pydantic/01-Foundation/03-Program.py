from pydantic import BaseModel
from typing import List ,Dict , Optional

class Cart(BaseModel):
    user_id: int
    items: List[str]
    qunatities = Dict[str, int]


class BlogPost (BaseModel):
    title: str
    content: str
    image = Optional[str] = None

     



