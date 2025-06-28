from pydantic import BaseModel 
from typing import List

# todo: Create Course model
# Each Course has modules
# Each Module has Lessons

class Lessons (BaseModel):
    Lesson_id: int
    topic : str

class module (BaseModel):
    module_id: int
    name: str
    lession: List[Lessons]

class course(BaseModel):
    course_id: int
    title: str
    modules = List[module]
