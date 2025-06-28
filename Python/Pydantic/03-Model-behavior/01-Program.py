from pydantic import BaseModel , field_validator , model_validator , computed_field

# model Behaviours 
# validator
 
class User (BaseModel):
    username: str 

    @field_validator ('Username')
    def username_len (cls , v):
        if len(v) < 4: 
            raise ValueError ("User Name Must be at 4 Charater")
        return v

class Signup (BaseModel):
    password: str
    confirm_password: str

    @model_validator(mode= 'after')
    def passmatch(cls , values):
        if values.password != values.confirm_password:
            raise ValueError ("Password do not match")
        return values
    
class Product (BaseModel):
    price : float
    qunatity : int 


    @computed_field
    @property
    def total_price(self)->float:
        return self.price * self.qunatity
    