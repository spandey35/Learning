from pydantic import BaseModel

## To Create Product Model with id, name, price , in_stock

class ProductModel (BaseModel):
    id : int
    name : str
    price : float
    in_stock : bool = True

user_input = {'id': 101, 'name': 'SurajPandey', "price": 10.5, "in_stock": True}

Product=ProductModel(**user_input)
print(Product)
