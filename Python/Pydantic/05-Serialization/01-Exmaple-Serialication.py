from pydantic import BaseModel , ConfigDict
from typing import List
from datetime import datetime, timezone

class Address (BaseModel):
    strret: str
    city: str
    pincode: str


class user(BaseModel):
    userid: int
    name: str
    email: str
    isactive: bool = None
    createdOn: datetime
    address : Address
    tags : List[str] = []

    
    model_config = ConfigDict(
        json_encoders= {datetime: lambda v: v.strftime('%d-%m-%Y %H:%M:%S'),
                        bool: lambda v: "IsActive" if v else "InActive"
                        },
        
    )


## Create a User Instance

user = user(
    userid= 1 ,
    name= "Suraj Pandey",
    email= "Suraj.pandey@SP.com",
    isactive= False,
    createdOn= datetime.now(timezone.utc),
    address=  Address(
        strret= "Malad",
        city= "Mumbai",
        pincode= "400097"
    ),
    tags=["A1-User", "Subscription"]
)

###  Using model_dump() ---> Dict

python_dict = user.model_dump()
print("\nDict Type:\n",python_dict)

## Using Model_dump_json -- Json

Python_joson= user.model_dump_json()
print("\nJoson Type:\n",Python_joson)
