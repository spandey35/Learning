from pydantic import BaseModel ,Field ,computed_field

# Todo: Create Booking model
# fields:
# - User_id : int
# - room_id : int
# - nights: int (must be >=1)
# - rate_per_night : float
# - Also, add computed feild : total_amount = nights * rate_per_night


class Booking(BaseModel):
    User_id : int
    room_id: int
    nights : int = Field(..., ge=1)
    rate_per_night : float

    @computed_field  # it will take always 2 decorater where   @computed_field,  @property (Values)
    @property
    def total_amount(self)->float:
        return self.nights * self.rate_per_night
    

