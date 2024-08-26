from pydantic import BaseModel

class PollingData(BaseModel):
    store_id: int
    local_timestamp: str
    day_of_week: int
    status: str
