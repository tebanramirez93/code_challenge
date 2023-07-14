from pydantic import BaseModel, Field

class InsertNewHired(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

    class Config:
        json_encoder = None
        json_schema_extra = {
            "example": {
                "id": 1,
                "name": "James Steward",
                "datetime": "2021-11-07T02:48:42Z",
                "department_id": 3,
                "job_id": 5
            }
        }

class InsertDepartments(BaseModel):
    id: int
    department: str


    class Config:
        json_encoder = None
        json_schema_extra = {
            "example": {
                "id": 1,
                "department": "Human Resources",
            }
        }


class InsertJobs(BaseModel):
    id: int
    job: str


    class Config:
        json_encoder = None
        json_schema_extra = {
            "example": {
                "id": 1,
                "job": "Developer",
            }
        }