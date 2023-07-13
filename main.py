import json
import shutil
import os
import jwt
import io
import pandas as pd
import pyarrow as pa
import fastavro
from fastapi import FastAPI
from google.cloud import bigquery 
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Response, Depends
from typing import Union, Any
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from google.cloud import bigquery 
from utils.processing import * 

# Creating instance for bigquery client

client = bigquery.Client()

########################### Security ##############################################################################################

SECURITY_ALGORITHM = 'HS256'
SECRET_KEY = '123456'

reusable_oauth2 = HTTPBearer(scheme_name='Authorization')

class LoginRequest(BaseModel):
    username: str
    password: str

def verify_password(username, password):
    username = 'prueba'
    password = 'challenge'

    if username == username and password == password:
        return True
    return False


def generate_token(username: Union[str, Any]) -> str:
    expire = datetime.utcnow() + timedelta(
        seconds=60 * 60 * 24 * 3  # Expired after 3 days
    )
    to_encode = {
        "exp": expire, "username": username
    }
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=SECURITY_ALGORITHM)
    return encoded_jwt

###################################################################################################################################

app = FastAPI(
    title='Code Challenge API',
    description='API created to fetch data samples to challenge # 1 & 2',
    version='0.0.1')


@app.post('/login',tags=["Authentication"])
def login(request_data: LoginRequest):
    print(f'[x] request_data: {request_data.__dict__}')
    if verify_password(username=request_data.username, password=request_data.password):
        token = generate_token(request_data.username)
        return {
            'This is the token to authenticate in this API': token
        }
    else:
        raise HTTPException(status_code=404, detail="User or passwor incorrect Please input correct Credentials")
    
############# API ########################################################################################################################

@app.post("/api/V1/move-between-databases", tags=["Code Challenge # 1"])
def move_between_databases():    
    
    tables_saved_json  = convert_files_xlsx_to_csv()
    return tables_saved_json
   


@app.post("/api/V1/create-historical-tables", tags=["Code Challenge # 1"])
def create_historical_tables():    
    
    tables_created_json = ingest_files_into_bq()
    return {"inserted tables": tables_created_json}
