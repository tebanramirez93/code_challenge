import json
import shutil
import pandas as pd
import pyarrow as pa
import os
import jwt
import io
import fastavro
from fastapi import FastAPI
from itertools import islice
from utils.processing import *
from utils.models import *
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Response, Depends
from security import validate_token
from typing import Union, Any
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from google.cloud import bigquery 

# Creating instance for bigquery client

client = bigquery.Client(project='datatest-305123')

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

@app.post("/api/V1/move-between-databases", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def move_between_databases():    
    
    tables_saved_json  = convert_files_xlsx_to_csv()
    return tables_saved_json
   


@app.post("/api/V1/create-historical-tables", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def create_historical_tables():    
    
    tables_created_json = ingest_files_into_bq()
    return {"inserted tables": tables_created_json}


@app.post("/api/V1/insert-rows-up-to-onek", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def insert_until_1k():    

    inserted_records = insert_rows_up_to_onek()
    return {"inserted_records": inserted_records}

# Endpoint to insert data using Json from Swagger of API REST Request with limit of 1000 of rows

@app.post("/api/V1/insert-data-new-hires", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def insert_data(data: InsertNewHired):
    try:
        data_dict = data.dict()
        table_ref = client.dataset('code_challenge').table('hired_employees')
        table = client.get_table(table_ref)
        
       
        limited_data = list(islice([data_dict], 1000))
        
        num_records = len(limited_data)  
        
        errors = client.insert_rows_json(table, limited_data)

        if errors:
            raise HTTPException(status_code=500, detail=f'Error while inserting data: {errors}')

        return {"message": "Data inserted successfully", "num_records": num_records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error: {str(e)}')




@app.post("/api/V1/insert-departments", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def insert_data(data: InsertDepartments):
    try:
        data_dict = data.dict()
        table_ref = client.dataset('code_challenge').table('departments')
        table = client.get_table(table_ref)
        
       
        limited_data = list(islice([data_dict], 1000))
        
        num_records = len(limited_data)  
        
        errors = client.insert_rows_json(table, limited_data)

        if errors:
            raise HTTPException(status_code=500, detail=f'Error while inserting data: {errors}')

        return {"message": "Data inserted successfully", "num_records": num_records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error: {str(e)}')
    


@app.post("/api/V1/insert-jobs", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def insert_data(data: InsertJobs):
    try:
        data_dict = data.dict()
        table_ref = client.dataset('code_challenge').table('jobs')
        table = client.get_table(table_ref)
        
       
        limited_data = list(islice([data_dict], 1000))
        
        num_records = len(limited_data)  
        
        errors = client.insert_rows_json(table, limited_data)

        if errors:
            raise HTTPException(status_code=500, detail=f'Error while inserting data: {errors}')

        return {"message": "Data inserted successfully", "num_records": num_records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error: {str(e)}')
    

@app.post("/api/V1/save-backup-avro", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def read_table_as_avro():
    files_exported  = export_table_to_avro()
    return files_exported

   
@app.post("/api/V1/save-backup-avro-per-table", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def read_table_as_avro(table_id):
    files_exported  = export_table_to_avro_on_demand(table_id)
    return files_exported


@app.post("/api/V1/restore-backup-per-table", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def resto_backup_per_file(table_id):
    files_exported  = load_avro_file(table_id)
    return files_exported

@app.post("/api/V1/restore-full-backup", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def resto_backup_full():
    files_exported  = load_avro_full()
    return files_exported



@app.post("/api/V1/refine-tables", tags=["Code Challenge # 1"],dependencies=[Depends(validate_token)])
def refine_tables():
    tables_curated  = clean_duplicated_data()
    return tables_curated

@app.post("/api/V1/summarize-per-dept-job", tags=["Code Challenge # 2"],dependencies=[Depends(validate_token)])
def summarize_per_dep_job():
    summarize()
    return "Tables of summarizing has been created correctly"


@app.post("/api/V1/higher-hiring", tags=["Code Challenge # 2"],dependencies=[Depends(validate_token)])
def higher_hiring_table():
    higher_hiring_mean()
    return "Tables of higher hiring has been created correctly"