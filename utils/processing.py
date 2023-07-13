import os 
import pandas as pd 
import csv 
import fastavro
import json
from google.cloud import bigquery, storage 

# Opening Client bigquery & Storage instances
storage_client = storage.Client(project='datatest-305123')
Client = bigquery.Client(project='datatest-305123')



## Historic location
folder_path = 'historic'
file_list = os.listdir(folder_path)


def convert_files_xlsx_to_csv():
    tables_saved = {}

    for file_name in file_list:
        historical_file = file_name.split('.')[0]
        source_path = folder_path + '/'
        df = pd.read_excel(source_path + historical_file + '.xlsx')
        df_csv = df.fillna(0)
        numeric_columns = df_csv.select_dtypes(include=[float]).columns
        df_csv[numeric_columns] = df_csv[numeric_columns].astype(int)  # Correcting default float assignation

        csv_path = 'gs://code-challenge-one/new_database/' + historical_file + '.csv'
        df_csv.to_csv(csv_path, index=False)
        tables_saved[historical_file] = csv_path


    tables_saved_json = json.dumps(tables_saved, indent=None)
    tables_saved_json = json.loads(tables_saved_json)
   
    
    return tables_saved_json



def ingest_files_into_bq():
    bucket_name = 'code-challenge-one' 
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    tables_created = {}

    for blob in blobs:
        if blob.name.endswith('.csv'):
            filename_path = blob.name
            file_name = filename_path.split('/')[1].replace('.csv', '')        

            with open(f'schemas/{file_name}.txt') as f:
                schema = f.read()

            try:
                creation_query = f"CREATE OR REPLACE EXTERNAL TABLE `datatest-305123.test.{file_name}_raw` ({schema}) OPTIONS (format = 'CSV', uris = ['gs://{bucket_name}/{filename_path}'], field_delimiter = ',', skip_leading_rows = 0)"
                query_job = Client.query(creation_query)
                query_job.result()
                tables_created[file_name + "_raw"] = "Table created successfully."
            except Exception as e:
                tables_created[file_name + "_raw"] = "Error creating table: " + str(e)

            try:
                format_query = f"CREATE OR REPLACE TABLE `datatest-305123.code_challenge.{file_name}` AS SELECT DISTINCT * FROM `datatest-305123.test.{file_name}_raw`"
                query_job = Client.query(format_query)
                query_job.result()
                tables_created[file_name] = "Table created successfully."
            except Exception as e:
                tables_created[file_name] = "Error creating table: " + str(e)

    tables_created_json = json.dumps(tables_created)
    tables_created_json = json.loads(tables_created_json)
    return tables_created_json