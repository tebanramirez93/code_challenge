import os 
import pandas as pd 
import csv 
import fastavro
import json
from google.cloud import bigquery, storage 
from itertools import islice


# Opening Client bigquery & Storage instances
storage_client = storage.Client(project='datatest-305123')
Client = bigquery.Client(project='datatest-305123')



## Historic location
folder_path = 'historic'
file_list = os.listdir(folder_path)

# bigquery functions schema 

schemas = {
    'hired_employees': [
        bigquery.SchemaField(name="id", field_type="INTEGER"),
        bigquery.SchemaField(name="name", field_type="STRING"),
        bigquery.SchemaField(name="datetime", field_type="STRING"),
        bigquery.SchemaField(name="department_id", field_type="INTEGER"),
        bigquery.SchemaField(name="job_id", field_type="INTEGER"),
    ],
    'departments': [
        bigquery.SchemaField(name="id", field_type="INTEGER"),
        bigquery.SchemaField(name="department", field_type="STRING"),
    ],
    'jobs': [
        bigquery.SchemaField(name="id", field_type="INTEGER"),
        bigquery.SchemaField(name="job", field_type="STRING"),
    ],
}


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



def insert_rows_up_to_onek():
    bucket_name = 'transport-bucket-challenge'
    storage_client = storage.Client()
    Client = bigquery.Client()

    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    inserted_records = {}

    for blob in blobs:
        if blob.name.endswith('.csv'):
            filename_path = blob.name

            file_name = filename_path.replace('.csv','')

            schema = schemas.get(file_name)
            dataset_ref = Client.dataset('code_challenge')
            table_ref = dataset_ref.table(file_name)

            if schema is None:
                print(f"No schema found for table: {file_name}")
                continue

            blob = bucket.blob(filename_path)
            csv_data = blob.download_as_text()
            data = list(islice(csv.reader(csv_data.splitlines(), delimiter=','), 1000))

            errors = Client.insert_rows(table_ref, data, selected_fields=schema)

            if not errors:
                print(f'Data inserted into table: {dataset_ref}.{table_ref}')
                records_inserted = len(data)
                print(f'Records inserted for table {file_name}: {records_inserted}')
                if file_name in inserted_records:
                    inserted_records[file_name] += records_inserted
                else:
                    inserted_records[file_name] = records_inserted
            else:
                print(f'Encountered errors while inserting data: {errors}')

    for table, records in inserted_records.items():
        print(f'Total records inserted for table {table}: {records}')

    return inserted_records


