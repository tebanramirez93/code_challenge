import os 
import pandas as pd 
import csv 
import fastavro
from google.cloud import bigquery 

# Opening Client bigquery instance 

## Historic location
folder_path = 'historic'
file_list = os.listdir(folder_path)

def convert_files_xlsx_to_csv():
    
      
    for file_name in file_list:
        historical_file = file_name.split('.')[0]
        source_path = folder_path + '/'
        destination_path = 'newDatabase/'
        df = pd.read_excel(source_path + historical_file + '.xlsx')
        df_csv = df.fillna(0)

        df_csv.to_csv('gs://code-challenge-one/new_database/' + historical_file + '.csv', index=False)  