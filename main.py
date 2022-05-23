
from datetime import datetime
from google.cloud import *
from google.cloud import bigquery
import json 
import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df
import logging
import os
from google.cloud import storage
from google.cloud.bigquery.client import Client
from google.oauth2 import service_account


# Path to the service account credentials
'''key_path = "sigma-scheduler-348710-0e55acb5c90d.json"
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)'''

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'sigma-scheduler-348710-0e55acb5c90d.json'
bq_client = Client()

# export GOOGLE_APPLICATION_CREDENTIALS="sigma-scheduler-348710-0e55acb5c90d.json"  

def upload_blob(bucket_name, source_file_name, destination_blob_name):
  #Uploads a file to the bucket
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)

  blob.upload_from_filename(source_file_name)

  print('File {} uploaded to {}.'.format(
      source_file_name,
      destination_blob_name))

def preprocess(js, name):
    with open(js) as file:
        data = json.load(file)
    if js == 'data/events.json':
        rsvps = pd.json_normalize(data, record_path=['rsvps'], record_prefix = 'rsvps_', meta=['name', 'status', 'time', 'duration', 'group_id', 'created', 'description'])
        rsvps.to_json(name, orient='records', lines=True)
    elif js == 'data/users.json':
        members = pd.json_normalize(data, record_path=['memberships'], record_prefix='memberships_', meta=['user_id', 'hometown', 'country', 'city'])
        members.to_json(name, orient='records', lines=True)
    else:
        df = pd.json_normalize(data)
        df.to_json(name, orient='records', lines=True)

"""def load_bq(file, table_id, load_type):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # job config bq
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=load_type
    )
    # Loading JSON file from local system
    with open(file, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    #OAUTH2 issue otherwise load from gcs bucket to bq tables
    '''
    #load from gcs
    n = ['events', 'groups', 'users', 'venues']
    if file == 'events_flat.json':
        uri=f'gs://pya_bucket/{n[0]}'
    elif file == 'groups_flat.json':
        uri=f'gs://pya_bucket/{n[1]}'
    elif file == 'users_flat.json':
        uri=f'gs://pya_bucket/{n[2]}'
    elif file == 'venues_flat.json':
        uri=f'gs://pya_bucket/{n[3]}'
    load_job = client.load_table_from_uri(
    uri,
    table_id,
    location="EU",  # Must match the destination dataset location.
    job_config=job_config,)  # Make an API request.
    '''

    load_job.result()  # Waits for the job to complete.
    #print(job)

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )"""

def load_bq(file, table_id, load_type):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # job config bq
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=load_type
    )
    # Loading JSON file from local system
    with open(file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.
    print(job)

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def main(file, table_name, src):
    
    table_id = '{}.{}'.format('meetup', table_name)

    print(f'preprocessing {src}')
    preprocess(src, file)
    
    print(f'uploading {file} to gcs: {table_name}')
    upload_blob('pya_bucket', file, table_name)

    print(f'loading to bq: {table_name}')
    load_bq(file, table_id, 'WRITE_TRUNCATE')

if __name__ == "__main__":
    string = '_flat.json'
    string1 = 'data/'
    string2 = '.json'
    list_src_name = ['events','groups','users','venues']
    for name in list_src_name:
        main(name+string, name, string1+name+string2)





