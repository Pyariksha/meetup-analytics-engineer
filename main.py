from google.cloud import *
from google.cloud import bigquery
import json 
import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df
import os
from google.cloud import storage
from google.cloud.bigquery.client import Client
from google.cloud.storage import Client
from google.oauth2 import service_account
import pandas_gbq

# Path to the service account credentials
'''
SCOPES = ['https://www.googleapis.com/auth/sqlservice.admin']
SERVICE_ACCOUNT_FILE = 'sigma-scheduler-348710-0e55acb5c90d.json'
credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
'''
'''
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'sigma-scheduler-348710-0e55acb5c90d.json'
'''

class Preprocess:
    def __init__(self, bucket_name_read, bucket_name_write):
        self.bucket_name_read = bucket_name_read
        self.bucket_name_write = bucket_name_write
       
    def get_raw(self, raw_path_gcs):
        client = Client()
        bucket_name_read = self.bucket_name_read
        bucket = client.get_bucket(bucket_name_read)
        blob = bucket.get_blob(raw_path_gcs)
        downloaded_json_file = json.loads(blob.download_as_text(encoding="utf-8"))
        return downloaded_json_file

    def transform_raw(self,downloaded_json_file):
        norm_file = pd.json_normalize(downloaded_json_file, record_path=['rsvps'], record_prefix = 'rsvps_', meta=['name', 'status', 'time', 'duration', 'group_id', 'created', 'description'])
        norm_file.to_json('events_norm.json', orient='records', lines=True)
        return norm_file
    
    def write_clean_to_gcs(self,norm_file,csv_name):
        bucket_name_write = self.bucket_name_write
        norm_file.to_csv(f'gs://{bucket_name_write}/{csv_name}', sep=',')
        print(f'Dataframe "norm_file" written as CSV to gcs bucket: {bucket_name_write}')

    def load_to_bq(self, norm_file, schema):
        norm_file=norm_file.applymap(str)
        project_name = 'sigma-scheduler-348710'#gcp project name
        dataset_name = 'meetup'
        table_name = 'events'
        table_id = '{}.{}'.format(dataset_name, table_name)
        #pandas_gbq.to_gbq(y,table_id, project_id=project_name, if_exists='append')#append tweets to table
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            #autodetect=True,
            schema=schema
            ,write_disposition='WRITE_TRUNCATE',)
        job = client.load_table_from_dataframe(
            norm_file, table_id, job_config=job_config
        )
        # Wait for the load job to complete.
        job.result()

schema_events = [
            bigquery.SchemaField("rsvps_guests", "STRING"),
            bigquery.SchemaField("rsvps_when", "STRING"),
            bigquery.SchemaField("rsvps_response", "STRING"),
            bigquery.SchemaField("rsvps_user_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("duration", "STRING"),
            bigquery.SchemaField("group_id", "STRING"),
            bigquery.SchemaField("created", "STRING"),
            bigquery.SchemaField("description", "STRING"),]

schema_groups = [
            bigquery.SchemaField("rsvps_guests", "STRING"),
            bigquery.SchemaField("rsvps_when", "STRING"),
            bigquery.SchemaField("rsvps_response", "STRING"),
            bigquery.SchemaField("rsvps_user_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("duration", "STRING"),
            bigquery.SchemaField("group_id", "STRING"),
            bigquery.SchemaField("created", "STRING"),
            bigquery.SchemaField("description", "STRING"),]
schema_users = [
            bigquery.SchemaField("rsvps_guests", "STRING"),
            bigquery.SchemaField("rsvps_when", "STRING"),
            bigquery.SchemaField("rsvps_response", "STRING"),
            bigquery.SchemaField("rsvps_user_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("duration", "STRING"),
            bigquery.SchemaField("group_id", "STRING"),
            bigquery.SchemaField("created", "STRING"),
            bigquery.SchemaField("description", "STRING"),]
schema_venues = [
            bigquery.SchemaField("rsvps_guests", "STRING"),
            bigquery.SchemaField("rsvps_when", "STRING"),
            bigquery.SchemaField("rsvps_response", "STRING"),
            bigquery.SchemaField("rsvps_user_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("duration", "STRING"),
            bigquery.SchemaField("group_id", "STRING"),
            bigquery.SchemaField("created", "STRING"),
            bigquery.SchemaField("description", "STRING"),]

runClass = Preprocess('pya_bucket1', 'pya_bucket1')
x = runClass.get_raw('raw/events.json')
y = runClass.transform_raw(x)
z = runClass.write_clean_to_gcs(y,'events_norm.csv')
zz = runClass.load_to_bq(y, schema_events)

'''
y=y.applymap(str)
print(y.dtypes)

project_name = 'sigma-scheduler-348710'#gcp project name
dataset_name = 'meetup'
table_name = 'events'
table_id = '{}.{}'.format(dataset_name, table_name)
#pandas_gbq.to_gbq(y,table_id, project_id=project_name, if_exists='append')#append tweets to table
client = bigquery.Client()  
job_config = bigquery.LoadJobConfig(
    #autodetect=True,
    schema=[
    bigquery.SchemaField("rsvps_guests", "STRING"),
    bigquery.SchemaField("rsvps_when", "STRING"),
    bigquery.SchemaField("rsvps_response", "STRING"),
    bigquery.SchemaField("rsvps_user_id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("time", "STRING"),
    bigquery.SchemaField("duration", "STRING"),
    bigquery.SchemaField("group_id", "STRING"),
    bigquery.SchemaField("created", "STRING"),
    bigquery.SchemaField("description", "STRING"),]
    ,write_disposition='WRITE_TRUNCATE',)
job = client.load_table_from_dataframe(
    y, table_id, job_config=job_config
)
# Wait for the load job to complete.
job.result()
'''
""" 
# export GOOGLE_APPLICATION_CREDENTIALS="sigma-scheduler-348710-0e55acb5c90d.json"  
'''
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    #Uploads a file to the bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))
    return blob

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
    job = client.load_table_from_uri(
    uri,
    table_id,
    location="EU",  # Must match the destination dataset location.
    job_config=job_config,)  # Make an API request.
    '''

    job.result()  # Waits for the job to complete.
    print(job)

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
string = '_flat.json'
string1 = 'data/'
string2 = '.json'
list_src_name = ['events','groups','users','venues']

def run_all(file, table_name, src):
    
    table_id = '{}.{}'.format('meetup', table_name)

    print(f'preprocessing {src}')
    preprocess(src, file)
    
    print(f'uploading {file} to gcs: {table_name}')
    upload_blob('pya_bucket', file, table_name)

    print(f'loading to bq: {table_name}')
    load_bq(file, table_id, 'WRITE_TRUNCATE')

if __name__ == "__main__":
    for name in list_src_name:
        run_all(name+string, name, string1+name+string2)
        '''

"""