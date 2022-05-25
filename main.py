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
#Initially used a class but unable to load into gcp cloud functions
class Preprocess:
        def __init__(self, bucket_name_read, bucket_name_write):
            self.bucket_name_read = bucket_name_read
            self.bucket_name_write = bucket_name_write'''

def get_raw(raw_path_gcs, bucket_name_read):
    '''
    This function gets raw json files that still contain repeated data into an object variable.
    '''
    client = Client()
    bucket_name_read = bucket_name_read
    bucket = client.get_bucket(bucket_name_read)
    blob = bucket.get_blob(raw_path_gcs)
    downloaded_json_file = json.loads(blob.download_as_text(encoding="utf-8"))
    print('1. Downloaded json object')
    return downloaded_json_file

def transform_raw(downloaded_json_file, name):
    '''
    This function transforms the downloaded json files by normalizing them.
    Note that the json files were saved locally as back ups.
    '''
    try:
        if name == "events":
            norm_file = pd.json_normalize(downloaded_json_file, record_path=['rsvps'], record_prefix = 'rsvps_', meta=['name', 'status', 'time', 'duration', 'group_id', 'created', 'description'])
            #norm_file.to_json('events_norm.json', orient='records', lines=True)
            print('2. Events normalized')
            return norm_file
        elif name == "groups":
            norm_file = pd.json_normalize(downloaded_json_file)
            #norm_file.to_json('groups_norm.json', orient='records', lines=True)
            print('2. Groups normalized')
            return norm_file
        elif name == "users":
            norm_file = pd.json_normalize(downloaded_json_file, record_path=['memberships'], record_prefix='memberships_', meta=['user_id', 'hometown', 'country', 'city'])
            #norm_file.to_json('users_norm.json', orient='records', lines=True)
            print('2. Users normalized')
            return norm_file
        elif name == "venues":
            norm_file = pd.json_normalize(downloaded_json_file)
            #norm_file.to_json('venues_norm.json', orient='records', lines=True)
            print('2. Venues normalized')
            return norm_file
    except:
        raise Exception('Failure in transform')

def write_clean_to_gcs(norm_file,csv_name, bucket_name_write):
    '''
    This function writes the clean data to gcs as a csv.
    This serves as a untouched source that is available outside of the database.
    '''
    bucket_name_write = bucket_name_write
    norm_file.to_csv('gs://{}/tmp/{}'.format(bucket_name_write,csv_name), sep=',')
    print('3. Dataframe "norm_file" written as CSV to gcs bucket: {}'.format(bucket_name_write))

def load_to_bq(norm_file, schema, name):
    '''
    This function loads the pandas dfs to big query tables.
    Note that all data types are string for simplicity of loading. This can be changed in the databases.
    '''
    norm_file=norm_file.applymap(str)
    project_name = 'sigma-scheduler-348710'# gcp project name
    dataset_name = 'meetup'
    table_name = name
    table_id = '{}.{}'.format(dataset_name, table_name)
    #pandas_gbq.to_gbq(y,table_id, project_id=project_name, if_exists='append')#append tweets to table
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        #autodetect=True, # This caused errors when big query tried to write file to parquet before loading
        schema=schema
        ,write_disposition='WRITE_TRUNCATE',) # Truncate to clear all rows and replace
    job = client.load_table_from_dataframe(
        norm_file, table_id, job_config=job_config
    )
    # Wait for the load job to complete.
    job.result()
    table = client.get_table(table_id)  # Make an API request.
    print("4. Loaded {} rows and {} columns to {} \n".format(table.num_rows, len(table.schema), table_id))

#schemas are explicitly saved in variables (avoids pyarrow errors when loading into bq)
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
            bigquery.SchemaField("group_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("created", "STRING"),
            bigquery.SchemaField("lat", "STRING"),
            bigquery.SchemaField("lon", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("link", "STRING"),
            bigquery.SchemaField("topics", "STRING"),]
schema_users = [
            bigquery.SchemaField("user_id", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("hometown", "STRING"),
            bigquery.SchemaField("memberships_group_id", "STRING"),
            bigquery.SchemaField("memberships_joined", "STRING"),]
schema_venues = [
            bigquery.SchemaField("venue_id", "STRING"),
            bigquery.SchemaField("lat", "STRING"),
            bigquery.SchemaField("lon", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("city", "STRING"),]

def main(data, context):
    '''
    This function runs the complete script for preprocessing the data files. 
    This function is included in the cloudbuild.yaml file for gcp.
    '''
    data = data
    context = context
    bucket_name_read = 'pya_bucket1'
    bucket_name_write = 'pya_bucket1'
    string1 = 'raw/'
    string2 = '.json'
    string3 = '.csv'
    list_src_name = ['events','groups','users','venues']
    for n in list_src_name:
        name = n
        print('Starting:',name)
        x = get_raw(string1+n+string2, bucket_name_read)
        y = transform_raw(x, name)
        write_clean_to_gcs(y,n+string3,bucket_name_write)
        if name == 'events':
            load_to_bq(y, schema_events, name)
        elif name == 'groups':
            load_to_bq(y, schema_groups, name)
        elif name == 'users':
            load_to_bq(y, schema_users, name)
        elif name == 'venues':
            load_to_bq(y, schema_venues, name)

if __name__ == '__main__':
    #ensures that we run the function if in main file
    main('data', 'context')