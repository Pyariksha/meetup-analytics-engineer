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
'''
class Preprocess:
        def __init__(self, bucket_name_read, bucket_name_write):
            self.bucket_name_read = bucket_name_read
            self.bucket_name_write = bucket_name_write'''

def get_raw(raw_path_gcs, bucket_name_read):#self
    client = Client()
    bucket_name_read = bucket_name_read #self
    bucket = client.get_bucket(bucket_name_read)
    blob = bucket.get_blob(raw_path_gcs)
    downloaded_json_file = json.loads(blob.download_as_text(encoding="utf-8"))
    return downloaded_json_file

def transform_raw(downloaded_json_file):#self
    norm_file = pd.json_normalize(downloaded_json_file, record_path=['rsvps'], record_prefix = 'rsvps_', meta=['name', 'status', 'time', 'duration', 'group_id', 'created', 'description'])
    norm_file.to_json('events_norm.json', orient='records', lines=True)
    return norm_file

def write_clean_to_gcs(norm_file,csv_name, bucket_name_write):#self
    bucket_name_write = bucket_name_write #self
    norm_file.to_csv('gs://{}/{}'.format(bucket_name_write,csv_name), sep=',')
    print('Dataframe "norm_file" written as CSV to gcs bucket: {}'.format(bucket_name_write))

def load_to_bq(norm_file, schema):
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

def main():
    bucket_name_read = 'pya_bucket1'
    bucket_name_write = 'pya_bucket1'
    x = get_raw('raw/events.json', bucket_name_read)#runClass.
    y = transform_raw(x)#runClass.
    write_clean_to_gcs(y,'events_norm.csv', bucket_name_write)#runClass.
    load_to_bq(y, schema_events)#runClass.


if __name__ == '__main__':
    #runClass = Preprocess('pya_bucket1', 'pya_bucket1')
    main()

'''
def classname(obj):
    cls = type(obj)
    module = cls.__module__
    name = cls.__qualname__
    if module is not None and module != "__builtin__":
        name = module + "." + name
    return name

print(classname(Preprocess))'''