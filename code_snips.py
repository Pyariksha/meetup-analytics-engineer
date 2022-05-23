import csv
from datetime import datetime
from google.cloud import *
from google.cloud import bigquery
import json 
import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df

#export GOOGLE_APPLICATION_CREDENTIALS="sigma-scheduler-348710-0e55acb5c90d.json"

events = pd.read_json('data/events.json')

with open("data/events.json") as file:
    data = json.load(file)

rsvps = pd.json_normalize(data, record_path=['rsvps'], record_prefix = 'rsvps_', meta=['name', 'status', 'time', 'duration', 'group_id', 'created', 'description'])

#events = pd.merge(events,rsvps, on='name')

#events = events.drop(['rsvps'], axis = 1)

rsvps.to_json('events_flat.json', orient='records', lines=True)

'''with open("events_new.json", "r") as read_file:
    data = json.load(read_file)
result = [json.dumps(record) for record in data]
with open('events-processed.json', 'w') as obj:
    for i in result:
        obj.write(i+'\n')'''

'''
groups = pd.read_json('data/groups.json')
events = pd.read_json('data/events.json')
users = pd.read_json('data/users.json')
venues = pd.read_json('data/venues.json')
'''

def preprocess(js, name):
    with open(js) as file:
        data = json.load(file)
    if js == 'data/events.json':
        rsvps = pd.json_normalize(data, record_path=['rsvps'], record_prefix = 'rsvps_', meta=['name', 'status', 'time', 'duration', 'group_id', 'created', 'description'])
        rsvps.to_json(name, orient='records', lines=True)
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

    job.result()  # Waits for the job to complete.
    print(job)

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def main(file, table_name):
    # `<<project-name>>.btd_in3.bse_daily_history`
    table_id = '{}.{}'.format('meetup', table_name)

    #file = preprocess_file(input_filename)
    preprocess("data/events.json", "events_flat.json")
    preprocess("data/groups.json", "groups_flat.json")
    preprocess("data/users.json", "users_flat.json")
    preprocess("data/venues.json", "venues_flat.json")
    load_bq(file, table_id, 'WRITE_TRUNCATE')

#if __name__ == "__main__":
    #string = '_flat.json'
    #list_src_file = ['events_flat.json','groups_flat.json','users_flat.json','venues_flat.json']
    #list_src_name = ['events','groups','users','venues']
    #for name in list_src_name:
        #main(name+string, name)

main("events_flat.json", 'events')
main("groups_flat.json", 'groups')
main("users_flat.json", 'users')
main("venues_flat.json", 'venues')


'''___test code: ignore___'''
#table_id_e = '{}.{}.{}'.format('sigma-scheduler-348710','meetup', 'events')
#table_id_g = '{}.{}.{}'.format('sigma-scheduler-348710','meetup', 'groups')
#table_id_u = '{}.{}.{}'.format('sigma-scheduler-348710','meetup', 'users')
#table_id_v = '{}.{}.{}'.format('sigma-scheduler-348710','meetup', 'venues')

#preprocess("data/events.json", "events_flat.json")
#preprocess("data/groups.json", "groups_flat.json")
#preprocess("data/users.json", "users_flat.json")
#preprocess("data/venues.json", "venues_flat.json")

#upload_blob('pya_bucket', 'events_flat.json', 'events')
#upload_blob('pya_bucket', 'groups_flat.json', 'groups')
#upload_blob('pya_bucket', 'users_flat.json', 'users')
#upload_blob('pya_bucket', 'venues_flat.json', 'venues')

#load_bq("events_flat.json", table_id_e, 'WRITE_TRUNCATE')
#load_bq("groups_flat.json", table_id_g, 'WRITE_TRUNCATE')
#load_bq("users_flat.json", table_id_u, 'WRITE_TRUNCATE')
#load_bq("venues_flat.json", table_id_v, 'WRITE_TRUNCATE')

'''
- name: 'gcr.io/cloud-builders/gcloud'
  args: ['functions', 'deploy', 'upload_blob', '--trigger-topic', 'meetup', '--runtime', 'python39', '--entry-point', 'upload_blob']
- name: 'gcr.io/cloud-builders/gcloud'
  args: ['functions', 'deploy', 'preprocess', '--trigger-topic', 'meetup', '--runtime', 'python39', '--entry-point', 'preprocess']
- name: 'gcr.io/cloud-builders/gcloud'
  args: ['functions', 'deploy', 'load_bq', '--trigger-topic', 'meetup', '--runtime', 'python39', '--entry-point', 'load_bq']
'''


