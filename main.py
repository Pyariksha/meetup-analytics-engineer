
from datetime import datetime
from google.cloud import *
from google.cloud import bigquery
import json 
import pandas as pd 
from pandas.io.json import json_normalize #package for flattening json in pandas df

# export GOOGLE_APPLICATION_CREDENTIALS="sigma-scheduler-348710-0e55acb5c90d.json"

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

    job.result()  # Waits for the job to complete.
    print(job)

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

#def main(file, table_name):
    # `<<project-name>>.btd_in3.bse_daily_history`
    #table_id = '{}.{}'.format('meetup', table_name)
table_id = '{}.{}'.format('meetup', 'events')
table_id = '{}.{}'.format('meetup', 'groups')
table_id = '{}.{}'.format('meetup', 'users')
table_id = '{}.{}'.format('meetup', 'venues')

#file = preprocess_file(input_filename)
preprocess("data/events.json", "events_flat.json")
preprocess("data/groups.json", "groups_flat.json")
preprocess("data/users.json", "users_flat.json")
preprocess("data/venues.json", "venues_flat.json")
#load_bq(file, table_id, 'WRITE_TRUNCATE')
load_bq("events_flat.json", table_id, 'WRITE_TRUNCATE')
load_bq("groups_flat.json", table_id, 'WRITE_TRUNCATE')
load_bq("users_flat.json", table_id, 'WRITE_TRUNCATE')
load_bq("venues_flat.json", table_id, 'WRITE_TRUNCATE')

#if __name__ == "__main__":
    #string = '_flat.json'
    #list_src_file = ['events_flat.json','groups_flat.json','users_flat.json','venues_flat.json']
    #list_src_name = ['events','groups','users','venues']
    #for name in list_src_name:
        #main(name+string, name)

#main("events_flat.json", 'events')
#main("groups_flat.json", 'groups')
#main("users_flat.json", 'users')
#main("venues_flat.json", 'venues')