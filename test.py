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



