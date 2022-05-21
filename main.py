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


#events = events.drop(['rsvps'], axis = 1)

rsvps.to_json('events_flat.json', orient='records', lines=True)

'''with open("events_new.json", "r") as read_file:
    data = json.load(read_file)
result = [json.dumps(record) for record in data]
with open('events-processed.json', 'w') as obj:
    for i in result:
        obj.write(i+'\n')'''

def load_bq(file, table_id, load_type):

    '''with open(file) as f:
        all_cols_from_file = f.readline()
    all_cols_from_file=all_cols_from_file.rstrip('\n').upper().split(',')
    print(all_cols_from_file)'''

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Post-2014
    #schema = [{'name': 'ts', 'type': 'DATE', 'mode': 'REQUIRED'}, {'name': 'sc_code', 'type': 'INTEGER', 'mode': 'REQUIRED'}, {'name': 'sc_name', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'sc_group', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'sc_type', 'type': 'STRING', 'mode': 'REQUIRED'}, {'name': 'open', 'type': 'NUMERIC', 'mode': 'REQUIRED'}, {'name': 'high', 'type': 'NUMERIC', 'mode': 'REQUIRED'}, {'name': 'low', 'type': 'NUMERIC', 'mode': 'REQUIRED'}, {'name': 'close', 'type': 'NUMERIC', 'mode': 'REQUIRED'}, {'name': 'last', 'type': 'NUMERIC', 'mode': 'REQUIRED'}, {'name': 'prevclose', 'type': 'NUMERIC', 'mode': 'REQUIRED'}, {'name': 'no_trades', 'type': 'INTEGER', 'mode': 'REQUIRED'}, {'name': 'NO_OF_SHRS', 'type': 'INTEGER', 'mode': 'REQUIRED'}, {'name': 'NET_TURNOVER', 'type': 'NUMERIC', 'mode': 'REQUIRED'}, {'name': 'TDCLOINDI', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'ISIN', 'type': 'STRING', 'mode': 'NULLABLE'}]

    job_config = bigquery.LoadJobConfig(autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        #schema=schema,
        write_disposition=load_type
    )

    # Loading CSV file from local system
    with open(file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    # Loading CSV file from GCS
    # uri = "gs://<<bucket-name>>/bse/bhavcopy_csv/EQ_ISINCODE_280921.CSV"
    # job = client.load_table_from_uri(uri, table_id, job_config=job_config) 

    job.result()  # Waits for the job to complete.
    print(job)

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def main():

    file = 'events_flat.json'
    # `<<project-name>>.btd_in3.bse_daily_history`
    table_name = 'events'
    table_id = '{}.{}'.format('meetup', table_name)

    #file = preprocess_file(input_filename)
    load_bq(file, table_id, 'WRITE_TRUNCATE')


if __name__ == "__main__":
    main()