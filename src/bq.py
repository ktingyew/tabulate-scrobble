import logging
import os

from google.cloud import bigquery as bq
import pandas as pd

PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = os.environ['DATASET_ID']
TABLE_ID = os.environ['TABLE_ID']

# Construct uri string to BigQuery table
TABLE_REF_STR: str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Authenticate with BigQuery
bq_client = bq.Client() # TODO: use production credentials

logger = logging.getLogger("main.bq")

fields = "Title:STRING,Artist:STRING,Album:STRING,Datetime:STRING," + \
    "Title_c:STRING,Artist_c:STRING,Datetime_n:DATETIME"

schema = [
    bq.SchemaField(
        name=f.split(':')[0], 
        field_type=f.split(':')[1], 
        mode='NULLABLE'
    ) 
    for f in fields.split(',')
]

def replace_bq_table(
    df: pd.DataFrame
) -> None :

    # Modify DateAdded type from "object" to "DateTime"
    # schema in bq is DATETIME; requires this to be in pd's datetime format
    df['Datetime_n'] = pd.to_datetime(df['Datetime_n'], format="%Y-%m-%d %H:%M:%S") 

    bq_client.load_table_from_dataframe(
        dataframe=df, 
        destination=TABLE_REF_STR,
        job_config=bq.job.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE")
    )
    logger.info(f"load_table_from_dataframe to {TABLE_REF_STR} successful")

