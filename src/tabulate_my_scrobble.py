from collections import namedtuple
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import sys

from google.cloud import bigquery as bq
import pandas as pd
import requests

# Allow import from parent folder
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from utils import find_file_with_latest_dt_in_dir

LOG_DIR = Path(os.environ['LOGS_TARGET'])
SCROBBLE_DIR = Path(os.environ['SCROBBLE_TARGET'])
MAPPER_FPATH = Path(os.environ['MAPPER_TARGET'])

PAGE_RETRIEVE_COUNT = os.environ['PAGE_RETRIEVE_COUNT']
LASTFM_USERNAME = os.environ['LASTFM_USERNAME']
LASTFM_API_KEY = os.environ['LASTFM_API_KEY']

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fmtter = logging.Formatter(
    "[%(asctime)s]; %(levelname)s; %(name)s; %(message)s", 
    "%Y-%m-%d %H:%M:%S")
file_handler = logging.FileHandler(LOG_DIR/"scrobble.log", encoding='utf8')
file_handler.setFormatter(fmtter)
logger.addHandler(file_handler)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(fmtter)
logger.addHandler(stdout_handler)


logger.info("STARTED: =========================================")

# Authenticate with BigQuery
bq_client = bq.Client()
logger.info(f"BigQuery. Successfully authenticated")

pages = list(range(1, int(PAGE_RETRIEVE_COUNT)+1))
page_ls = []
for p in pages: 
    payload = {
        'limit': 200,
        'method': 'user.getrecenttracks', 
        'page': p,
        'user': LASTFM_USERNAME,
        'api_key': LASTFM_API_KEY,
        'format': 'json'
    }
    r = requests.get(
        'https://ws.audioscrobbler.com/2.0/', 
        params=payload
    )
    page_ls.append(r)
    
    if r.status_code != 200:
        raise ConnectionError(
            f"Status Code of {r.status_code} from {r.url}. Aborting.")
logger.info(
    f"Last.fm: Retrieval of JSON object from last.fm " 
    + "API successful"
)

Scrobble = namedtuple("Scrobble", "Title Artist Album Datetime")
records = []
for page in page_ls:
    pg = page.json()['recenttracks']['track']
    for i in pg:
        try:
            records.append(
                Scrobble(
                    i['name'], 
                    i['artist']['#text'], 
                    i['album']['#text'], 
                    i['date']['#text']
                )
            )
        except KeyError:
            records.append(
                Scrobble(
                    i['name'], 
                    i['artist']['#text'],
                    i['album']['#text'], 
                    None
                )
            )

# Drop entries with missing dt
new = pd.DataFrame(data=records) \
    .dropna(axis=0, subset=['Datetime']) \
    .reset_index(drop=True) 

# Adjust datetime of scrobbles 8-hours ahead (to SGT)
dt_formatter = lambda x : (
    datetime.strptime(x, "%d %b %Y, %H:%M") \
    + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"
)
new['Datetime_n'] = new['Datetime'].apply(dt_formatter)



# FROM LOCAL: Load the most recent comprehensive scrobble dataset; 
# the one where we will be adding the new scrobbles   
fpath = find_file_with_latest_dt_in_dir(directory=SCROBBLE_DIR)
old = pd.read_json(
    fpath, 
    orient='records', 
    convert_dates=False, 
    lines=True) # also pandas by default convert the string format of 
                # datetime to their datetime format, which is annoying.
logger.info(f"Local scrobble: Old file detected (n={len(old)}): {fpath}")

# Concatenate old and new scrobbles together. Duplicated scrobbles (overlaps
# in new) are filtered away first. Then we map the remaining scrobbles.

# Filter
most_rct_datetime_from_old = old.iloc[0]['Datetime_n'] 
idx = new[new['Datetime_n'] == most_rct_datetime_from_old].index.tolist()[0]
new = new.iloc[:idx].copy()

# Load mapper as df, and apply
mapper = pd.read_csv(f"{MAPPER_FPATH}", sep='\t') # tsv file
for i in range(len(new)):
    title, artist = new.iloc[i]['Title'], new.iloc[i]['Artist'] 
    # attempt to look for "correct answer" in `mapper` by generating 
    # filtered df
    ans_df = mapper[
        (mapper['Artist_s'] == artist) 
        & (mapper['Title_s'] == title)
    ] 
    if len(ans_df) == 1: # there is an answer
        new.at[i, 'Title_c'] = ans_df.values.tolist()[0][2]
        new.at[i, 'Artist_c'] = ans_df.values.tolist()[0][3]
    else: # we populate the field with easy to find tags
        new.at[i, 'Title_c'] = "XXxXX"
        new.at[i, 'Artist_c'] =  "XXxXX" 

# Concatenate to `out`
out = pd.concat([new, old], ignore_index=True)


 # SAVE SCROBBLES =========================================

# Export to host (local)
# Name with datetime (adjusted) of latest scrobble
fname = "scrobbles " + \
        datetime.strptime(
            out.iloc[0]['Datetime_n'], 
            "%Y-%m-%d %H:%M:%S"
        ).strftime("%Y-%m-%d %H-%M-%S") + \
        ".jsonl" 
fpath = f"{SCROBBLE_DIR}/{fname}"

# save df as json newline delimited (.jsonl) with utf-8 encoding
with open(fpath, 'w', encoding='utf-8') as fh:
    out.to_json(fh, force_ascii=False, orient='records', lines=True) 
    logger.info(
        f"Scrobble: Successfully saved (n={len(out)}): {fpath}")

# Replace scrobbles in BigQuery

PROJECT_ID = os.environ['PROJECT_ID']
DATASET_LATEST_ID = os.environ['DATASET_LATEST_ID']
TABLE_SCROB_ID = os.environ['TABLE_SCROB_ID']

ds_ref = bq.dataset.DatasetReference(PROJECT_ID, DATASET_LATEST_ID) 
tbl_ref = bq.table.TableReference(ds_ref, TABLE_SCROB_ID) 

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

# initialise table with schema using its (tbl) ref
tbl = bq.Table(tbl_ref, schema=schema) 
bq_client.delete_table(tbl, not_found_ok=True) # Truncate the table
# set optional parameter exists_ok=True to ignore error of table 
# already existing
bq_client.create_table(tbl) 

# schema in bq is DATETIME; requires this to be in pd's datetime format
out['Datetime_n'] = pd.to_datetime(out['Datetime_n']) 
bq_client.load_table_from_dataframe(out, tbl_ref)
logger.info(f"load_table_from_dataframe to {tbl_ref} successful")

logger.info("COMPLETED: =======================================")

