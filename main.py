from collections import namedtuple
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import sys

import pandas as pd
import requests

from src.bq import replace_bq_table
from src.date_utils import find_file_with_latest_dt_in_dir

LOG_DIR = Path(os.environ['LOGS_TARGET'])
SCROBBLE_DIR = Path(os.environ['SCROBBLE_TARGET'])
MAPPER_FPATH = Path(os.environ['MAPPER_TARGET'])

PAGE_RETRIEVE_COUNT = os.environ['PAGE_RETRIEVE_COUNT']
LASTFM_USERNAME = os.environ['LASTFM_USERNAME']
LASTFM_API_KEY = os.environ['LASTFM_API_KEY']

logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)
fmtter = logging.Formatter(
    "[%(asctime)s]; %(levelname)8s; %(name)20s; %(message)s", 
    "%Y-%m-%d %H:%M:%S")
file_handler = logging.FileHandler(LOG_DIR/"scrobble.log", encoding='utf8')
file_handler.setFormatter(fmtter)
logger.addHandler(file_handler)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(fmtter)
logger.addHandler(stdout_handler)


logger.info("STARTED: =========================================")

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

# Replace scrobbles table in BigQuery
replace_bq_table(df=out)

logger.info("COMPLETED: =======================================")

