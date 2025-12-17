import pandas as pd
from opensearchpy import OpenSearch, helpers
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CSV_FILE: str = "Data/Final_Clean_Data_NYC_validated.csv"
INDEX_NAME: str = "nyc_marathon"

logger.info("Loading CSV...")
df: pd.DataFrame = pd.read_csv(CSV_FILE)
df = df.where(pd.notnull(df), None)  # Replace NaN with None for JSON

logger.info("Connecting to OpenSearch...")
client: OpenSearch = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "admin123")
)

if not client.indices.exists(index=INDEX_NAME):
    logger.info(f"Creating index: {INDEX_NAME}")
    client.indices.create(index=INDEX_NAME)

actions: List[Dict[str, Any]] = [
    {"_index": INDEX_NAME, "_source": record}
    for record in df.to_dict(orient="records")
]

logger.info(f"Starting bulk indexing of {len(actions)} records...")
success: int = 0
failed: int = 0

for ok, item in helpers.streaming_bulk(client, actions):
    if ok:
        success += 1
    else:
        failed += 1

logger.info(f"Bulk indexing complete: {success} succeeded, {failed} failed.")
