# clean_rerun.py
import sys
import subprocess
from opensearchpy import OpenSearch, helpers, NotFoundError
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

python_exe = sys.executable  # use the current Python from the virtual environment

raw_index = "nyc_marathon_raw"
agg_index = "nyc_marathon_aggregates"

# Connect to OpenSearch
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "admin123")
)

# Delete old indices if they exist
for idx in [raw_index, agg_index]:
    try:
        client.indices.delete(index=idx)
        logger.info(f"Deleted index: {idx}")
    except NotFoundError:
        logger.info(f"Index {idx} does not exist, skipping deletion.")
    except Exception as e:
        logger.error(f"Error deleting index {idx}: {e}")

# Recreate raw index
try:
    client.indices.create(index=raw_index)
    logger.info(f"Created index: {raw_index}")
except Exception as e:
    logger.error(f"Error creating index {raw_index}: {e}")

# Load CSV and clean data
csv_file = r"C:\Users\mvoen\OneDrive\Desktop\BigDataFinal\Data\Final_Clean_Data_NYC_validated.csv"
df = pd.read_csv(csv_file)

# Replace NaN with "None" for string fields
df = df.fillna("None")

# Make sure numeric columns are integers
for col in ["Year", "Age", "Overall", "finish_seconds", "Finish"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

# Make column names safe for OpenSearch
df.columns = [c.replace(" ", "_").replace(".", "_") for c in df.columns]

# Batch ingestion with progress logging
batch_size = 5000
logger.info(f"Starting ingestion of {len(df)} rows into {raw_index} in batches of {batch_size}...")

for i in range(0, len(df), batch_size):
    batch = df.iloc[i:i+batch_size]
    actions = [{"_index": raw_index, "_source": row.to_dict()} for _, row in batch.iterrows()]
    success, errors = helpers.bulk(client, actions, raise_on_error=False)
    if errors:
        logger.warning(f"{len(errors)} documents failed in batch {i // batch_size + 1}")
    logger.info(f"Inserted {i + len(batch)} / {len(df)} rows")

logger.info(f"Completed ingestion of {len(df)} rows into {raw_index}")

# Check for duplicates
logger.info("Checking for duplicates...")
docs = helpers.scan(
    client,
    index=raw_index,
    query={"query": {"match_all": {}}},
    _source_includes=["Name", "Year", "Finish_Time"]
)

seen = set()
to_delete = []
count = 0

for doc in docs:
    source = doc["_source"]
    key = (source.get("Name"), source.get("Year"), source.get("Finish_Time"))
    if key in seen:
        to_delete.append({"_op_type": "delete", "_index": raw_index, "_id": doc["_id"]})
    else:
        seen.add(key)
    count += 1
    if count % 10000 == 0:
        logger.info(f"Checked {count} documents for duplicates...")

if to_delete:
    success, errors = helpers.bulk(client, to_delete, raise_on_error=False)
    if errors:
        logger.warning(f"{len(errors)} duplicates failed to delete")
    logger.info(f"Deleted {len(to_delete)} duplicate documents.")
else:
    logger.info("No duplicates found.")

# Run daily ETL scripts
daily_scripts = [r"Data\Validation.py", r"Data\load_to_opensearch.py"]
logger.info("Running Daily ETL pipeline...")

for script in daily_scripts:
    logger.info(f"Running script: {script}")
    try:
        subprocess.run([python_exe, script], check=True)
        logger.info(f"Finished script: {script}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script}")
        logger.error(f"Exit code: {e.returncode}")

logger.info("Daily ETL pipeline finished.")

# Print final row counts
try:
    raw_count = client.count(index=raw_index)['count']
except Exception:
    raw_count = 0

try:
    agg_count = client.count(index=agg_index)['count']
except Exception:
    agg_count = 0

logger.info(f"Final Raw index row count: {raw_count}")
logger.info(f"Final Aggregated index row count: {agg_count}")
