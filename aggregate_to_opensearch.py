# aggregate_to_opensearch.py
import polars as pl
from opensearchpy import OpenSearch, helpers
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INDEX_RAW = "nyc_marathon"
INDEX_AGG = "nyc_marathon_aggregates"

# ----------------------------
# OpenSearch client
# ----------------------------
client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "admin123")
)

# ----------------------------
# Load CSV
# ----------------------------
logger.info("Loading validated CSV for aggregation...")
df = pl.read_csv("Data/Final_Clean_Data_NYC_validated.csv")

# Normalize column names to lowercase
df = df.rename({col: col.lower() for col in df.columns})

# Ensure required columns exist and cast to float
for col in ["finish_seconds", "overall", "year"]:
    if col not in df.columns:
        df = df.with_columns([pl.Series(col, [None] * df.height)])
    df = df.with_columns([df[col].cast(pl.Float64)])

logger.info("Computing aggregations...")

# ----------------------------
# Aggregations (overall only)
# ----------------------------

# Finishers count per year
finishers_count = df.group_by("year").agg(pl.count().alias("finishers_count"))

# Avg finish time of top 100 overall
df_100th = df.filter(pl.col("overall") <= 100)
avg_100th_time = df_100th.group_by("year").agg(
    pl.mean("finish_seconds").alias("avg_100th_place_time")
)

# Total runners per year
total_runners = df.group_by("year").agg(pl.count().alias("total_runners"))

# Winning time per year (minimum finish_seconds)
winning_time = df.group_by("year").agg(pl.min("finish_seconds").alias("winning_time"))

# ----------------------------
# Join all aggregations
# ----------------------------
agg_df = (
    finishers_count
    .join(avg_100th_time, on="year", how="left")
    .join(total_runners, on="year", how="left")
    .join(winning_time, on="year", how="left")
)

# Prepare records for OpenSearch
# Prepare records for OpenSearch with _index
records = [
    {**{k: (v if v is not None else None) for k, v in row.items()}, "_index": INDEX_AGG}
    for row in agg_df.to_dicts()
]

# ----------------------------
# Upload to OpenSearch
# ----------------------------
if client.indices.exists(index=INDEX_AGG):
    client.indices.delete(index=INDEX_AGG)
client.indices.create(index=INDEX_AGG)

success, failed = 0, 0
for ok, item in helpers.streaming_bulk(client, records):
    if ok:
        success += 1
    else:
        failed += 1

logger.info(f"Aggregations indexed: {success} succeeded, {failed} failed.")
logger.info("Done ✔")
