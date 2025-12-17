# Data/Validation.py
import polars as pl
from pydantic import BaseModel, ValidationError
from typing import Optional, Any, List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define what a runner record should look like
class RunnerRecord(BaseModel):
    Year: int
    Race: str
    Name: str
    Gender: Optional[str] = None
    Age: Optional[int] = None
    State: Optional[str] = None
    Country: Optional[str] = None
    Overall: Optional[int] = None
    Finish_Time: Optional[str] = None
    finish_seconds: Optional[int] = None
    Finish: Optional[int] = None

# Turn a HH:MM:SS time string into seconds
def time_to_seconds(t: str) -> Optional[int]:
    try:
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s
    except:
        # If it's not a valid time, just return None
        return None

# Validate a single row of data
def validate_row_dict(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Make sure empty fields are set to "None"
    for k, v in row.items():
        if v == "" or v is None:
            row[k] = "None"
    if row.get("State") == "-0":
        row["State"] = "None"
    # Convert finish time to seconds if it's available
    if row.get("Finish_Time") and row["Finish_Time"] != "None":
        row["finish_seconds"] = time_to_seconds(row["Finish_Time"])
    else:
        row["finish_seconds"] = "None"
    try:
        validated: RunnerRecord = RunnerRecord(**row)
        return validated.model_dump()
    except ValidationError as e:
        # Just warn if something doesn't match the schema
        logger.warning(f"Validation failed for row: {row.get('Name', 'Unknown')} | {e}")
        return None

# Validate a full CSV file
def validate_csv(input_file: str, output_file: str) -> pl.DataFrame:
    logger.info(f"Loading CSV: {input_file}")
    df: pl.DataFrame = pl.read_csv(input_file)
    # Make sure Finish Time column has a consistent name
    if "Finish Time" in df.columns:
        df = df.rename({"Finish Time": "Finish_Time"})
    logger.info("Running validation on each row...")
    rows: List[Dict[str, Any]] = df.to_dicts()
    validated: List[Dict[str, Any]] = [
        r for r in (validate_row_dict(r) for r in rows) if r is not None
    ]
    df_validated: pl.DataFrame = pl.DataFrame(validated)
    # Keep columns in a consistent order
    columns_order = ["Year","Race","Name","Gender","Age","State","Country",
                     "Overall","Finish_Time","finish_seconds","Finish"]
    df_validated = df_validated.select(columns_order)
    # Replace any leftover nulls with "None"
    df_validated = df_validated.fill_null("None")
    logger.info(f"Saving cleaned CSV to: {output_file}")
    df_validated.write_csv(output_file)
    logger.info("All done! CSV validation finished.")
    return df_validated

# If this script is run directly, validate the CSV
if __name__ == "__main__":
    input_file = r"C:\Users\mvoen\OneDrive\Desktop\BigDataFinal\Data\NYC_Marathon_Results.csv"
    output_file = r"C:\Users\mvoen\OneDrive\Desktop\BigDataFinal\Data\Final_Clean_Data_NYC_validated.csv"
    validate_csv(input_file, output_file)
