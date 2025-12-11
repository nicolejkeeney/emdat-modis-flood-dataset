"""
split_emdat_ids_into_batches.py

This script reads a CSV file containing EM-DAT flood event records and splits the unique
event IDs (from a specified column) into multiple plain-text files for batch processing.

Each output file contains a fixed number of event IDs (defined by NUM_IDS_PER_FILE),
with one ID per line. This is useful for parallelizing flood detection jobs.

"""

import pandas as pd
import os
import math
import sys

sys.path.append("../")  # Add one directory up, so you can import the utils module
from utils.utils_misc import check_file_exists


DATA_DIR = "../data/"
EMDAT_CSV = f"{DATA_DIR}emdat_floods_by_mon_yr_adm1.csv"

# Name of the column to generate unique IDs from
# Must be a column in EMDAT_CSV
COLUMN = "mon-yr-adm1-id"
# COLUMN = "id"

# How many IDs to have per file
NUM_IDS_PER_FILE = 2650

# Define output dir from column name
OUTPUT_DIR = f"{DATA_DIR}emdat_{COLUMN.replace('-', '_')}/"


def split_csv_column_to_txt_batches(
    input_csv,
    column="mon-yr-adm1-id",
    batch_size=500,
    output_prefix=f"emdat_{COLUMN.replace('-', '_')}",
    output_dir=OUTPUT_DIR,
):
    # Load the CSV and drop any missing values in the target column
    df = pd.read_csv(input_csv)
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in {input_csv}")

    # Get unique, non-nan ID values
    ids = df[column].dropna().unique()

    # Calculate number of batches
    num_batches = math.ceil(len(ids) / batch_size)

    for i in range(num_batches):
        # Get the slice of IDs for this batch (e.g., 0–499, 500–999, etc.)
        batch_ids = ids[i * batch_size : (i + 1) * batch_size]

        # Construct the output file name (e.g., flood_id_1.txt, flood_id_2.txt, ...)
        output_filename = os.path.join(output_dir, f"{output_prefix}_{i+1}.txt")

        # Write each ID to a new line in the output file
        with open(output_filename, "w") as f:
            f.write("\n".join(batch_ids))

        # Print a status message showing how many IDs were written and where
        print(f"Wrote {len(batch_ids)} IDs to {output_filename}")


# Example usage
if __name__ == "__main__":
    # Make sure paths exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    check_file_exists(EMDAT_CSV)

    # Generate txt batches
    print(
        f"Generating text files with unique ids with {NUM_IDS_PER_FILE} ids per file."
    )
    split_csv_column_to_txt_batches(
        EMDAT_CSV, column=COLUMN, batch_size=NUM_IDS_PER_FILE
    )
    print("Script complete")
