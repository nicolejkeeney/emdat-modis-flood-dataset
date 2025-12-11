"""
Preprocess and disaggregate EM-DAT flood events.

This script preprocesses EM-DAT flood data and disaggregates events by
administrative zone (admin1) and month. Multi-month and multi-region events
are split into individual admin1-month records.

Inputs
------
- emdat-2000-2024.csv : EM-DAT disaster records, manually preprocessed
- GAUL_2015/g2015_2014_2 : GAUL Level 2 administrative boundaries shapefile

Outputs
-------
- emdat_floods_by_mon_yr_adm1.csv : Disaggregated flood events by admin1 and month

Examples
--------
$ python disaggregate_emdat.py

"""

import pandas as pd
import numpy as np
import inspect
import geopandas as gpd
from utils.emdat_toolbox import (
    expand_admin_units,
    split_event_by_month,
    add_event_dates,
)
from utils.utils_misc import check_dir_exists, check_file_exists

DATA_DIR = "../data/"
EMDAT_FILEPATH = f"{DATA_DIR}emdat_2000_2024.csv"
GAUL_L2_FILEPATH = f"{DATA_DIR}g2015_2014_2/"
OUTPUT_FILEPATH = f"{DATA_DIR}emdat_floods_by_mon_yr_adm1.csv"


def read_gaul_shapefile(gaul_filepath):
    """
    Read GAUL level 2 shapefile and remove duplicates.

    Parameters
    ----------
    gaul_filepath : str
        Path to GAUL level 2 shapefile directory.

    Returns
    -------
    geopandas.GeoDataFrame
        GAUL level 2 GeoDataFrame.
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    gaul_l2 = gpd.read_file(gaul_filepath)

    # Only 5 admin 2 codes are duplicates
    # Drop them and keep the first value
    gaul_l2.drop_duplicates(subset=["ADM2_CODE"], keep="first", inplace=True)

    print(f"{inspect.currentframe().f_code.co_name}: Completed successfully")

    return gaul_l2


def expand_admin_zones(emdat_df, gaul_l2):
    """
    Match EM-DAT data with GAUL admin codes and deduplicate by admin1 zone.

    Parameters
    ----------
    emdat_df : pandas.DataFrame
        EM-DAT flood events DataFrame.
    gaul_l2 : geopandas.GeoDataFrame
        GAUL level 2 GeoDataFrame.

    Returns
    -------
    pandas.DataFrame
        DataFrame with admin1 codes matched and duplicate rows removed.
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    expanded_data = []
    for _, row in emdat_df.iterrows():
        static_columns = [col for col in emdat_df.columns if col not in ["Admin Units"]]
        expanded_data.extend(expand_admin_units(row, static_columns))

    emdat_df = pd.DataFrame(expanded_data)

    # Merge GAUL info based on Admin2 code
    emdat_df = pd.merge(
        emdat_df,
        gaul_l2[["ADM2_CODE", "ADM1_CODE", "ADM1_NAME"]],
        how="left",
        left_on=["adm2_code"],
        right_on=["ADM2_CODE"],
    )

    # Combine GAUL columns with EM-DAT data
    emdat_df["adm1_code"] = emdat_df["adm1_code"].combine_first(emdat_df["ADM1_CODE"])
    emdat_df["adm1_name"] = emdat_df["adm1_name"].combine_first(emdat_df["ADM1_NAME"])
    emdat_df = emdat_df.drop(["ADM2_CODE", "ADM1_CODE", "ADM1_NAME"], axis=1)

    # Drop rows with missing data
    emdat_df = emdat_df.dropna(subset=["adm1_code"])

    # Drop duplicates at the admin1 level
    emdat_df = emdat_df.drop_duplicates(subset=["adm1_code", "id"])

    print(f"{inspect.currentframe().f_code.co_name}: Completed successfully")

    return emdat_df


def add_monthly_rows(emdat_df):
    """
    Split disaster events across month boundaries into one row per month.

    Parameters
    ----------
    emdat_df : pandas.DataFrame
        DataFrame with event start and end dates.

    Returns
    -------
    pandas.DataFrame
        Expanded DataFrame with one row per month spanned by each event.
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    df = pd.concat(
        [split_event_by_month(row) for _, row in emdat_df.iterrows()], ignore_index=True
    )

    print(f"{inspect.currentframe().f_code.co_name}: Completed successfully")

    return df


def append_adm1_code_to_id(df):
    """
    Appends the administrative level 1 code to the 'mon-yr-id' column to create a unique
    monthly identifier per admin1 zone, and stores the result in a new column
    called 'mon-yr-adm1-id'.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing at least the 'mon-yr-id' and 'adm1_code' columns.

    Returns
    -------
    pandas.DataFrame
        The modified DataFrame with a new 'mon-yr-adm1-id' column added.
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    def _make_mon_yr_adm1_id(row):
        if pd.notna(row["adm1_code"]):
            return f"{row['mon-yr-id']}-{int(row['adm1_code'])}"
        else:
            return np.nan

    df["mon-yr-adm1-id"] = df.apply(_make_mon_yr_adm1_id, axis=1)

    print(f"{inspect.currentframe().f_code.co_name}: Completed successfully")

    return df


def clean_and_export(emdat_df, output_filepath):
    """
    Filter and export cleaned EM-DAT data to CSV.

    Parameters
    ----------
    emdat_df : pandas.DataFrame
        Final processed EM-DAT DataFrame.
    output_filepath : str
        Path for output CSV file.
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    cols_to_keep = [
        "id",
        "mon-yr-adm1-id",
        "mon-yr",
        "Start Date",
        "End Date",
        "ISO",
        "adm1_name",
        "adm1_code",
        "data_processing_flags",
    ]
    emdat_df = emdat_df[cols_to_keep]
    emdat_df.to_csv(output_filepath, encoding="utf-8-sig", index=False)
    print(f"File exported to: {output_filepath}")
    print(f"{inspect.currentframe().f_code.co_name}: Completed successfully")


def main():
    """Run complete EM-DAT preprocessing and disaggregation pipeline."""

    print("Starting EM-DAT preprocessing and disaggregation")

    # Check that filepaths and directories exist
    check_file_exists(EMDAT_FILEPATH)
    check_dir_exists(GAUL_L2_FILEPATH)

    # Read in data
    emdat_df = pd.read_csv(EMDAT_FILEPATH)
    gaul_l2 = read_gaul_shapefile(GAUL_L2_FILEPATH)

    # Initialize data processing flags column
    if "data_processing_flags" not in emdat_df.columns:
        emdat_df["data_processing_flags"] = ""
    else:
        # Convert existing values to string and replace NaNs with empty strings
        emdat_df["data_processing_flags"] = emdat_df["data_processing_flags"].apply(
            lambda x: str(int(x)) if pd.notna(x) else ""
        )

    # Expand admin zones (match to GAUL codes)
    emdat_df = expand_admin_zones(emdat_df, gaul_l2)

    # Add event dates
    emdat_df = add_event_dates(emdat_df)

    # Split events by month
    emdat_df = add_monthly_rows(emdat_df)

    # Append admin1 code to create unique monthly IDs
    emdat_df = append_adm1_code_to_id(emdat_df)

    # Export disaggregated dataset
    clean_and_export(emdat_df, OUTPUT_FILEPATH)

    print("Completed EM-DAT preprocessing and disaggregation")


if __name__ == "__main__":
    main()
