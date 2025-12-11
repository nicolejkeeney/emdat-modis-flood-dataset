"""
Postprocess and clean flood event metrics dataset.

This script performs final postprocessing on flood event metrics, including
adding data quality flags, computing event duration, sorting by original event
order, and correcting country assignments where GAUL differs from EM-DAT.

Inputs
------
- event_metrics.csv : Flood metrics for each disaggregated event
- emdat_floods_by_mon_yr_adm1.csv : Disaggregated EM-DAT events
- emdat_2000_2024.csv : Original EM-DAT records (used for sorting)

Outputs
-------
- emdat_modis_flood_dataset.csv : Final cleaned dataset with:
    - Flood impact metrics (population, area, duration)
    - Data quality flags (1-15)
    - Event duration
    - Corrected country assignments

Examples
--------
$ python dataset_postprocessing.py

Notes
-----
Data quality flags indicate issues such as missing data, coordinate mismatches,
flooded area of zero, etc. See flag documentation for details.

"""

import pandas as pd
from utils.emdat_toolbox import add_event_dates
from utils.utils_misc import summarize_flags

DATA_DIR = "../data/"
METRICS_FILEPATH = f"{DATA_DIR}event_metrics.csv"
EMDAT_DISAGGREGATED_FILEPATH = f"{DATA_DIR}emdat_floods_by_mon_yr_adm1.csv"
EMDAT_NONDISAGREGGATED_FILEPATH = (
    f"{DATA_DIR}emdat-2000-2024.csv"  # Used for sorting data into original order
)
OUTPUT_FILEPATH = f"{DATA_DIR}emdat_modis_flood_dataset.csv"

# Dictionary mapping problematic adm1_codes to correct countries per GAUL
# These codes appear in multiple countries in the source data but should be assigned to one country
COUNTRY_CORRECTIONS = {
    2720: "Spain",
    2961: "Timor-Leste",
    25351: "Montenegro",
    25355: "Montenegro",
    25356: "Montenegro",
    25365: "Montenegro",
    25372: "Serbia",
    25373: "Serbia",
    25375: "Serbia",
    25376: "Serbia",
    25378: "Serbia",
    25379: "Serbia",
    25381: "Serbia",
    25385: "Serbia",
    25389: "Serbia",
    25394: "Serbia",
    25395: "Serbia",
    40408: "Jammu and Kashmir",
    40409: "Jammu and Kashmir",
    40422: "Jammu and Kashmir",
    40423: "Jammu and Kashmir",
    40424: "Jammu and Kashmir",
    40425: "Jammu and Kashmir",
    40426: "Jammu and Kashmir",
    40427: "Jammu and Kashmir",
    40428: "Jammu and Kashmir",
    40429: "Jammu and Kashmir",
    40430: "Jammu and Kashmir",
    40431: "Jammu and Kashmir",
}


def regex(flag):
    """
    Create a regex pattern to match a specific flag in a semicolon-separated string.

    Parameters
    ----------
    flag : int or str
        Flag number to match.

    Returns
    -------
    str
        Regex pattern string.
    """
    return rf"(?:^|;\s*){flag}(?:$|;\s*)"


def get_missing_rows(emdat_orig_df, emdat_processed_df):
    """
    Identify flood events missing from a processed EMDAT dataset and flag issues.
    Also adds start and end date.

    Parameters
    ----------
    emdat_orig_df : pd.DataFrame
        Original EMDAT dataset with all disaster records.
    emdat_processed_df : pd.DataFrame
        Processed EMDAT dataset to compare against.

    Returns
    -------
    pd.DataFrame
        Subset of `emdat_orig_df` for missing events, with added
        `data_processing_flags`, `flags`, `Start Date`, and `End Date` columns.
    """
    print("Identifying missing events and adding flags...")

    # Get IDs that are missing from the final dataset
    orig_ids = emdat_orig_df["id"].unique()
    final_df_ids = emdat_processed_df["id"].unique()
    missing_ids = [id for id in orig_ids if id not in final_df_ids]

    # Create flags column
    missing_df = emdat_orig_df[emdat_orig_df["id"].isin(missing_ids)].copy()
    missing_df["data_processing_flags"] = ""  # Required for add_event_dates function
    missing_df["flags"] = ""

    # Get Start and End Date columns
    missing_df = add_event_dates(missing_df)

    # Add flags for missing events
    mask9 = missing_df["Start Date"].isna() | missing_df["End Date"].isna()
    missing_df.loc[mask9, "flags"] += "; 9"
    print(f"  Added flag 9 to {mask9.sum()} missing events (missing start/end date)")

    mask10 = missing_df["Admin Units"].isna()
    missing_df.loc[mask10, "flags"] += "; 10"
    print(f"  Added flag 10 to {mask10.sum()} missing events (missing admin units)")

    mask11 = missing_df["flags"] == ""
    missing_df.loc[mask11, "flags"] += "; 11"
    print(f"  Added flag 11 to {mask11.sum()} missing events (other reasons)")

    # Format the date same as the metrics, as a string
    for col in ["Start Date", "End Date"]:
        missing_df[col] = pd.to_datetime(missing_df[col], errors="coerce")
        missing_df[col] = missing_df[col].dt.strftime("%m/%d/%Y")

    return missing_df


def add_data_flags(metrics_df, emdat_df, emdat_orig_df):
    """
    Add data quality flags to the metrics dataframe.

    Flags:
    1 - Start day originally NaN
    2 - End day originally NaN
    3 - Start date before 2000-02-25 (no Terra data)
    4 - No tif found (other reasons)
    5 - GPW file not found
    6 - Coordinate mismatch between flood and population data
    7-8, 13-15 - Direct copy from data_processing_flags
    9 - Missing start/end date
    10 - Missing admin units
    11 - Other reasons for missing
    12 - Flooded area = 0

    Parameters
    ----------
    metrics_df : pd.DataFrame
        Event metrics dataframe.
    emdat_df : pd.DataFrame
        Disaggregated EMDAT dataframe.
    emdat_orig_df : pd.DataFrame
        Original non-disaggregated EMDAT dataframe.

    Returns
    -------
    pd.DataFrame
        Dataframe with added 'flags' column.
    """
    print("\nAdding data quality flags...")

    # Fill error columns with empty string instead of NaN
    metrics_df["metrics_error"] = metrics_df["metrics_error"].fillna("")
    metrics_df["data_processing_flags"] = metrics_df["data_processing_flags"].fillna("")
    emdat_df = emdat_df.drop("data_processing_flags", axis=1, errors="ignore")

    # Merge metrics into emdat dataframe
    flags_df = emdat_df.merge(metrics_df, on=list(emdat_df.columns), how="left")
    flags_df["flags"] = ""

    # Get missing rows and add appropriate flags
    missing_df = get_missing_rows(emdat_orig_df, metrics_df)
    missing_df = missing_df[
        [col for col in missing_df.columns if col in flags_df.columns]
    ]
    flags_df = pd.concat([flags_df, missing_df], ignore_index=True)

    # Replace EMDAT preprocessing string flags with appropriate numerical flags
    mask1 = flags_df["data_processing_flags"].str.contains(
        "Start day originally NaN", na=False
    )
    flags_df.loc[mask1, "flags"] += "; 1"
    print(f"  Added flag 1 to {mask1.sum()} events (start day originally NaN)")

    mask2 = flags_df["data_processing_flags"].str.contains(
        "End day originally NaN", na=False
    )
    flags_df.loc[mask2, "flags"] += "; 2"
    print(f"  Added flag 2 to {mask2.sum()} events (end day originally NaN)")

    # Direct copy flags
    mask7 = flags_df["data_processing_flags"].str.contains(regex(7), na=False)
    flags_df.loc[mask7, "flags"] += "; 7"
    print(f"  Added flag 7 to {mask7.sum()} events")

    mask8 = flags_df["data_processing_flags"].str.contains(regex(8), na=False)
    flags_df.loc[mask8, "flags"] += "; 8"
    print(f"  Added flag 8 to {mask8.sum()} events")

    mask13 = flags_df["data_processing_flags"].str.contains(regex(13), na=False)
    flags_df.loc[mask13, "flags"] += "; 13"
    print(f"  Added flag 13 to {mask13.sum()} events")

    mask14 = flags_df["data_processing_flags"].str.contains(regex(14), na=False)
    flags_df.loc[mask14, "flags"] += "; 14"
    print(f"  Added flag 14 to {mask14.sum()} events")

    mask15 = flags_df["data_processing_flags"].str.contains(regex(15), na=False)
    flags_df.loc[mask15, "flags"] += "; 15"
    print(f"  Added flag 15 to {mask15.sum()} events")

    # GPW file not found
    mask5 = flags_df["metrics_error"].str.contains(
        "data/GPW_by_adm1/", na=False
    ) & flags_df["metrics_error"].str.contains("FileNotFound", na=False)
    flags_df.loc[mask5, "flags"] += "; 5"
    print(f"  Added flag 5 to {mask5.sum()} events (GPW file not found)")

    # Coordinate mismatch
    mask6 = (
        flags_df["metrics_error"].str.contains("ValueError", na=False)
        & flags_df["metrics_error"].str.contains("Coordinate", na=False)
        & flags_df["metrics_error"].str.contains("has mismatched shapes", na=False)
    )
    flags_df.loc[mask6, "flags"] += "; 6"
    print(f"  Added flag 6 to {mask6.sum()} events (coordinate mismatch)")

    # Start date before Terra satellite data available
    mask3 = pd.to_datetime(flags_df["Start Date"], format="mixed") < pd.to_datetime(
        "2000-02-25"
    )
    flags_df.loc[mask3, "flags"] += "; 3"
    print(f"  Added flag 3 to {mask3.sum()} events (start date before 2000-02-25)")

    # No tif found for reasons other than flag 3
    mask4_metrics = (
        flags_df["metrics_error"].str.contains("RasterioIOError", na=False)
        & flags_df["metrics_error"].str.contains(
            ".tif: No such file or directory", na=False
        )
        & (~mask3)
    )
    flags_df.loc[mask4_metrics, "flags"] += "; 4"
    print(f"  Added flag 4 to {mask4_metrics.sum()} events (no tif found)")

    # Flooded area = 0
    mask12 = flags_df["flooded_area"] == 0
    flags_df.loc[mask12, "flags"] += "; 12"
    print(f"  Added flag 12 to {mask12.sum()} events (flooded area = 0)")

    # Drop temporary columns
    flags_df = flags_df.drop(columns=["data_processing_flags", "metrics_error"])

    # Sort and clean flags
    flags_df["flags"] = flags_df["flags"].apply(sort_flags)
    flags_df["flags"] = flags_df["flags"].str.lstrip("; ")

    return flags_df


def sort_flags(flag_str):
    """
    Sort numbers in a semicolon-separated flag string in ascending order.

    Parameters
    ----------
    flag_str : str
        String of numbers separated by semicolons, e.g., "12; 2; 1".

    Returns
    -------
    str
        Sorted numbers as a semicolon-separated string, e.g., "1; 2; 12".
    """
    if not flag_str:
        return ""
    nums = [int(f) for f in flag_str.split(";") if f.strip()]
    return "; ".join(map(str, sorted(nums)))


def add_event_duration(df):
    """
    Add event duration in days.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'Start Date' and 'End Date' columns.

    Returns
    -------
    pd.DataFrame
        DataFrame with new 'event_duration (days)' column.
    """
    print("\nAdding event duration...")
    event_duration = df["End Date"] - df["Start Date"] + pd.Timedelta(days=1)
    df["event_duration (days)"] = event_duration.dt.days
    print(f"  Event duration added for {len(df)} events")
    return df


def correct_country_assignments(df):
    """
    Correct country assignments for admin1 codes where GAUL differs from EM-DAT.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'adm1_code' and 'Country' columns.

    Returns
    -------
    pd.DataFrame
        DataFrame with corrected country assignments.
    """
    print("\nCorrecting country assignments...")
    corrections_made = 0
    for code, correct_country in COUNTRY_CORRECTIONS.items():
        mask = df["adm1_code"] == code
        if mask.any():
            df.loc[mask, "Country"] = correct_country
            corrections_made += mask.sum()
    print(
        f"  Corrected {corrections_made} rows across {len(COUNTRY_CORRECTIONS)} admin1 codes"
    )
    return df


def sort_by_original_order(df, emdat_orig_df):
    """
    Sort dataframe by original EM-DAT event order.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to sort.
    emdat_orig_df : pd.DataFrame
        Original EM-DAT dataframe with reference order.

    Returns
    -------
    pd.DataFrame
        Sorted dataframe.
    """
    print("\nSorting by original event order...")
    df["id"] = pd.Categorical(
        df["id"], categories=emdat_orig_df["id"].values, ordered=True
    )
    df = df.sort_values("id").reset_index(drop=True)
    print(f"  Sorted {len(df)} events")
    return df


def main():
    """
    Main execution pipeline for dataset postprocessing.
    """
    print("Starting dataset postprocessing pipeline...")

    # Read in data
    print("\nReading input files...")
    metrics_df = pd.read_csv(METRICS_FILEPATH)
    emdat_df = pd.read_csv(EMDAT_DISAGGREGATED_FILEPATH)
    emdat_orig_df = pd.read_csv(EMDAT_NONDISAGREGGATED_FILEPATH)
    print(f"  Loaded {len(metrics_df)} metric records")
    print(f"  Loaded {len(emdat_df)} disaggregated events")
    print(f"  Loaded {len(emdat_orig_df)} original events")

    # Step 1: Add data quality flags
    output_df = add_data_flags(metrics_df, emdat_df, emdat_orig_df)

    # Step 2: Convert dates to datetime and add event duration
    output_df["Start Date"] = pd.to_datetime(output_df["Start Date"], format="mixed")
    output_df["End Date"] = pd.to_datetime(output_df["End Date"], format="mixed")
    output_df = add_event_duration(output_df)

    # Step 3: Sort by original event order
    output_df = sort_by_original_order(output_df, emdat_orig_df)

    # Step 4: Correct country assignments
    output_df = correct_country_assignments(output_df)

    # Step 5: Print flag summary
    print("\nFlag summary:")
    summarize_flags(output_df, verbose=True)

    # Step 6: Export final dataset
    print("\nExporting final dataset...")
    output_df.to_csv(OUTPUT_FILEPATH, encoding="utf-8-sig", index=False)
    print(f"  File exported to: {OUTPUT_FILEPATH}")
    print(f"  Total records: {len(output_df)}")
    print(f"  Unique events: {output_df['id'].nunique()}")

    print("\nPostprocessing complete.")


if __name__ == "__main__":
    main()
