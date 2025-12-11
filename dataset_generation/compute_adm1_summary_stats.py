"""
Compute admin1-level summary statistics from MODIS flood dataset.

This script aggregates flood event data by admin1 region to compute:
- Mean flooded population
- Mean flooded area
- Mean normalized flooded area
- Total event count

Inputs
------
- emdat_modis_flood_dataset.csv : Main flood dataset with mon-yr-adm1-id events

Outputs
-------
- adm1_summary_stats.csv : Summary statistics grouped by admin1 code

Notes
-----
Excludes events with:
- Flag 12 (zero flooded pixels)
- Zero or NaN values in flooded_population, flooded_area, or flooded_area_norm
"""

import pandas as pd
import numpy as np

DATA_DIR = "../data/"
INPUT_FILEPATH = f"{DATA_DIR}emdat_modis_flood_dataset.csv"
OUTPUT_FILEPATH = f"{DATA_DIR}adm1_summary_stats.csv"


def filter_by_flags(df, flags, exclude=False):
    """
    Filter DataFrame by data quality flags.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with 'flags' column
    flags : list of int
        Flag numbers to filter on
    exclude : bool, optional
        If True, exclude rows with these flags. If False, keep only rows with these flags.
        Default is False.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame
    """
    # Handle NaN flags
    mask = df["flags"].isna()

    # Check if any of the specified flags are present
    for flag in flags:
        mask |= df["flags"].astype(str).str.contains(str(flag), na=False)

    if exclude:
        return df[~mask]
    else:
        return df[mask]


def compute_summary_stats(events_df):
    """
    Compute admin1-level summary statistics from flood events.

    Parameters
    ----------
    events_df : pd.DataFrame
        Flood events DataFrame with columns: adm1_code, flooded_population,
        flooded_area, flooded_area_norm, flags

    Returns
    -------
    pd.DataFrame
        Summary statistics grouped by admin1 code
    """
    print("Computing admin1 summary statistics...")
    print(f"  Initial events: {len(events_df)}")

    # Compute total event count per admin1 (ALL events, no filtering)
    event_counts = (
        events_df.groupby("adm1_code", as_index=False)
        .agg({"mon-yr-adm1-id": "count"})
        .rename(columns={"mon-yr-adm1-id": "event_count"})
    )
    print(f"  Event counts computed for {len(event_counts)} admin1 regions")

    # Filter out events with flag 12 (zero flooded pixels) for mean calculations
    events_filtered = filter_by_flags(events_df, flags=[12], exclude=True)
    print(f"  After excluding flag 12: {len(events_filtered)}")

    # Filter out rows with 0 or NaN in the flooded variables
    events_filtered = events_filtered[
        (events_filtered["flooded_population"] > 0)
        & (events_filtered["flooded_area"] > 0)
        & (events_filtered["flooded_area_norm"] > 0)
        & events_filtered["flooded_population"].notna()
        & events_filtered["flooded_area"].notna()
        & events_filtered["flooded_area_norm"].notna()
    ]
    print(f"  After excluding 0/NaN values: {len(events_filtered)}")

    # Compute mean values from filtered data
    mean_stats = (
        events_filtered.groupby("adm1_code", as_index=False)
        .agg(
            {
                "flooded_population": "mean",
                "flooded_area": "mean",
                "flooded_area_norm": "mean",
            }
        )
        .rename(
            columns={
                "flooded_population": "mean_flooded_population",
                "flooded_area": "mean_flooded_area",
                "flooded_area_norm": "mean_flooded_area_norm",
            }
        )
    )

    # Merge event counts with mean statistics
    summary_df = event_counts.merge(mean_stats, on="adm1_code", how="left")

    print(f"  Summary computed for {len(summary_df)} admin1 regions")

    return summary_df


def main():
    """Main execution function."""
    # Read the dataset
    print(f"Reading data from {INPUT_FILEPATH}...")
    events_df = pd.read_csv(INPUT_FILEPATH)

    # Compute summary statistics
    summary_df = compute_summary_stats(events_df)

    # Save to CSV
    summary_df.to_csv(OUTPUT_FILEPATH, index=False)
    print(f"\nSummary statistics saved to {OUTPUT_FILEPATH}")
    print(f"Columns: {list(summary_df.columns)}")
    print(f"\nPreview:")
    print(summary_df.head())


if __name__ == "__main__":
    main()
