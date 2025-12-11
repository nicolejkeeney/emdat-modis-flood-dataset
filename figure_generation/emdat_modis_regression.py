"""
emdat_modis_regression.py

Compare EMDAT total affected vs MODIS flooded population
Aggregates flood events and creates scatter plot with regression analysis

"""

import os
import pandas as pd
import numpy as np
from data_analysis_utils import plot_scatter_with_regression

DATA_DIR = "../data/"
EVENTS_FILEPATH = f"{DATA_DIR}emdat_modis_flood_dataset.csv"
EMDAT_FILEPATH = f"{DATA_DIR}emdat_2000_2024.csv"
OUTPUT_DIR = "figs/"
OUTPUT_FIG_PATH = f"{OUTPUT_DIR}emdat_modis_regression.png"


def main():
    # Make output dir if it doesn't already exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Read in data
    events_df = pd.read_csv(EVENTS_FILEPATH)
    emdat_df = pd.read_csv(EMDAT_FILEPATH, usecols=["id", "Total Affected"])

    # Agreggate by event ID to combine flooded population from all disaggregated events
    events_agg_df = events_df.groupby("id", as_index=False).agg(
        {"flooded_population": "sum"}
    )

    # Merge with EMDAT data to get Total Affected
    events_agg_df = events_agg_df.merge(emdat_df, on="id", how="inner").dropna()

    events_agg_df.rename(
        columns={
            "flooded_population": "MODIS_flooded_population",
            "Total Affected": "EMDAT_total_affected",
        },
        inplace=True,
    )

    # Replace zeros with NaN, then drop all NaNs
    len_before = len(events_agg_df)
    events_agg_df = events_agg_df.replace(0, np.nan).dropna()
    num_dropped = len_before - len(events_agg_df)
    print(
        f"Replacing zeroes with NaN: {(num_dropped/len_before)*100:.1f}% of the total adm1-month floods"
    )

    # Add log-scale
    events_agg_df["MODIS_flooded_population (log)"] = events_agg_df[
        "MODIS_flooded_population"
    ].apply(np.log)
    events_agg_df["EMDAT_total_affected (log)"] = events_agg_df[
        "EMDAT_total_affected"
    ].apply(np.log)

    plot_scatter_with_regression(
        events_agg_df,
        y_col="EMDAT_total_affected (log)",
        x_col="MODIS_flooded_population (log)",
        title="EM-DAT people affected vs. MODIS-derived flooded population",
        ylabel="EM-DAT people affected (log scale)",
        xlabel="MODIS-derived flooded population (log scale)",
        figsize=(7.5, 5),
        xlim=(-2.5, 17.5),
        ylim=(0, 20),
        tick_interval=2.5,
        one_to_one=False,
        save_path=OUTPUT_FIG_PATH,
    )


if __name__ == "__main__":
    main()
