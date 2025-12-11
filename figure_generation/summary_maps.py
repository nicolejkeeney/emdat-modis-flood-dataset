"""
summary_maps.py

Generate choropleth maps of MODIS flood statistics at admin1 level (2000-2024).

"""

import os
import pandas as pd
import geopandas as gpd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from time import time
from datetime import timedelta
import inspect

# Figure settings
FIG_DPI = 600
plt.rcParams["font.family"] = "Georgia"

# Input paths
DATA_DIR = "../data/"
EVENTS_ADM1_FILEPATH = f"{DATA_DIR}adm1_summary_stats.csv"
GAUL_L1_FILEPATH = f"{DATA_DIR}g2015_2014_1/"
COUNTRY_BOUNDARIES_FILEPATH = f"{DATA_DIR}ne_110m_admin_0_countries"

# Output paths
FIG_DIR = "figs/"
MAPS_DIR = f"{FIG_DIR}adm1_maps/"


def read_and_prepare_data():
    """
    Read and prepare admin1 summary statistics and geographic boundaries.

    Returns
    -------
    tuple
        (events_adm1_df, countries_gdf) - Admin1 flood statistics and country boundaries
    """
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    # Read in data
    events_adm1_df = pd.read_csv(EVENTS_ADM1_FILEPATH)
    gaul_l1 = gpd.read_file(GAUL_L1_FILEPATH)
    countries_gdf = gpd.read_file(COUNTRY_BOUNDARIES_FILEPATH)

    # Drop Antarctica from countries
    countries_gdf = countries_gdf[countries_gdf["ADMIN"] != "Antarctica"]

    # Clean up GAUL gdf so it can be easily merged
    gaul_l1 = gaul_l1[["ADM1_CODE", "geometry"]].rename(
        columns={"ADM1_CODE": "adm1_code"}
    )

    # Add in GAUL geometry for admin1 regions
    events_adm1_df = events_adm1_df.merge(gaul_l1, on="adm1_code", how="left")
    events_adm1_df = gpd.GeoDataFrame(events_adm1_df)

    print(f"{inspect.currentframe().f_code.co_name}: Complete.")

    return events_adm1_df, countries_gdf


def make_map(
    df,
    col,
    label="",
    vmin=None,
    vmax=None,
    title=None,
    cmap="Blues",
    save_path=None,
    borders=None,
    border_linewidth=0.15,
):
    """
    Plot a geospatial DataFrame on a Robinson projection map.

    Parameters
    ----------
    df : geopandas.GeoDataFrame
        GeoDataFrame containing geometry and the data column to plot.
    col : str
        Column name in `df` to visualize.
    label : str, optional
        Label for the legend colorbar. Default is "".
    vmin : float, optional
        Minimum value for the colormap scaling. If None, inferred from data.
    vmax : float, optional
        Maximum value for the colormap scaling. If None, inferred from data.
    title : str, optional
        Map title. Default is None.
    cmap : str, optional
        Matplotlib colormap to use. Default is "Blues".
    save_path : str, optional
        Path to save the figure. Include file extension (i.e. ".png")
        If provided, the figure is saved to disk.
    borders : geopandas.GeoDataFrame, optional
        GeoDataFrame of boundaries for overlay. Default is None.
    border_linewidth: float, optional
        Linewidth of the borders. Default to 0.15

    Notes
    -----
    The function creates a matplotlib figure but does not return it.
    The plot is displayed inline in interactive environments and
    optionally saved if `save_path` is provided.
    """
    # Initialize figure
    fig, ax = plt.subplots(figsize=(10, 6), subplot_kw={"projection": ccrs.Robinson()})

    # Compute vmin, vmax if not provided
    if vmin is None:
        vmin = df[col].quantile(0.05)  # 5th percentile
    if vmax is None:
        vmax = df[col].quantile(0.95)  # 95th percentile

    # Plot data
    df.plot(
        column=col,
        cmap=cmap,
        legend=True,
        ax=ax,
        vmin=vmin,
        vmax=vmax,
        legend_kwds={"shrink": 0.5, "label": label, "extend": "max"},
        transform=ccrs.PlateCarree(),
    )

    # Plot additional borders
    if borders is not None:
        borders.boundary.plot(
            ax=ax,
            linewidth=border_linewidth,
            color="grey",
            transform=ccrs.PlateCarree(),
        )

    # Make map pretty
    if title is not None:
        ax.set_title(title, fontsize=14)
    ax.set_axis_off()

    # Save figure
    if save_path is not None:
        plt.savefig(save_path, dpi=FIG_DPI, bbox_inches="tight")
        print(f"{inspect.currentframe().f_code.co_name}: Saved figure to {save_path}")


def make_adm1_maps(events_adm1_df, countries_gdf):
    """
    Generate choropleth maps for admin1-level MODIS flood statistics (2000-2024).

    Parameters
    ----------
    events_adm1_df : geopandas.GeoDataFrame
       Admin1 flood statistics with geometry and MODIS-derived metrics.
    countries_gdf : geopandas.GeoDataFrame
       Country boundaries for map borders.
    """

    start_time = time()
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    make_map(
        df=events_adm1_df,
        col="mean_flooded_population",
        label="Population",
        title="Mean Flooded Population by Admin1 Region (2000-2024)",
        cmap="Purples",
        save_path=f"{MAPS_DIR}mean_flooded_population.png",
        borders=countries_gdf,
    )

    make_map(
        df=events_adm1_df,
        col="mean_flooded_area",
        label="Area (km²)",
        title="Mean Flooded Area by Admin1 Region (2000-2024)",
        cmap="Blues",
        save_path=f"{MAPS_DIR}mean_flooded_area.png",
        borders=countries_gdf,
    )

    make_map(
        df=events_adm1_df,
        col="mean_flooded_area_norm",
        label="Normalized Area",
        title="Mean Normalized Flooded Area by Admin1 Region (2000-2024)",
        cmap="Blues",
        save_path=f"{MAPS_DIR}mean_flooded_area_norm.png",
        borders=countries_gdf,
    )

    make_map(
        df=events_adm1_df,
        col="event_count",
        label="Number of Events",
        title="Total Flood Events by Admin1 Region (2000-2024)",
        cmap="Oranges",
        save_path=f"{MAPS_DIR}event_count.png",
        borders=countries_gdf,
    )

    td = timedelta(seconds=int(time() - start_time))
    print(f"{inspect.currentframe().f_code.co_name}: Complete.")
    print(f"{inspect.currentframe().f_code.co_name}: Time elapsed: {td}")


def make_subregion_maps(events_subregion_df):
    """
    Generate choropleth maps for subregional flood statistics.

    Parameters
    ----------
    events_subregion_df : geopandas.GeoDataFrame
        Subregional flood events data with geometry and statistics columns.

    """
    start_time = time()
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    make_map(
        df=events_subregion_df,
        col="mean_total_affected",
        label="# people",
        vmin=0,
        vmax=150000,
        title="Average number of people affected (2000-2024)",
        cmap="Purples",
        save_path=f"{MAPS_DIR_SUBREGION}av_affected_subregion.png",
        borders=events_subregion_df,
        border_linewidth=0.25,
    )

    make_map(
        df=events_subregion_df,
        col="median_total_affected",
        label="# people",
        vmin=0,
        vmax=150000,
        title="Median number of people affected (2000-2024)",
        cmap="Purples",
        save_path=f"{MAPS_DIR_SUBREGION}median_affected_subregion.png",
        borders=events_subregion_df,
        border_linewidth=0.25,
    )

    make_map(
        df=events_subregion_df,
        col="mean_damages",
        label="Adjusted damages ('000 US$)",
        vmin=0,
        vmax=350000,
        title="Average economic damages (2000-2024)",
        cmap="Greens",
        save_path=f"{MAPS_DIR_SUBREGION}av_damages_subregion.png",
        borders=events_subregion_df,
        border_linewidth=0.25,
    )

    make_map(
        df=events_subregion_df,
        col="max_damages",
        label="Adjusted damages ('000 US$)",
        vmin=0,
        vmax=10000000,
        title="Maximum economic damages (2000-2024)",
        cmap="Greens",
        save_path=f"{MAPS_DIR_SUBREGION}max_damages_subregion.png",
        borders=events_subregion_df,
        border_linewidth=0.25,
    )

    make_map(
        df=events_subregion_df,
        col="id_count",
        label="Number of events",
        vmin=0,
        vmax=800,
        title="Total number inland floods (2000-2024)",
        cmap="Oranges",
        save_path=f"{MAPS_DIR_SUBREGION}event_count_subregion.png",
        borders=events_subregion_df,
        border_linewidth=0.25,
    )

    td = timedelta(seconds=int(time() - start_time))
    print(f"{inspect.currentframe().f_code.co_name}: Complete.")
    print(f"{inspect.currentframe().f_code.co_name}: Time elapsed: {td}")


def make_region_maps(events_region_df):
    """
    Generate choropleth maps for regional flood statistics.

    Parameters
    ----------
    events_region_df : geopandas.GeoDataFrame
        Regional flood events data with geometry and statistics columns.

    """
    start_time = time()
    print(f"{inspect.currentframe().f_code.co_name}: Starting...")

    make_map(
        df=events_region_df,
        col="mean_total_affected",
        label="# people",
        vmin=10000,
        vmax=150000,
        title="Average number of people affected (2000-2024)",
        cmap="Purples",
        save_path=f"{MAPS_DIR_REGION}av_affected_region.png",
        borders=events_region_df,
        border_linewidth=0.25,
    )

    make_map(
        df=events_region_df,
        col="mean_damages",
        label="Damages ('000 US$)",
        vmin=600000,
        vmax=2000000,
        title="Average economic damages (2000-2024)",
        cmap="Greens",
        save_path=f"{MAPS_DIR_REGION}av_damages_region.png",
        borders=events_region_df,
        border_linewidth=0.25,
    )

    make_map(
        df=events_region_df,
        col="max_damages",
        label="Adjusted damages ('000 US$)",
        vmin=0,
        vmax=20000000,
        title="Maximum economic damages (2000-2024)",
        cmap="Greens",
        save_path=f"{MAPS_DIR_REGION}max_damages_region.png",
        borders=events_region_df,
        border_linewidth=0.25,
    )

    make_map(
        df=events_region_df,
        col="id_count",
        label="Number of events",
        vmin=100,
        vmax=1500,
        title="Total number inland floods (2000-2024)",
        cmap="Oranges",
        save_path=f"{MAPS_DIR_REGION}event_count_region.png",
        borders=events_region_df,
        border_linewidth=0.25,
    )

    td = timedelta(seconds=int(time() - start_time))
    print(f"{inspect.currentframe().f_code.co_name}: Complete.")
    print(f"{inspect.currentframe().f_code.co_name}: Time elapsed: {td}")


def main():
    """Main execution function."""
    # Make output dir if it doesn't already exist
    os.makedirs(MAPS_DIR, exist_ok=True)

    # Read and prepare data
    events_adm1_df, countries_gdf = read_and_prepare_data()

    # Make admin1 maps
    make_adm1_maps(events_adm1_df, countries_gdf)


if __name__ == "__main__":
    main()
