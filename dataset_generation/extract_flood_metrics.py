"""
Extract flood impact metrics from flooded pixel imagery.

This script computes flood metrics for a single disaggregated EM-DAT flood event
by combining MODIS-based flooded pixel imagery with GPW population data. It clips
both datasets to the admin1 boundary, ensures spatial alignment, and calculates
population and area metrics.

Inputs
------
- {mon-yr-adm1-id}.tif : MODIS flood mask from Earth Engine
- GPW_by_adm1/{year}/gpw_adm1_{code}_year_{year}.nc : GPW population data
- GAUL_2015/g2015_2014_1 : GAUL Level 1 administrative boundaries shapefile
- Command-line argument : mon-yr-adm1-id (e.g., 04-2011-0131-CAN-825)

Outputs
-------
- {mon-yr-adm1-id}_metrics.csv : Flood metrics including:
    - Total and flooded population
    - Total and flooded area
    - Average flood duration
    - Cloud cover statistics
    - Error messages (if any)

Examples
--------
$ python extract_flood_metrics.py 04-2011-0131-CAN-825

"""

import xarray as xr
import rioxarray as rio
import geopandas as gpd
import numpy as np
import pandas as pd
import time
import argparse
import os

from utils.utils_misc import check_dir_exists, map_years_to_gpw_intervals

DATA_DIR = "../data/"
FLOODS_DIR = f"{DATA_DIR}EE_flooded_pixels/"
GPW_DIR = f"{DATA_DIR}GPW_by_adm1/"
GAUL_L1_FILEPATH = f"{DATA_DIR}GAUL_2015/g2015_2014_1/"
OUTPUT_DIR = f"{DATA_DIR}event_metrics/"
# OUTPUT_FILEPATH is set in the script since the path depends on command line argument


def parse_args():
    """Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="Compute people affected and total flooded area from flooded image"
    )
    parser.add_argument("mon_yr_adm1_id", type=str, help="ID")
    return parser.parse_args()


def max_coord_diff(ds1, ds2, coord):
    """
    Compute the maximum absolute difference between a single coordinate
    (e.g., 'x' or 'y') in two xarray Datasets or DataArrays.

    Parameters
    ----------
    ds1 : xr.Dataset or xr.DataArray
        First dataset or data array.
    ds2 : xr.Dataset or xr.DataArray
        Second dataset or data array.
    coord : str
        The coordinate name to compare.

    Returns
    -------
    float
        Maximum absolute difference between coordinate values.

    Raises
    ------
    KeyError
        If the coordinate is missing in either dataset.
    ValueError
        If the coordinate shapes do not match.
    """
    a = ds1.coords[coord].values
    b = ds2.coords[coord].values

    if a.shape != b.shape:
        raise ValueError(
            f"Coordinate '{coord}' has mismatched shapes: {a.shape} vs {b.shape}"
        )

    return np.max(np.abs(a - b)).item()


def main():
    # Log start time
    start_time = time.time()

    # Parse command line arguments
    args = parse_args()
    mon_yr_adm1_id = args.mon_yr_adm1_id

    # Make output dir if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Initialize values in case error is raised
    (
        error,
        total_population,
        average_population_density,
        total_area,
        total_num_pixels,
        num_flooded_pixels,
        flooded_population,
        flooded_area,
        av_duration_flooded_pixels,
        av_perc_cloud_cover_flooded,
        av_clear_views_flooded,
        total_flooded_pixel_days,
    ) = (
        "",
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
        np.nan,
    )

    try:
        # Check if directories and input files exist
        check_dir_exists(FLOODS_DIR)
        check_dir_exists(GAUL_L1_FILEPATH)
        check_dir_exists(GPW_DIR)

        # Years that correspond to a GPW file
        POP_YR_DICT = map_years_to_gpw_intervals()

        # Get year and adm1 code from flood ID
        year = POP_YR_DICT[int(mon_yr_adm1_id[3:7])]
        adm1_code = int(mon_yr_adm1_id.split("-")[-1])

        # Open flooded image data
        flood_filepath = f"{FLOODS_DIR}{mon_yr_adm1_id}.tif"
        print(f"Path to tif: {flood_filepath}")
        flood_im_da = rio.open_rasterio(
            flood_filepath, masked=True
        )  # Open as xr.DataArray

        # Reassign band dimension to the actual descriptive band names (rather than 1,2,3,4)
        # Convert variable dimension to data variables (xr.DataArray --> xr.Dataset)
        flood_im_da = flood_im_da.rename({"band": "variable"}).assign_coords(
            variable=list(flood_im_da.attrs["long_name"])
        )
        flood_im = flood_im_da.to_dataset(dim="variable")

        # Open population data
        gpw_adm1 = xr.open_dataset(
            f"{GPW_DIR}{year}/gpw_adm1_{adm1_code}_year_{year}.nc"
        )

        # Open admin 1 boundaries
        gaul_l1 = gpd.read_file(GAUL_L1_FILEPATH)

        # Get adm1 geometry and bounds of the geometry
        adm1_geom = gaul_l1[gaul_l1["ADM1_CODE"] == adm1_code].geometry
        minx, miny, maxx, maxy = adm1_geom.total_bounds

        # Clip data to adm1 bounding box to avoid exploding memory
        flood_im_bounding_box = flood_im.rio.clip_box(
            minx=minx, miny=miny, maxx=maxx, maxy=maxy
        )

        # Now, clip to adm1 zone
        flood_im_adm1 = flood_im_bounding_box.rio.clip(
            adm1_geom, adm1_geom.crs, drop=True, all_touched=True
        )

        # Get the two datasets on the same grid
        threshold = 1e-10  # realllyyy small threshold for difference in the grids

        # Check to make sure the difference isn't large!
        x_diff = max_coord_diff(flood_im_adm1, gpw_adm1, coord="x")
        y_diff = max_coord_diff(flood_im_adm1, gpw_adm1, coord="y")

        # Assign GPW coords to flooded image coords
        if x_diff < threshold and y_diff < threshold:
            flood_im_adm1 = flood_im_adm1.assign_coords(
                {
                    "x": gpw_adm1.coords["x"].values,
                    "y": gpw_adm1.coords["y"].values,
                }
            )

        else:
            raise ValueError(
                f"Flooded image and GPW population data have distinct grids.\nMax difference in x coordinates: {x_diff}\nMax difference in y coordinates: {y_diff}"
            )

        # Get total population, average population density, and area
        total_population = gpw_adm1["population_count"].sum().item()
        average_population_density = gpw_adm1["population_density"].mean().item()
        total_area = gpw_adm1["area"].sum().item()

        # Get flooded population and area
        gpw_masked = gpw_adm1.where(flood_im_adm1["flooded"] == 1)
        flooded_population = (
            gpw_masked["population_count"].sum().item()
        )  # units: persons
        flooded_area = gpw_masked["area"].sum().item()  # units: km2

        # Get average duration of the flooded pixels
        flooded_im_masked = flood_im_adm1.where(flood_im_adm1["flooded"] == 1)
        av_duration_flooded_pixels = flooded_im_masked["duration"].mean().item()

        # Get average percent cloud cover of the flooded pixels
        av_perc_cloud_cover_flooded = (
            flooded_im_masked["clear_perc_scaled"].mean().item()
        )

        # Get average number of clear (cloud free) views of the flooded pixels
        av_clear_views_flooded = flooded_im_masked["clear_views"].mean().item()

        # Get total flooded pixel days
        total_flooded_pixel_days = flooded_im_masked["duration"].sum().item()

        # Get total pixels
        total_num_pixels = flood_im_adm1.sizes["x"] * flood_im_adm1.sizes["y"]
        num_flooded_pixels = flooded_im_masked["flooded"].sum().item()

    except Exception as e:
        # Save error message
        error = f"{type(e).__name__}: {e}"
        print(f"Error recorded: {error}")

    finally:
        # Save results as a dataframe
        results_df = pd.DataFrame(
            {
                "mon-yr-adm1-id": mon_yr_adm1_id,
                "adm1_code": adm1_code,
                "total_population": total_population,
                "average_population_density": average_population_density,
                "total_area": total_area,
                "total_num_pixels": total_num_pixels,
                "num_flooded_pixels": num_flooded_pixels,
                "flooded_population": flooded_population,
                "flooded_area": flooded_area,
                "mean_duration_flooded_pixels": av_duration_flooded_pixels,
                "mean_percent_cloud_cover_flooded_pixels": av_perc_cloud_cover_flooded,
                "mean_clear_views_flooded_pixels": av_clear_views_flooded,
                "total_flooded_pixel_days": total_flooded_pixel_days,
                "metrics_error": error,
            },
            index=[0],
        )
        output_filepath = f"{OUTPUT_DIR}{mon_yr_adm1_id}_metrics.csv"
        results_df.to_csv(output_filepath, encoding="utf-8-sig", index=False)
        print(f"File saved to: {output_filepath}")

    # Compute elapsed time
    elapsed = time.time() - start_time
    hours, minutes = divmod(elapsed // 60, 60)
    print("Script complete.")
    print(f"Elapsed time: {int(hours)}h {int(minutes)}m")


if __name__ == "__main__":
    main()
