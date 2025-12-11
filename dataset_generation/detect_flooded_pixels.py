"""
Detect flooded pixels using Google Earth Engine.

This script processes disaggregated EM-DAT flood events using MODIS satellite
imagery and the Cloud2Street flood detection algorithm. For each event, it
detects flooded pixels, masks false positives (using slope and permanent water),
and exports results to Google Drive.

Inputs
------
- emdat_floods_by_mon_yr_adm1.csv : Disaggregated flood events
- GAUL_2015/g2015_2014_1 : GAUL Level 1 administrative boundaries shapefile
- ID list text file : File containing one flood event ID per line

Outputs
-------
- {mon-yr-adm1-id}.tif : GeoTIFF with 4 bands for each event:
    - Band 1 (flooded): Binary flood mask (1=flooded, 0=not flooded)
    - Band 2 (duration): Number of days pixel was flooded
    - Band 3 (clear_views): Number of cloud-free observations
    - Band 4 (clear_perc_scaled): Percent of clear views (0-100)

Examples
--------
$ python detect_flooded_pixels.py id_list.txt

Notes
-----
- Requires Earth Engine authentication
- Outputs are exported to Google Drive folder: EE_flooded_pixels
- Task manager keeps queue below Earth Engine limits

"""

import pandas as pd
import numpy as np
import geopandas as gpd
import ee
import time
import argparse
from datetime import datetime
import os
import inspect
from utils import flood_detection, modis_toolbox
from utils.utils_misc import check_dir_exists, check_file_exists
from utils.logger import setup_logger, close_logger

DATA_DIR = "../data/"
EMDAT_FILEPATH = f"{DATA_DIR}emdat/emdat_floods_by_mon_yr_adm1.csv"
GAUL_L1_FILEPATH = f"{DATA_DIR}GAUL_2015/g2015_2014_1"
DRIVE_EXPORT_FOLDER = "EE_flooded_pixels"
EE_PROJECT_NAME = "clim-haz"  # Earth Engine project name (must be registered already)


def parse_args():
    """
    Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments with attributes like `id_file`.
    """
    parser = argparse.ArgumentParser(
        description="Run flood detection on batches of events specified by an ID list text file."
    )
    parser.add_argument(
        "id_file",
        help="Path to the text file containing one flood event ID per line.",
    )
    return parser.parse_args()


def read_ids_from_txt(filepath):
    """
    Read a text file containing one ID per line and return as a list.

    Parameters
    ----------
    filepath : str
        Path to the .txt file with one ID per line.

    Returns
    -------
    list of str
        List of event IDs.
    """
    with open(filepath, "r") as f:
        return [line.strip() for line in f if line.strip()]


def build_emdat_geodataframe(logger):
    """
    Load EM-DAT event data and merge it with GAUL Admin1 geometries.

    This function reads the GAUL Level 1 shapefile and EM-DAT flood event CSV,
    joins the spatial data to the event records based on ADM1 codes,
    and returns a GeoDataFrame with geometries attached.

    Parameters
    ---------
    logger : logging.Logger
        Logger for error reporting.

    Returns
    -------
    emdat_floods_by_adm1 : GeoDataFrame
        EM-DAT flood events enriched with GAUL Admin1 geometries.
    """

    logger.info(f"{inspect.currentframe().f_code.co_name}: Starting...")

    # Read in data
    gaul_l1 = gpd.read_file(GAUL_L1_FILEPATH).rename(
        columns={"ADM1_CODE": "adm1_code", "geometry": "adm1_geometry"}
    )
    emdat = pd.read_csv(EMDAT_FILEPATH)

    # Add adm1 geometry as column
    # Match on adm1 code
    emdat = emdat.merge(gaul_l1[["adm1_code", "adm1_geometry"]], on="adm1_code")
    emdat = gpd.GeoDataFrame(emdat, geometry="adm1_geometry")

    logger.info(f"{inspect.currentframe().f_code.co_name}: Completed successfully")

    return emdat


def manage_task_queue(logger, threshold=290, sleep_length=900):
    """
    Sleep if Earth Engine task queue is near the limit.

    Parameters
    ----------
    logger : logging.Logger
        Logger for error reporting.
    threshold : int, optional
        Task limit to wait for (default is 290).
    sleep_length : int, optional
        Seconds to wait when task queue is at limit (default is 900 seconds: 15 minutes)
    """

    def _get_active_task_count():
        """Return the number of active Earth Engine tasks (READY or RUNNING)."""
        return sum(task.state in ["READY", "RUNNING"] for task in ee.batch.Task.list())

    while _get_active_task_count() >= threshold:
        logger.info(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Task queue near limit ({threshold}). Sleeping for {sleep_length / 60:.1f} minutes..."
        )
        time.sleep(sleep_length)
        logger.info(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Woke up, rechecking task queue..."
        )


def process_event(event, event_id, flood_bounds, logger):
    """
    Run the flood detection and prepare image export for a single event.
    Apply a permanent water mask, land mask (to mask out oceans), and a slope mask.

    Parameters
    ----------
    event : Series
        Row of a GeoDataFrame containing flood event info.
    event_id: str
        Event id
    flood_bounds: ee.Geometry
        Bounds to use for flood geometry
    logger : logging.Logger
        Logger for error reporting.

    Returns
    -------
    flood_image: EE.Image()

    """
    logger.info(f"{inspect.currentframe().f_code.co_name}: Starting...")

    began, ended = event["Start Date"], event["End Date"]

    # Check if began, ended are valid
    def _is_invalid_date(x):
        return isinstance(x, float) and np.isnan(x)

    if any(_is_invalid_date(x) for x in [began, ended]):
        raise ValueError(
            f"Start or end date is NaN or None. \nFailed to process event {event_id}"
        )

    # Terra satellite only has observations from 2000-02-24.
    # So, if the start date of the event is on or before that date, the event can't be processed
    if datetime.strptime(began, "%Y-%m-%d") <= datetime.strptime(
        "2000-02-25", "%Y-%m-%d"
    ):
        raise ValueError(
            f"Start date is before 2000-02-25, and Terra Surface Reflectance data is not available before that date.\nFailed to process event {event_id}"
        )

    # Detect flooded pixels
    flood_map = flood_detection.detect_flooded_pixels(
        flood_bounds, began, ended, "standard"
    )
    flood_map = modis_toolbox.apply_slope_mask(flood_map, thresh=5)

    # Add JRC permanent water mask
    perm_water = modis_toolbox.get_jrc_perm(flood_bounds)
    flood_map = flood_map.addBands(perm_water)

    # Get land polygons
    land_mask = modis_toolbox.get_land_mask(flood_bounds)
    flood_map = flood_map.addBands(land_mask)

    # Mask ocean areas and permanent water in flooded and duration bands
    flood_map = flood_map.addBands(
        flood_map.select("flooded")
        .where(land_mask.neq(1), 0)
        .where(flood_map.select("jrc_perm_water").eq(1), 0)
        .rename("flooded"),
        overwrite=True,
    )

    flood_map = flood_map.addBands(
        flood_map.select("duration")
        .where(land_mask.neq(1), 0)
        .where(flood_map.select("jrc_perm_water").eq(1), 0)
        .rename("duration"),
        overwrite=True,
    )

    # Get clear_perc (which is actually a fraction 0-1) as a percentage (0-100)
    # Convert to int to avoid export error
    flood_map = flood_map.addBands(
        flood_map.select("clear_perc").multiply(100).rename("clear_perc_scaled"),
        overwrite=True,
    )

    # Convert to int16
    flood_image = ee.Image.cat(
        [
            flood_map.select("flooded").toUint16(),
            flood_map.select("duration").toUint16(),
            flood_map.select("clear_views").toUint16(),
            flood_map.select("clear_perc_scaled").toUint16(),
        ]
    ).rename(["flooded", "duration", "clear_views", "clear_perc_scaled"])

    logger.info(f"Completed flooded pixel algorithm for event {event_id}")

    logger.info(f"{inspect.currentframe().f_code.co_name}: Completed successfully")

    return flood_image


def export_event_to_gdrive(
    event_id, flood_image, ee_flood_bounds, drive_export_folder, logger
):
    """
    Submit an Earth Engine task to export a flood image to google drive.

    The affine transformation defines the spatial referencing of the global
    MODIS 250m grid in geographic coordinates (EPSG:4326). It specifies the
    pixel size (~0.00225°), the origin at the top-left corner (-180° longitude,
    90° latitude), and a north-up orientation with a negative pixel height.
    This transform matches exactly with Earth Engine’s crsTransform parameter,
    just expressed in different formats for compatibility with different tools.
    Using this consistent grid definition ensures precise pixel alignment across
    datasets and exports. Previously, just using scale=250, the grids were all
    veryyyyy slightly misaligned, which caused issues with aligning each
    flooded pixel image with the reprojected global GPW population data.

    Band 0: flooded
    Band 1: duration
    Band 2: clear view
    Band 3: clear view percentage (0-100)

    Parameters
    ----------
    event_id : str
        ID of the flood event.
    flood_image : ee.Image
        Image containing flood information.
    ee_flood_bounds : ee.Geometry
        Simplified rectangular region to export.
    drive_export_folder : str
        Name of folder in Google Drive to export images to
    logger : logging.Logger
        Logger for error reporting.
    """
    logger.info(f"{inspect.currentframe().f_code.co_name}: Starting...")

    # MODIS global grid params in EPSG:4326
    modis_crs = "EPSG:4326"
    modis_scale_deg = 0.002245788210298804  # degrees per pixel (~250m at equator)
    modis_origin_lon = -180.0
    modis_origin_lat = 90.0

    # Build the crsTransform affine array
    # Format: [scale_x, shear_x, translate_x, shear_y, scale_y, translate_y]
    crs_transform = [
        modis_scale_deg,
        0,
        modis_origin_lon,
        0,
        -modis_scale_deg,
        modis_origin_lat,
    ]

    # Reproject the flood image to the MODIS global grid
    flood_image_reprojected = flood_image.reproject(
        crs=modis_crs,
        crsTransform=crs_transform,
        scale=None,  # scale is ignored if crsTransform is provided
    )

    task = ee.batch.Export.image.toDrive(
        image=flood_image_reprojected.select(
            ["flooded", "duration", "clear_views", "clear_perc_scaled"]
        ),
        description=f"{event_id}",
        folder=drive_export_folder,
        fileNamePrefix=f"{event_id}",
        region=ee_flood_bounds,
        scale=None,  # scale ignored because crsTransform is used
        crs=modis_crs,
        crsTransform=crs_transform,
        maxPixels=1e13,
        shardSize=128,
        fileFormat="GeoTIFF",
    )
    task.start()

    logger.info(f"{inspect.currentframe().f_code.co_name}: Completed successfully")


def initialize_log_csv(logger, log_dir="logs", log_filename="flood_export_log.csv"):
    """
    Initialize the CSV error log if it does not exist.

    Parameters
    ----------
    logger : logging.Logger
        Logger for error reporting.
    log_dir: str
        Logs directory
    log_filename : str, optional
        CSV log filename (default is "flood_export_log.csv").

    Returns
    -------
    str
        Path to the log file.
    list of str
        Log column names.
    """
    logger.info(f"{inspect.currentframe().f_code.co_name}: Starting...")

    log_filepath = f"{log_dir}/{log_filename}"
    log_columns = ["event_id", "succeeded", "error_type", "error_message", "timestamp"]
    if not os.path.exists(log_filepath):
        df = pd.DataFrame(columns=log_columns)
        df.to_csv(log_filepath, index=False)

    logger.info(f"{inspect.currentframe().f_code.co_name}: Completed successfully")

    return log_filepath, log_columns


def main():
    """
    Main execution script.
    """

    start_time = time.time()
    timestamp = datetime.now().strftime("%Y%m%d%H%M")

    # Get ids from argument input
    args = parse_args()
    id_list_file = args.id_file
    flood_ids = read_ids_from_txt(id_list_file)

    # Build logfile name with timestamp, create directory
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)  # Create logs dir if it doesn't already exist
    logger, _ = setup_logger(
        log_dir, log_filename="flood_export_output", timestamp=timestamp, verbose=True
    )

    logger.info("Starting script detect_flooded_pixels.py")
    logger.info(f"ID file: {id_list_file}")

    # Initialize logger csv
    # This tracks every event in a row
    log_csv_filepath, _ = initialize_log_csv(
        logger, log_dir=log_dir, log_filename=f"flood_export_log_{timestamp}.csv"
    )

    # Check that filepaths and directories exist
    check_file_exists(EMDAT_FILEPATH)
    for dir in [DATA_DIR, GAUL_L1_FILEPATH]:
        check_dir_exists(dir)

    # Initialize ee API
    logger.info(f"Initializing ee for project name: {EE_PROJECT_NAME}")
    ee.Initialize(project=EE_PROJECT_NAME)
    logger.info("Initialized ee")

    emdat_floods = build_emdat_geodataframe(logger)

    # Just get events in the flood id subset
    logger.info(f"Filtering to {len(flood_ids)} flood IDs.")
    emdat_floods = emdat_floods[emdat_floods["mon-yr-adm1-id"].isin(flood_ids)]
    logger.info(f"{len(emdat_floods)} matched rows found.")

    logger.info(
        f"Running flood detection algorithm + data export to Google Drive for {len(emdat_floods)} events"
    )
    snooze_button, event_num = 1, 1
    for event_id in flood_ids:
        logger.info(f"Running event {event_num}/{len(flood_ids)}")

        # Check if we have worn out GEE
        if snooze_button % 50 == 0:  # if true - hit the snooze button
            logger.info("Giving GEE a breather for 15 mins")
            time.sleep(900)

        try:
            # Get event corresponding to that ID
            event = emdat_floods[emdat_floods["mon-yr-adm1-id"] == event_id]
            if len(event) < 1:
                raise ValueError(f"No event found for id: {event_id}")
            event = event.iloc[0]

            manage_task_queue(logger)

            # Get adm1 geometry, but reduce to rectangular bounds because it works faster than complex geometries.
            # This includes all the points in the original polygon
            # Code runs much faster this way and avoids EarthEngine crapping out due to complex geometries
            # DO NOT grab the bounds of an ee.Geometry object; this can exceed the payload limit
            # Better to grab the bounds of a shapely object instead
            flood_poly = event["adm1_geometry"]

            # Get Shapely bounds
            (
                xmin,
                ymin,
                xmax,
                ymax,
            ) = flood_poly.bounds  # tuple of (minx, miny, maxx, maxy)

            # Create EE rectangle directly from bounds
            ee_flood_bounds = ee.Geometry.Rectangle([xmin, ymin, xmax, ymax])

            flood_image = process_event(event, event_id, ee_flood_bounds, logger)

            export_event_to_gdrive(
                event_id,
                flood_image,
                ee_flood_bounds,
                drive_export_folder=DRIVE_EXPORT_FOLDER,
                logger=logger,
            )

            # For the logger
            succeeded = True
            error_type = np.nan
            error_message = np.nan

            snooze_button += 1  # Only count it if the image actually exported

        except Exception as e:
            # Event is skipped, but tracked in the logger
            succeeded = False
            error_type = type(e).__name__
            error_message = str(e)
            logger.error(
                f"Failed to process event {event_id}: {error_type} - {error_message}"
            )

        finally:  # Always run this, even if there's an Exception
            logger.info(f"Appending infomation to logger csv: {log_csv_filepath}")
            log_entry = {
                "event_id": event_id,
                "succeeded": succeeded,
                "error_type": error_type,
                "error_message": error_message,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            log_df = pd.DataFrame([log_entry])
            log_df.to_csv(log_csv_filepath, mode="a", index=False, header=False)

            event_num += 1

    # Compute elapsed time
    elapsed = time.time() - start_time
    hours, minutes = divmod(elapsed // 60, 60)
    logger.info("Script complete.")
    logger.info(f"Elapsed time: {int(hours)}h {int(minutes)}m")

    close_logger(logger)


if __name__ == "__main__":
    main()
