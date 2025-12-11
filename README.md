# EM-DAT MODIS Flood Dataset

This repository contains code for spatially and temporally disaggregating flood disaster records from EM-DAT using MODIS satellite imagery. The pipeline processes raw EM-DAT flood events (2000-2024), splits multi-region/multi-month events into admin1-month records, and detects floods using satellite imagery via Google Earth Engine.

## Overview

This work develops methods for:
1. **EM-DAT Event Disaggregation & Geospatial Encoding**: Splitting multi-region/multi-month events into admin1-month records and matching each to its corresponding GAUL administrative region 1 polygon
2. **Flood Detection**: Using an adapted version of the [Cloud2Street flood detection algorithm](https://github.com/cloudtostreet/MODIS_GlobalFloodDatabase) to create floodmaps for each admin1-month event
3. **Visualization & Analysis**: Tools for creating flood maps, summary statistics, and comparing EM-DAT vs MODIS-derived metrics

### Final Dataset Output

The pipeline produces `emdat_modis_flood_dataset.csv` containing MODIS-derived flood metrics for disaggregated EM-DAT events (2000-2024):
- **Temporal resolution**: Admin1-month level
- **Spatial resolution**: MODIS 250m imagery
- **Key metrics**: Flooded population, flooded area, normalized flooded area
- **Quality flags**: Data quality indicators for each event

## Data Sources

- **EM-DAT**: International disaster database providing flood event records (2000-2024)
- **MODIS**: Satellite imagery (Terra/Aqua) for flood detection via Google Earth Engine
- **GAUL 2015**: Global Administrative Unit Layers (admin level 1 boundaries)
- **GPW**: NASA's Gridded Global Population of the World dataset

## Repository Structure

```
├── dataset_generation/           # Data processing pipeline
│   ├── disaggregate_emdat.py
│   ├── detect_flooded_pixels.py
│   ├── extract_flood_metrics.py
│   ├── combine_csvs.py
│   ├── dataset_postprocessing.py # Data quality flags and cleanup
│   ├── compute_adm1_summary_stats.py  # Generate admin1 summary statistics
│   ├── utils/                    # Helper modules (flood detection, MODIS toolbox, etc.)
│   └── generate_input_files/     # Scripts for creating batch input files
│
├── figure_generation/            # Visualization and analysis scripts
│   ├── visualize_floodmap.py     # Create flood map visualizations
│   ├── summary_maps.py           # Generate admin1 choropleth maps
│   ├── emdat_modis_regression.py # Compare EM-DAT vs MODIS metrics
│   ├── event_duration_violinplot.py  # Event duration distributions
│   └── data_analysis_utils.py    # Shared plotting utilities
│
├── data/                         # Data files (not tracked in git, except flags)
│   ├── data_processing_flags.csv      # Flag definitions
│   ├── emdat_modis_flood_dataset.csv  # Final dataset output
│   └── adm1_summary_stats.csv         # Admin1-level summary statistics
│
├── environment.yml               # Conda environment specification
└── LICENSE                       # MIT License
```

## Installation

### Environment Setup

This project uses conda for dependency management:

```bash
conda env create -f environment.yml
conda activate flood-impacts
```

### Key Dependencies

- **Geospatial**: `geopandas`, `rasterio`, `xarray`, `cartopy`
- **Earth Engine**: `earthengine-api` (requires GEE account for flood detection step)
- **Data Processing**: `pandas`, `numpy`, `scipy`
- **Parallel processing**: `dask`, `exactextract`

See `environment.yml` for complete dependency list with version specifications.

## Pipeline Steps

### 1. Disaggregate EM-DAT Events
**Script:** `disaggregate_emdat.py`

Preprocesses raw EM-DAT flood records and disaggregates events by admin1 zone and month. This script:
- Filters for inland floods (2000-2024)
- Removes events with missing critical date information
- Adjusts 2024 damages using CPI
- Matches EM-DAT location strings to GAUL admin1 codes
- Splits multi-month/multi-region events into individual admin1-month records

**Inputs:**
- `data/emdat/emdat-2000-2024.csv` (raw EM-DAT download from [emdat.be](https://www.emdat.be/))
- GAUL Level 2 shapefile (`data/GAUL_2015/g2015_2014_2`)

**Outputs:**
- `data/emdat/emdat_floods_by_mon_yr_adm1.csv`

**Helper modules:**
- `utils/emdat_toolbox.py`

---

### 2. Generate Input Files for Parallel Processing
**Scripts:** `generate_input_files/*.py`

Creates text files that batch event IDs for parallel processing.

Run all scripts in `generate_input_files/`:
- `split_emdat_ids_into_batches.py`
- `generate_adm1_code_inputs.py`
- `generate_year_day_file.py`

**Inputs:**
- `data/emdat/emdat_floods_by_mon_yr_adm1.csv` — *output from Step 1*

**Outputs:**
- Text files saved to `text_inputs/` subdirectories

---

### 3. Detect Flooded Pixels Using Google Earth Engine
**Script:** `detect_flooded_pixels.py`

Uses the Google Earth Engine (GEE) Python API to run the Cloud2Street MODIS flood detection algorithm for each disaggregated flood event. Exports 4-band GeoTIFF images to Google Drive (flooded, duration, clear_views, clear_perc_scaled).

**Requires:** GEE account and authentication (`earthengine authenticate`)

**Inputs:**
- Text input files from `text_inputs/emdat_mon_yr_adm1_id/` — *outputs from Step 2*
- `data/emdat/emdat_floods_by_mon_yr_adm1.csv` — *output from Step 1*
- GAUL Level 1 shapefile

**Outputs:**
- GeoTIFF files exported to Google Drive
- `.log` file recording script execution
- `.csv` file summarizing success/failure for each event

**Helper modules:**
- `utils/flood_detection.py` (Cloud2Street algorithm)
- `utils/modis_toolbox.py`
- `utils/logger.py`

**Example usage:**
```bash
python detect_flooded_pixels.py ../text_inputs/emdat_mon_yr_adm1_id/emdat_mon_yr_adm1_id_1.txt
```

---

### 4. Extract Flood Metrics from Images
**Script:** `extract_flood_metrics.py`

Downloads GEE-exported flood images and computes zonal statistics (flooded area, flooded population, cloud cover metrics) for each event.

**Inputs:**
- MODIS flood GeoTIFF images — *outputs from Step 3*
- GAUL Level 1 shapefile
- Text input files from `text_inputs/emdat_mon_yr_adm1_id/`

**Outputs:**
- CSV files named `<mon-yr-adm1-id>_metrics.csv` for each event

---

### 5. Combine Flood Metric CSVs
**Script:** `combine_csvs.py`

Merges individual event metric CSV files into a single consolidated file.

**Inputs:**
- Individual `*_metrics.csv` files — *outputs from Step 4*

**Outputs:**
- `data/event_intermediate_files/event_metrics.csv`

---

### 6. Dataset Postprocessing
**Script:** `dataset_postprocessing.py`

Consolidates data quality checks, flag assignment, and final dataset preparation. This script:
- Adds data quality flags (1-15) indicating missing data, processing issues, and impact allocation methods
- Corrects country assignment mismatches between EM-DAT and GAUL
- Sorts events to match original EM-DAT ordering
- Produces the final cleaned dataset

**Inputs:**
- `data/event_intermediate_files/event_metrics.csv` — *output from Step 5*
- `data/emdat/emdat_floods_by_mon_yr_adm1.csv` — *output from Step 1*
- `data/emdat/emdat-2000-2024.csv` — *original EM-DAT data*

**Outputs:**
- `data/emdat_modis_flood_dataset.csv` — **Final dataset**

**Dataset Columns:**
- `mon-yr-adm1-id`: Unique identifier for each admin1-month flood event
- `id`: Original EM-DAT event ID
- `mon-yr`: Month-year of event
- `start_date`, `end_date`: Event temporal bounds
- `ISO`: Country code
- `adm1_code`: GAUL admin1 region code
- `flooded_population`: Estimated population in flooded areas
- `flooded_area`: Flooded area (km²)
- `flooded_area_norm`: Flooded area normalized by admin1 area
- `flags`: Data quality flags (see `data/data_processing_flags.csv`)

---

### 7. Generate Admin1 Summary Statistics (Optional)
**Script:** `compute_adm1_summary_stats.py`

Aggregates flood events by admin1 region to compute summary statistics for mapping and analysis.

**Inputs:**
- `data/emdat_modis_flood_dataset.csv` — *output from Step 6*

**Outputs:**
- `data/adm1_summary_stats.csv`

**Summary Statistics:**
- Mean flooded population per admin1
- Mean flooded area per admin1
- Mean normalized flooded area per admin1
- Total event count per admin1

---

## Visualization and Analysis

The `figure_generation/` directory contains scripts for visualizing and analyzing the flood dataset.

### Visualize Individual Flood Maps
**Script:** `visualize_floodmap.py`

Creates map visualizations of individual flood events overlaid on OpenStreetMap basemaps.

**Usage:**
```bash
python visualize_floodmap.py ../data/example_floodmaps/02-2020-0089-USA-3238.tif
python visualize_floodmap.py ../data/example_floodmaps/02-2020-0089-USA-3238.tif --show
```

**Configuration:** Edit global variables at top of script to customize:
- `FLOOD_VARIABLE`: Variable to plot ("flooded", "duration", "clear_views", "clear_perc_scaled")
- `SHOW_ADMIN_BOUNDARY`: Toggle admin1 boundary display
- `FIG_DPI`: Output resolution
- `FLOODMAP_DIR`: Output directory

**Outputs:** PNG maps saved to `figs/floodmaps/`

---

### Generate Admin1 Summary Maps
**Script:** `summary_maps.py`

Creates global choropleth maps showing admin1-level flood statistics.

**Requires:** `data/adm1_summary_stats.csv` (generated by `compute_adm1_summary_stats.py`)

**Outputs:** Four maps saved to `figs/adm1_maps/`:
- Mean flooded population
- Mean flooded area
- Mean normalized flooded area
- Total event count

---

### Compare EM-DAT vs MODIS Metrics
**Script:** `emdat_modis_regression.py`

Creates scatter plots comparing EM-DAT reported impacts with MODIS-derived flood metrics.

**Outputs:** Regression plot saved to `figs/`

---

### Event Duration Analysis
**Script:** `event_duration_violinplot.py`

Compares event duration distributions between floods with zero vs non-zero flooded area.

**Outputs:** Violin plot saved to `figs/`

---

## Key Helper Modules

Located in `utils/`:

- **`flood_detection.py`**: Cloud2Street MODIS flood detection algorithm (adapted)
- **`modis_toolbox.py`**: MODIS preprocessing (pan-sharpening, QA masking, slope/water masks)
- **`emdat_toolbox.py`**: EM-DAT preprocessing utilities (date parsing, admin unit expansion)
- **`logger.py`**: Logging setup
- **`utils_misc.py`**: General utilities (file checks, year mapping, etc.)

---

## Data Quality Flags

The dataset includes quality flags (1-15) indicating:
- Missing or estimated data (dates, locations, impacts)
- Processing issues (no MODIS data, cloud cover problems)
- Impact allocation methods (population-weighted vs. reported)

See `data/data_processing_flags.csv` for complete flag definitions.

---

## Citation

If you use this code or data, please cite:

> Keeney, N. (2025). *Flood Disaster Impacts: EM-DAT and MODIS Analysis*. Master's Thesis, Department of Civil & Environmental Engineering, Colorado State University.

Presented at the American Geophysical Union (AGU) Fall Meeting 2025.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Issues and Support

If you encounter any problems with the code, data, or documentation, please open an issue on GitHub.
