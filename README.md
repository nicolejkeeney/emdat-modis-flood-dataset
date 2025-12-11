# EM-DAT MODIS Flood Dataset

This repository contains code for spatially and temporally disaggregating flood disaster records from EM-DAT using MODIS satellite imagery. The pipeline processes raw EM-DAT flood events (2000-2024), splits multi-region/multi-month events into admin1-month records, and detects floods using satellite imagery via Google Earth Engine.

Floodmaps are generated for each disagreggated event. For example, these visualizations of floodmaps from three different flood events:   <br>  
<img src="figure_generation/figs/floodmaps/10-2023-0651-USA-3244_flooded.png" width="250" style="display:inline-block; margin-right:5px;">
<img src="figure_generation/figs/floodmaps/09-2024-0648-CMR-818_flooded.png" width="250" style="display:inline-block; margin-right:5px;">
<img src="figure_generation/figs/floodmaps/02-2020-0089-USA-3238_flooded.png" width="250" style="display:inline-block;">

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
