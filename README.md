# EM-DAT MODIS Flood Dataset
[![DOI](https://zenodo.org/badge/1114140425.svg)](https://doi.org/10.5281/zenodo.17905100)

This repository contains code for spatially and temporally disaggregating flood disaster records from EM-DAT using MODIS satellite imagery. The pipeline processes raw EM-DAT flood events (2000-2024), splits multi-region/multi-month events into admin1-month records, and creates flood maps for each disaggregated event using satellite imagery via Google Earth Engine.

Flood maps are generated for each disagreggated event. For example, the images below show flood maps from two different flood events:   <br>  
<img src="figure_generation/figs/floodmaps/10-2023-0651-USA-3244_flooded.png" width="250" style="display:inline-block; margin-right:5px;">
<img src="figure_generation/figs/floodmaps/09-2024-0648-CMR-818_flooded.png" width="250" style="display:inline-block; margin-right:5px;">

## Overview

This work develops methods for:
1. **EM-DAT Event Disaggregation & Geospatial Encoding**: Splitting multi-region/multi-month events into admin1-month records and matching each to its corresponding GAUL administrative region 1 polygon
2. **Flood Detection**: Using an adapted version of the [Cloud2Street flood detection algorithm](https://github.com/cloudtostreet/MODIS_GlobalFloodDatabase) to create flood maps for each admin1-month event
3. **Visualization & Analysis**: Tools for creating flood maps, summary statistics, and comparing EM-DAT vs MODIS-derived metrics

### Final Dataset Output

The pipeline produces `emdat_modis_flood_dataset.csv` containing MODIS-derived flood metrics for disaggregated EM-DAT events (2000-2024):
- **Temporal/spatial resolution**: Admin1-month level for each flood event
- **Key metrics**: Flooded population, flooded area, normalized flooded area
- **Quality flags**: Data quality indicators for each event

## Data Sources

- **EM-DAT**: International disaster database providing flood event records (2000-2024)
- **MODIS**: Satellite imagery (Terra/Aqua) for flood detection via Google Earth Engine
- **GAUL 2015**: Global Administrative Unit Layers (admin level 1 & 2 boundaries)
- **GPW**: NASA's Gridded Global Population of the World dataset

## Repository Structure

```
├── dataset_generation/           # Data processing pipeline
│   ├── disaggregate_emdat.py     # Perform flood event disaggregation and GAUL polygon matching 
│   ├── detect_flooded_pixels.py  # Create flood maps 
│   ├── extract_flood_metrics.py  # Compute flood map metrics: flooded population, flooded area, etc. 
│   ├── dataset_postprocessing.py # Data quality flags and cleanup
│   ├── compute_adm1_summary_stats.py  # Generate admin1 summary statistics
|   ├── split_emdat_ids_into_batches.py # Generate text inputs for batch processing of flood maps 
│   ├── utils/                    # Helper modules (flood detection, MODIS toolbox, etc.)
│
├── figure_generation/            # Visualization and analysis scripts
│   ├── visualize_floodmap.py     # Create flood map visualizations
│   ├── summary_maps.py           # Generate admin1 choropleth maps
│   ├── emdat_modis_regression.py # Compare EM-DAT vs MODIS metrics
│   ├── event_duration_violinplot.py  # Event duration distributions
│   └── data_analysis_utils.py    # Shared plotting utilities
│
├── data/                        
│   ├── data_processing_flags.csv      # Flag definitions
│   ├── emdat_modis_flood_dataset.csv  # Final dataset output
│   ├── emdat_2000_2024.csv            # Manually cleaned EM-DAT flood records
│   └── adm1_summary_stats.csv         # Admin1-level summary statistics
│
├── environment.yml               # Conda environment specification
└── LICENSE                       # MIT License
```

## Installation

This project uses conda for dependency management:

```bash
conda env create -f environment.yml
conda activate flood-impacts
```
