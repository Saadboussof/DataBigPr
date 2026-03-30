# TaaSim - Casablanca Urban Mobility

Advanced Big Data Capstone Project. 

## Project Setup

Due to GitHub's file size limits, the large data datasets used for this project are not included in this repository. 
To run the notebooks and pipelines locally, please download the following datasets and place them within the `data/` directory:

- `train.csv` (Porto Taxi Trajectories from Kaggle/UCI)
- `yellow_tripdata_2024-01.parquet` (and other months from NYC TLC Trip Records)
- Other `*.parquet` files.

*Note: The `zone_mapping.csv` is included as it is a small mapping reference.*

## Getting Started
1. Place the required data inside the `data/` folder.
2. Start the services:
   ```bash
   docker-compose up -d
   ```
3. Explore the Jupyter notebooks in the `notebooks/` directory.
