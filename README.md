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

## Upload Raw Data To MinIO (Data Lake)

This project includes MinIO as local S3-compatible storage and auto-creates the `raw` bucket.

To upload local raw files from `data/` into MinIO:

```powershell
./scripts/upload_raw_to_minio.ps1
```

The script uploads:
- `train.csv` to `s3a://raw/porto/train.csv`
- `yellow_tripdata_*.parquet` to `s3a://raw/nyc/`
- `zone_mapping.csv` to `s3a://raw/reference/zone_mapping.csv` (if present)

Optional dry-run (no upload, only prints command):

```powershell
./scripts/upload_raw_to_minio.ps1 -DryRun
```

## Run Python Components With MinIO Data

The Python pipeline components now default to MinIO paths:

- Spark transformation: reads `s3a://raw/porto/train.csv` and writes `s3a://curated/porto/porto_casablanca_parquet`
- GPS producer: reads `s3a://raw/porto/train.csv`

If you run the GPS producer directly, install `boto3` first:

```bash
pip install boto3
```

If running outside Docker, set MinIO endpoint/credentials (example in PowerShell):

```powershell
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "admin"
$env:MINIO_SECRET_KEY = "password"
```
