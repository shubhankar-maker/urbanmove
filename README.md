# UrbanMove — Data Engineering Portfolio Project

UrbanMove is a hands-on data engineering project focused on ingesting,
validating, and preparing NYC transportation and city-service data for
analytics.

## Project goals

- Build reusable Python ingestion modules.
- Validate incoming data and handle common API and file errors.
- Write raw source data to local storage as the initial ingestion stage.
- Test ingestion behavior using pytest.
- Extend the pipeline toward orchestration, cloud storage, and lakehouse
  processing in later phases.

## Data sources

- NYC Yellow Taxi trip records (Parquet)
- NYC taxi zone lookup (CSV)
- Open-Meteo historical weather API
- NYC 311 service requests API

## Current implementation

Python ingestion modules are located in `src/urbanmove/ingestion/`.

Implemented ingestion capabilities include:

- Taxi zones: download and validate the required CSV schema and identifiers.
- Weather: request hourly weather data, validate dates and timestamps,
  and save the resulting CSV.
- NYC 311: retrieve records using API pagination, validate required fields
  and unique identifiers, and save the resulting CSV.
- Yellow Taxi trips: download monthly Parquet files, validate required
  columns and row counts, reuse valid existing files, and preserve an
  existing destination if a replacement download fails.

Taxi-trip Parquet validation reads data in batches to check readability
without loading the entire file into a Pandas DataFrame.

## Repository layout

```text
src/urbanmove/ingestion/   Python ingestion modules
tests/                    Automated tests
scripts/                  Data inspection utilities
airflow/dags/              Initial Airflow demonstration DAG
requirements.txt          Python dependencies
pytest.ini                Pytest configuration
