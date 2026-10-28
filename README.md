# UrbanMove — Data Engineering Portfolio Project

UrbanMove is a hands-on data engineering portfolio project focused on
ingesting, validating, and preparing NYC transportation and city-service
data for analytics.

## Project goals

- Build reusable Python ingestion modules.
- Validate incoming data and handle common API and file errors.
- Store raw source data locally during the initial ingestion phase.
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

- **Taxi zones:** Download and validate CSV schema and identifiers.
- **Weather:** Request hourly weather data, validate dates and timestamps,
  and save CSV output.
- **NYC 311:** Retrieve API records using pagination, validate required
  fields and unique identifiers, and save CSV output.
- **Yellow Taxi trips:** Download monthly Parquet files, validate required
  columns and row counts, reuse valid existing files, and preserve an
  existing destination when a replacement download fails.

Taxi-trip Parquet validation reads data in batches rather than loading the
entire file into a Pandas DataFrame.

## Repository layout

```text
src/urbanmove/ingestion/   Python ingestion modules
tests/                     Automated tests
scripts/                   Data inspection utilities
airflow/dags/              Initial Airflow demonstration DAG
requirements.txt           Python dependencies
pytest.ini                 Pytest configuration
README.md                  Project documentation
```

## Local setup

Use Python 3.12 or a compatible Python version.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run tests

```bash
pytest -v
```

The current test suite covers schema and input validation, empty files,
API errors, pagination, mocked downloads, and selected failure-handling
scenarios.

## Inspect taxi-trip data

After downloading the January 2025 NYC Yellow Taxi Parquet file into
`data/raw/taxi_trips/`, run:

```bash
python scripts/inspect_taxi_data.py
```

Downloaded data under `data/` is excluded from Git.

## Current status and limitations

- Python ingestion modules and automated tests are implemented.
- The current Airflow DAG is a demonstration, not a complete ingestion
  orchestration workflow.
- AWS S3 integration and Databricks lakehouse processing are future
  development stages; they are not yet implemented end to end.
- Data-quality anomalies, including negative fares, zero distances, and
  unusual distances, require explicit business rules before records are
  filtered or quarantined.
- Parquet batch readability checks do not guarantee detection of every
  possible corruption scenario.

## Development principles

- Preserve raw source data.
- Validate schemas and identifiers.
- Use temporary files to avoid publishing incomplete downloads.
- Test failure scenarios without relying on live network services.
- Document limitations rather than silently dropping questionable data.
