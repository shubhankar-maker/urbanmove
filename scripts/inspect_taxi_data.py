from pathlib import Path
import pandas as pd

data_dir = Path("data/raw/taxi_trips")
files = list(data_dir.glob("*.parquet"))

if not files:
    print("No taxi-trip Parquet files found.")
else:
    file_path = files[0]
    print(f"Inspecting: {file_path}")

    df = pd.read_parquet(file_path)

    print("\nShape:")
    print(df.shape)

    print("\nColumns and data types:")
    print(df.dtypes)

    print("\nFirst 5 rows:")
    print(df.head())

    print("\nMissing values per column:")
    print(df.isna().sum())

    print("\nExact duplicate rows:")
    print(df.duplicated().sum())

    print("\nPickup timestamp range:")
    print(df["tpep_pickup_datetime"].min())
    print(df["tpep_pickup_datetime"].max())

    print("\nTrip distance summary:")
    print(df["trip_distance"].describe())


    print("\nInvalid trip checks:")

    print(
        "Zero or negative distances:",
        (df["trip_distance"] <= 0).sum(),
    )

    print(
        "Negative fares:",
        (df["fare_amount"] < 0).sum(),
    )

    print(
        "Negative total amounts:",
        (df["total_amount"] < 0).sum(),
    )

    print(
        "Drop-off before pickup:",
        (
            df["tpep_dropoff_datetime"]
            < df["tpep_pickup_datetime"]
        ).sum(),
    )
