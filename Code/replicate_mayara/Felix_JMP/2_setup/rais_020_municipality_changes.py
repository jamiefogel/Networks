import pandas as pd
from config import root
import os
from spec_parser import parse_spec


chosen_spec, market_vars, file_suffix, _3states = parse_spec(root)

# Paths
base_path = root + "/Code/replicate_mayara"
monopsas_path = f"{base_path}/monopsonies/sas"

# Year range
first_year = 1986 # XX 1985
last_year = 2015 #XX 2015


# ********** Identify Municipality Code Changes **********
all_location_changes = []  # Placeholder for location changes data

for year in range(first_year, last_year):
    print(year)
    next_year = year + 1

    # Load RAIS data for consecutive years (deduplicate fakeid_estab + municipality)
    if not os.path.exists(f"{monopsas_path}/rais{year}{_3states}.parquet") or not os.path.exists(f"{monopsas_path}/rais{next_year}{_3states}.parquet"):
        print(f'File not found. Skipping {year}')
        continue
    rais_base = pd.read_parquet(f"{monopsas_path}/rais{year}{_3states}.parquet", columns=["fakeid_estab", "municipality"])
    rais_next = pd.read_parquet(f"{monopsas_path}/rais{next_year}{_3states}.parquet", columns=["fakeid_estab", "municipality"])

    # Deduplicate to avoid Cartesian product
    rais_base = rais_base.drop_duplicates(subset=["fakeid_estab", "municipality"])
    rais_next = rais_next.drop_duplicates(subset=["fakeid_estab", "municipality"])

    # Merge on establishment ID (fakeid_estab)
    merged = rais_base.merge(
        rais_next,
        on="fakeid_estab",
        suffixes=("_base", "_out")
    )

    # Filter rows where municipalities are different and not null
    filtered = merged[
        (merged["municipality_base"] != merged["municipality_out"]) &
        merged["municipality_base"].notna() &
        merged["municipality_out"].notna()
    ]

    # Group by base and out municipalities, count the establishments
    grouped = (
        filtered
        .groupby(["municipality_base", "municipality_out"], as_index=False)
        .agg(tot_firms=("fakeid_estab", "count"))
    )

    # Add year information
    grouped["base_year"] = year
    grouped["out_year"] = next_year

    # Append to the results list
    all_location_changes.append(grouped)


# Combine results for all years
location_changes = pd.concat(all_location_changes, ignore_index=True)

# Save to Parquet
location_changes.to_parquet(f"{monopsas_path}/rais_municipality_changes{_3states}.parquet", index=False)


# ********** Number of Observations by Municipality **********
all_muni_counts = []  # Placeholder for municipality counts

for year in range(first_year, last_year + 1):
    print(year)
    if not os.path.exists(f"{monopsas_path}/rais{year}{_3states}.parquet"):
        print(f'File not found. Skipping {year}')
        continue
    # Load RAIS data for the year
    rais_year = pd.read_parquet(f"{monopsas_path}/rais{year}{_3states}.parquet", columns=["fakeid_worker", "municipality"])

    # Group by municipality and count observations
    grouped = rais_year.groupby("municipality", as_index=False).agg(tot_obs=("fakeid_worker", "count"))
    grouped["year"] = year

    all_muni_counts.append(grouped)

# Combine results for all years
municipality_counts = pd.concat(all_muni_counts, ignore_index=True)

# Save to Parquet
municipality_counts.to_parquet(f"{monopsas_path}/rais_obs_by_municipality_year{_3states}.parquet", index=False)

print("Processing complete. Output files saved in:", monopsas_path)
