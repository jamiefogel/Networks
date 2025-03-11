import pandas as pd
import os
from config import root
from spec_parser import parse_spec


chosen_spec, market_vars, file_suffix, _3states = parse_spec(root)

# Set base directory paths
base_path =  root + "Code/replicate_mayara/"


base_path = root + "/Code/replicate_mayara"
monopsas_path = f"{base_path}/monopsonies/sas"
ipea_path = f"{base_path}/publicdata/IPEA/IPEA_minwage"
tariffs_path = f"{base_path}/publicdata/Tariffs"
harmonized_path = f"{base_path}/raisdictionaries/harmonized"
other_path = f"{base_path}/publicdata/other"


# Define the range of years
first_year = 1986 # XX Changed from 1985 to 1986
last_year = 2014 # XX 2015

# Placeholder for worker data across years
worker_data = []

# Load crosswalk data for agegroup to age
crosswalk_agegroup_to_age = pd.read_pickle(f"{monopsas_path}/crosswalk_agegroup_to_age.pkl")

for year in range(first_year, last_year + 1):
    print(year)
    try:
        # Try to read the yearly RAIS data
        rais_data = pd.read_parquet(f"{monopsas_path}/rais{year}{_3states}.parquet")
    except FileNotFoundError:
        # Print a warning message if the file does not exist and continue to the next year
        print(f"Warning: File for year {year} not found. Skipping...")
        continue

    if year < 1994:
        # Merge with crosswalk for agegroup
        merged = rais_data.merge(crosswalk_agegroup_to_age, on="agegroup", how="left")
        merged["birthyear"] = year - merged["age"]
        merged["year"] = year
        merged["female"] = (merged["gender"] == 2).astype(int)
        worker_data.append(
            merged[["fakeid_worker", "female", "educ", "birthyear", "year"]].drop_duplicates()
        )

    elif year < 2002 or year > 2010:
        rais_data['age'] = rais_data['age'].astype('Int64')
        rais_data["birthyear"] = year - rais_data["age"]
        rais_data["year"] = year
        rais_data["female"] = (rais_data["gender"] == 2).astype(int)
        worker_data.append(
            rais_data[["fakeid_worker", "female", "educ", "birthyear", "year"]].drop_duplicates()
        )

    else:
        # Extract birthyear from birthdate
        rais_data["birthyear"] = rais_data["birthdate"].astype(str).str[-4:].astype(int)
        rais_data["year"] = year
        rais_data["female"] = (rais_data["gender"] == 2).astype(int)
        worker_data.append(
            rais_data[["fakeid_worker", "female", "educ", "birthyear", "year"]].drop_duplicates()
        )

# Combine all years into a single DataFrame
worker_all_years = pd.concat(worker_data, ignore_index=True)

del rais_data, worker_data


# Separate datasets by trait
gender_data = worker_all_years[worker_all_years["female"].notna()][["fakeid_worker", "female", "year"]]
educ_data = worker_all_years[worker_all_years["educ"].notna()][["fakeid_worker", "educ", "year"]]
birth_data = worker_all_years[worker_all_years["birthyear"].notna()][["fakeid_worker", "birthyear", "year"]]

del worker_all_years

# Sort each dataset by worker and year
gender_data = gender_data.sort_values(by=["fakeid_worker", "year"])
educ_data = educ_data.sort_values(by=["fakeid_worker", "year"])
birth_data = birth_data.sort_values(by=["fakeid_worker", "year"])

# Get the earliest records for each worker
gender_master = gender_data.groupby("fakeid_worker").first().reset_index().drop(columns=["year"])
educ_master = educ_data.groupby("fakeid_worker").first().reset_index().drop(columns=["year"])
birth_master = birth_data.groupby("fakeid_worker").first().reset_index().drop(columns=["year"])

# Merge the traits into a single dataset
rais_worker_traits_master = (
    gender_master.merge(birth_master, on="fakeid_worker", how="left")
    .merge(educ_master, on="fakeid_worker", how="left")
)

# Save the final dataset
rais_worker_traits_master.to_pickle(f"{monopsas_path}/rais_worker_traits_master{_3states}.pkl")

# Clean up temporary dataframes
del  gender_data, educ_data, birth_data, gender_master, educ_master, birth_master
