# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 09:57:38 2025

@author: p13861161
"""
import os
import pandas as pd
from config import root
import sys
from spec_parser import parse_spec


chosen_spec, market_vars, file_suffix, _3states = parse_spec(root)


# Paths
base_path = root + "/Code/replicate_mayara"
monopsas_path = f"{base_path}/monopsonies/sas"



# Crosswalk file: municipality_rais -> mmc
crosswalk_path = os.path.join(monopsas_path, "crosswalk_muni_to_mmc_DK17.parquet")

# Year range
first_year = 1985
last_year  = 2015

# ------------------------------------------------
#  1. Read crosswalk: municipality -> (municipality, mmc)
# ------------------------------------------------
crosswalk_muni = pd.read_parquet(crosswalk_path)  
# This file should have at least: ["municipality_rais", "mmc"]

# ------------------------------------------------
#  2. For each year, create location data
# ------------------------------------------------
location_list = []

for year in range(first_year, last_year + 1):
    rais_file = os.path.join(monopsas_path, f"rais{year}{_3states}.parquet")
    if not os.path.exists(rais_file):
        print(f"Warning: RAIS file for year {year} not found. Skipping.")
        continue
    
    # Read minimal columns from rais data
    df_rais = pd.read_parquet(rais_file, columns=["fakeid_estab", "municipality"]).drop_duplicates()
    
    # Left join with crosswalk
    merged = df_rais.merge(
        crosswalk_muni, 
        left_on="municipality", 
        right_on="codemun",
        how="left"
    )
    
    # We want final columns to match the SAS macro:
    # a.fakeid_estab,
    # b.municipality_rais as municipality,
    # b.mmc,
    # year
    # Also keep them distinct
    merged = merged.drop_duplicates(subset=["fakeid_estab", "codemun", "mmc"])
    merged.drop(columns='municipality', inplace=True)
    merged = merged.rename(columns={"codemun": "municipality"})
    merged["year"] = year
    
    location_list.append(merged[["fakeid_estab", "municipality", "mmc", "year"]])

# Combine all years
location_df = pd.concat(location_list, ignore_index=True)

# ------------------------------------------------
#  3. Split into "estabmiss" vs. "estabhas" 
#     (SAS checks for municipality = . or last4='9999')
# ------------------------------------------------
def is_missing_muni(m):
    """
    Replicates the SAS logic:
      if municipality is null OR
         last 4 digits are '9999',
      then it's missing.
    """
    if pd.isna(m):
        return True
    # Convert numeric code to 6-digit string (like put(municipality, 6.) in SAS)
    m_str = str(int(m)).rjust(6, "0")
    # last4 = substring from position 3..6 (1-based in SAS)
    if m_str[2:] == "9999":
        return True
    return False

location_df["is_miss"] = location_df["municipality"].apply(is_missing_muni)

estabmiss_df = location_df[location_df["is_miss"]]
estabhas_df  = location_df[~location_df["is_miss"]]

# ------------------------------------------------
#  4. For estabhas: keep earliest year per fakeid_estab
# ------------------------------------------------
estabhas_df = estabhas_df.sort_values(["fakeid_estab", "year"])
estabhas_df = estabhas_df.drop_duplicates(subset=["fakeid_estab"], keep="first")

# Mimic SAS final dataset: monopsas.rais_estab_location_master
# Drop columns not in final SAS dataset (year, is_miss)
rais_estab_location_master = estabhas_df.drop(columns=["year", "is_miss"])

# ------------------------------------------------
#  5. Identify establishments that never have a valid municipality
# ------------------------------------------------
estabmiss_unique = estabmiss_df["fakeid_estab"].drop_duplicates()
estabhas_unique  = rais_estab_location_master["fakeid_estab"].drop_duplicates()

# In SAS, monopsas.miss_location_estabs is distinct estabs in estabmiss 
# that do NOT appear in monopsas.rais_estab_location_master
missing_estab_ids = estabmiss_unique[~estabmiss_unique.isin(estabhas_unique)]

miss_location_estabs = pd.DataFrame({"fakeid_estab": missing_estab_ids})

# ------------------------------------------------
#  6. (Optional) Save or export as needed
# ------------------------------------------------
# Example: save as parquet
output_dir = os.path.join(monopsas_path, "outputs")
os.makedirs(output_dir, exist_ok=True)

rais_estab_location_master.to_parquet(
    os.path.join(output_dir, "rais_estab_location_master{_3states}.parquet"),
    index=False
)

miss_location_estabs.to_parquet(
    os.path.join(output_dir, "miss_location_estabs{_3states}.parquet"),
    index=False
)

print("Done. 'rais_estab_location_master' and 'miss_location_estabs' created.")
