import pandas as pd
import os
import numpy as np
from config import root

# Paths
base_path = root + "/Code/replicate_mayara"
monopsas_path = f"{base_path}/monopsonies/sas"

# Paths
base_path = root + "/Code/replicate_mayara"
monopsonies_path = f"{base_path}/monopsonies"
harmonized_path = f"{base_path}/raisdictionaries/harmonized"

# Year range
first_year = 1986 #XX 1985
last_year = 2014 #XX 2015


# -------------------------------------------------------------------
#  STEP 1: cnae macro
#   - For each year, gather fakeid_firm and cnae95 (if >= 1995),
#     else cnae95 = missing.
#   - Append them all
#   - Keep only if cnae95 is in valid_cnae95 (inner join)
#   - Identify missing cnae95 firms
#   - Then keep earliest valid cnae95 per firm
# -------------------------------------------------------------------

all_firms_list = []

for year in range(first_year, last_year + 1):
    fpath = f"{monopsas_path}/rais{year}.parquet"
    if not os.path.exists(fpath):
        print(f"Warning: RAIS file for {year} not found - skipping.")
        continue
    
    if year < 1995:
        # cnaeclass95 doesn't exist pre-1995
        df = pd.read_parquet(fpath, columns=["fakeid_firm"]).drop_duplicates()
        df["cnae95"] = np.nan
    else:
        # Post-1995
        cols = ["fakeid_firm", "cnaeclass95"]
        # read cnaeclass95 if it exists
        try:
            df = pd.read_parquet(fpath, columns=cols).drop_duplicates()
            df = df.rename(columns={"cnaeclass95": "cnae95"})
        except Exception as e:
            print(f"Could not read cnaeclass95 for {year} - skipping.")
            continue
    
    df["year"] = year
    all_firms_list.append(df)

all_firms = pd.concat(all_firms_list, ignore_index=True)

# Read valid CNAE95
valid_cnae95 = pd.read_parquet(f"{monopsas_path}/valid_cnae95.parquet")
valid_cnae95_codes = valid_cnae95["cnae95"].unique()

# Subset only valid cnae (inner join logic)
# In SAS we do an inner join on cnae95 with valid_cnae95.
# But remember cnae95 can be NA for < 1995, so those won't match.
df_valid = all_firms.dropna(subset=["cnae95"])
df_valid = df_valid[df_valid["cnae95"].isin(valid_cnae95_codes)]

# Combine valid-cnae rows
rais_firm_cnae95_master = df_valid.copy()

# Identify missing-cnae firms:
# i.e. those that never appear in rais_firm_cnae95_master
all_firms_unique = all_firms[["fakeid_firm"]].drop_duplicates()
valid_firms      = rais_firm_cnae95_master["fakeid_firm"].drop_duplicates()
missing_firms    = all_firms_unique[~all_firms_unique["fakeid_firm"].isin(valid_firms)].copy()
missing_firms["cnae95"] = np.nan  # to match SAS structure
missing_firms = missing_firms.drop_duplicates()

# Now keep earliest valid cnae95 for each firm (SAS "if first.fakeid_firm then ...")
# Sort by (fakeid_firm, year)
rais_firm_cnae95_master = rais_firm_cnae95_master.sort_values(["fakeid_firm", "year"])
# Drop duplicates to keep earliest year row per firm
rais_firm_cnae95_master = rais_firm_cnae95_master.drop_duplicates(subset=["fakeid_firm"], keep="first")
# We can drop the 'year' column as SAS does
rais_firm_cnae95_master = rais_firm_cnae95_master.drop(columns=["year"])

# Save intermediate results
rais_firm_cnae95_master.to_parquet(f"{monopsonies_path}/rais_firm_cnae95_master.parquet", index=False)
missing_firms.to_parquet(f"{monopsonies_path}/miss_cnae95_firms.parquet", index=False)
print("Step 1 complete: Created rais_firm_cnae95_master and identified missing_cnae95_firms.")


# -------------------------------------------------------------------
#  STEP 2: ibgecross macro
#   - For 1985-1994, gather earliest IBGE-subactivity per firm
#   - Build crosswalk with rais_firm_cnae95_master
#   - Keep most frequent match per ibgesubactivity
# -------------------------------------------------------------------
ibge_list = []
for year in range(1985, 1995):
    fpath = f"{monopsas_path}/rais{year}.parquet"
    if not os.path.exists(fpath):
        print(f"Warning: no file for IBGE subactivity in {year} - skipping.")
        continue
    
    df = pd.read_parquet(fpath, columns=["fakeid_firm", "ibgesubactivity"]).drop_duplicates()
    df = df.dropna(subset=["ibgesubactivity"])
    df["year"] = year
    ibge_list.append(df)

if len(ibge_list) > 0:
    tempibge = pd.concat(ibge_list, ignore_index=True)
    # Sort by (fakeid_firm, year), keep earliest
    tempibge = tempibge.sort_values(["fakeid_firm", "year"])
    tempibge = tempibge.drop_duplicates(subset=["fakeid_firm"], keep="first")
    
    # Build crosswalk with rais_firm_cnae95_master
    # We'll do an inner join on fakeid_firm
    cross_ibge = tempibge.merge(rais_firm_cnae95_master, on="fakeid_firm", how="inner")
    # Now group by (ibgesubactivity, cnae95) and count
    cross_ibge = cross_ibge.groupby(["ibgesubactivity", "cnae95"])["fakeid_firm"].size().reset_index(name="obs")
    # Sort descending by obs, then keep the first row per ibgesubactivity
    cross_ibge = cross_ibge.sort_values(["ibgesubactivity", "obs"], ascending=[True, False])
    cross_ibge = cross_ibge.drop_duplicates(subset=["ibgesubactivity"], keep="first")
    
    cross_ibge = cross_ibge.drop(columns=["obs"])
    cross_ibge.rename(columns={"cnae95": "cnae95_from_ibge"}, inplace=True)
    
    cross_ibge.to_parquet(f"{monopsonies_path}/crosswalk_ibgesubactivity_cnae95.parquet", index=False)
    print("Step 2 complete: Created crosswalk_ibgesubactivity_cnae95.")
else:
    cross_ibge = pd.DataFrame(columns=["ibgesubactivity","cnae95_from_ibge"])
    print("Step 2: No IBGE data found in range 1985–1994, crosswalk empty.")


# -------------------------------------------------------------------
#  STEP 3: cnae20cross macro
#   - For 2006-2015, skipping 2010
#   - Build crosswalk from cnae20 -> cnae95
#   - Keep most frequent match
# -------------------------------------------------------------------
cnae20_list = []
for year in range(2006, 2016):
    if year == 2010:
        continue  # skip year=2010 as SAS does
    fpath = f"{monopsas_path}/rais{year}.parquet"
    if not os.path.exists(fpath):
        print(f"Warning: no file for year={year}, skipping in cnae20 crosswalk.")
        continue
    
    # read cnaeclass20, cnaeclass95
    try:
        df = pd.read_parquet(fpath, columns=["fakeid_firm", "cnaeclass20", "cnaeclass95"]).drop_duplicates()
        df = df.dropna(subset=["cnaeclass20", "cnaeclass95"])
        cnae20_list.append(df)
    except:
        print(f"Could not read cnae20/cnae95 for {year}, skipping.")


if len(cnae20_list) > 0:
    cnae20_df = pd.concat(cnae20_list, ignore_index=True)
    # Group by cnae20, cnae95, count
    cnae20_df = cnae20_df.groupby(["cnaeclass20", "cnaeclass95"]).size().reset_index(name="obs")
    cnae20_df = cnae20_df.sort_values(["cnaeclass20","obs"], ascending=[True, False])
    # Keep top row per cnae20
    cnae20_df = cnae20_df.drop_duplicates(subset=["cnaeclass20"], keep="first")
    
    cnae20_df = cnae20_df.drop(columns=["obs"])
    # Rename cnaeclass95 -> cnae95_from_cnae20
    cnae20_df.rename(columns={"cnaeclass95":"cnae95_from_cnae20"}, inplace=True)
    
    cnae20_df.to_parquet(f"{monopsonies_path}/crosswalk_cnae20_cnae95.parquet", index=False)
    print("Step 3 complete: Created crosswalk_cnae20_cnae95.")
else:
    cnae20_df = pd.DataFrame(columns=["cnaeclass20","cnae95_from_cnae20"])
    print("Step 3: No cnae20 data found in 2006–2015 range, crosswalk empty.")


# -------------------------------------------------------------------
#  STEP 4: assign macro
#   - For missing_firms, see if we can glean an ibgesubactivity from 1985–1994
#     (keep earliest year). Also glean a cnae20 from 2006–2015 (skip 2010)
#     (keep earliest year).
#   - Then merge with the crosswalks:
#       coalesce(ibge_cnae, cnae20_cnae)
#     in that order: IBGE first, then CNAE20
#   - Union that with the existing rais_firm_cnae95_master
# -------------------------------------------------------------------

# A) Earliest IBGE for missing-cnae firms
ibge_miss_list = []
for year in range(1985, 1995):
    fpath = f"{monopsas_path}/rais{year}.parquet"
    if not os.path.exists(fpath):
        continue
    df = pd.read_parquet(fpath, columns=["fakeid_firm", "ibgesubactivity"])
    # keep only missing-firms
    df = df[df["fakeid_firm"].isin(missing_firms["fakeid_firm"])]
    df = df.dropna(subset=["ibgesubactivity"]).drop_duplicates()
    df["year"] = year
    ibge_miss_list.append(df)

if len(ibge_miss_list) > 0:
    ibge_miss = pd.concat(ibge_miss_list, ignore_index=True)
    ibge_miss = ibge_miss.sort_values(["fakeid_firm","year"])
    ibge_miss = ibge_miss.drop_duplicates(subset=["fakeid_firm"], keep="first")
    ibge_miss = ibge_miss.drop(columns=["year"])
else:
    ibge_miss = pd.DataFrame(columns=["fakeid_firm","ibgesubactivity"])

# B) Earliest CNAE20 for missing-cnae firms
cnae20_miss_list = []
for year in range(2006, 2016):
    if year == 2010:
        continue
    fpath = f"{monopsas_path}/rais{year}.parquet"
    if not os.path.exists(fpath):
        continue
    df = pd.read_parquet(fpath, columns=["fakeid_firm", "cnaeclass20"])
    # Keep only missing-firms
    df = df[df["fakeid_firm"].isin(missing_firms["fakeid_firm"])]
    df = df.dropna(subset=["cnaeclass20"]).drop_duplicates()
    df["year"] = year
    cnae20_miss_list.append(df)

if len(cnae20_miss_list) > 0:
    cnae20_miss = pd.concat(cnae20_miss_list, ignore_index=True)
    cnae20_miss = cnae20_miss.sort_values(["fakeid_firm","year"])
    cnae20_miss = cnae20_miss.drop_duplicates(subset=["fakeid_firm"], keep="first")
    cnae20_miss = cnae20_miss.drop(columns=["year"])
else:
    cnae20_miss = pd.DataFrame(columns=["fakeid_firm","cnaeclass20"])

# C) Combine IBGE and CNAE20 in one table (like SAS "misslinks" table)
miss_links = missing_firms.merge(ibge_miss, on="fakeid_firm", how="left") \
                          .merge(cnae20_miss, on="fakeid_firm", how="left")

# D) Merge with the two crosswalks, applying coalesce( ibge-first, cnae20-second )
#    cross_ibge has columns [ibgesubactivity, cnae95_from_ibge]
#    cnae20_df has columns [cnaeclass20, cnae95_from_cnae20]
miss_links = miss_links.merge(cross_ibge, on="ibgesubactivity", how="left") \
                       .merge(cnae20_df, on="cnaeclass20", how="left")

# Now apply coalesce in the correct order: first IBGE, then cnae20
# So if we have cnae95_from_ibge not null, use that; else use cnae95_from_cnae20
miss_links["cnae95"] = np.where(
    miss_links["cnae95_from_ibge"].notna(),
    miss_links["cnae95_from_ibge"],
    miss_links["cnae95_from_cnae20"]
)

# Mark that we assigned these missing cnae95
miss_links["cnae95_assigned"] = np.where(miss_links["cnae95"].notna(), 1, 0)

# E) Union with rais_firm_cnae95_master
temp_assigned = miss_links[miss_links["cnae95_assigned"] == 1].copy()
temp_assigned = temp_assigned[["fakeid_firm","cnae95","cnae95_assigned"]].drop_duplicates()

rais_firm_cnae95_master["cnae95_assigned"] = 0

final_master = pd.concat([
    rais_firm_cnae95_master[["fakeid_firm","cnae95","cnae95_assigned"]],
    temp_assigned
], ignore_index=True)

# (Optional) remove duplicates if any firm ended up in both sets:
final_master = final_master.drop_duplicates(subset=["fakeid_firm","cnae95_assigned"], keep="first")

# Save final
final_master.to_parquet(f"{monopsonies_path}/rais_firm_cnae95_master_plus.parquet", index=False)
print("Step 4 complete: Final master with assigned CNAE95 created.")