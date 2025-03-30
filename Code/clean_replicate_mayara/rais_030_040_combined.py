
import pandas as pd
import numpy as np
import os
from config import root
import sys
from spec_parser import parse_spec


# Paths
monopsas_path =  root + "/Code/clean_replicate_mayara/monopsonies/sas"
crosswalk_path =  root + "/Code/replicate_mayara/monopsonies/sas"

# -------------------------------
# Step 0: Define the processing years
# -------------------------------
years = range(1986, 2001)
_3states = ''

# XX this is where we will add the gamma and iota variables we need to merge in as well
addl_market_vars =[]

# -------------------------------
# Step 1: Load external lookup tables
# -------------------------------
# These files provide the necessary mappings:
# - crosswalk: maps municipality to mmc
# - firm_master: provides firm-level cnae95 information
# - df_valid_cbo94: provides valid occupation codes (for years < and >= 1994)
mmc_codemun_crosswalk   = pd.read_parquet(os.path.join(crosswalk_path, "crosswalk_muni_to_mmc_DK17.parquet"))  
firm_cnae_crosswalk     = pd.read_parquet(os.path.join(crosswalk_path, f"rais_firm_cnae95_master_plus.parquet")).drop_duplicates()
df_valid_cbo94          = pd.read_parquet(os.path.join(crosswalk_path, "valid_cbo94.parquet")).drop_duplicates()

# -------------------------------
# Prepare lists to collect outputs across years
# -------------------------------
firm_collapsed_list = []  # Aggregated firm-level data (grouped by cbo942d)

# -------------------------------
# Process each year in a loop
# -------------------------------
for year in years:
    print(f"Processing year {year}...")
   
    # --- Step 2: Read the raw RAIS data for the current year ---
    # (Assumes a CSV file named "rais_<year>.csv")
    needed_cols = [
        "pis", "cnpj_raiz", "codemun",
        "earningsdecmw", "ibgesubsector", "jid",
        "admmonth", "educ", "agegroup", "empmonths", "earningsavgmw",
        "gender", 'cbo1994'  # either "cbo" or "cbo94"
    ] + addl_market_vars
    
    rais_file = os.path.join(monopsas_path, f"rais_mayara_pull_{year}{_3states}.parquet")
    df = pd.read_parquet( rais_file, columns=needed_cols)
    assert not df.duplicated().any(), "Duplicate Observations found in the dataset"
 
    
    # Flag the modal ibge_subsector for each firm. We will merge this back on below. Group by firm and ibgesubsector to count workers. Note that we are doing this within each year, which I think is what Mayara does. 
    df_grouped = df.groupby(['cnpj_raiz', 'ibgesubsector']).size().reset_index(name='emp')  
    # For each firm, select the row (i.e. the ibgesubsector) with the maximum employee count
    idx = df_grouped.groupby('cnpj_raiz')['emp'].idxmax()  
    # XXJSF: Mayara writes that a small number of firms have estabs in different sectors so she chooses the subsector for the largest establishment so what we do is consistent with that. 
    ibge_df = df_grouped.loc[idx, ['cnpj_raiz', 'ibgesubsector']].copy()
   

    # --- Step 4: Worker-level processing (similar to rais_030.sas) ---
    # Determine which occupation code variable to use based on the year.
   
    # Drop ibgesubsector because we will merge on the one modal one flagged above
    df = df.drop(columns='ibgesubsector')
    
    # Merge raw data with mmc_codemun_crosswalk (to bring in mmc) via municipality code.
    workers = pd.merge(df, mmc_codemun_crosswalk, left_on='codemun', right_on='codemun', how='outer', indicator=True)  
    print(workers._merge.value_counts())
    workers = workers.loc[workers._merge=='both']
    workers.drop(columns='_merge', inplace=True)
    ## XXBM: is it dropping a lot of firms from df? probably there's nothing we can do here
    # Merge with firm_cnae_crosswalk to add cnae95 (join on cnpj_raiz)
    workers = pd.merge(workers, firm_cnae_crosswalk, on='cnpj_raiz', how='outer', indicator=True)  
    print(workers._merge.value_counts())
    workers = workers.loc[workers._merge=='both']
    workers.drop(columns='_merge', inplace=True)
    # Merge with the IBGE data to ensure the firm’s ibgesubsector is based on the largest establishment
    workers = pd.merge(workers, ibge_df, on='cnpj_raiz', how='outer', indicator=True)  
    print(workers._merge.value_counts())
    print(workers.cnpj_raiz.isna().sum())
    workers = workers.loc[workers._merge=='both']
    workers.drop(columns='_merge', inplace=True)
    # Left join with df_valid_cbo94 on the occupation code
    workers = pd.merge(workers, df_valid_cbo94, left_on='cbo1994', right_on='cbo94', how='left', suffixes=('', '_vd'))
   
    
    # Additionally, drop observations if admission month (admmonth) equals 12.
    #if 'admmonth' in workers.columns:
    #    cond = (workers['admmonth'] != 12)
   
    workers_final = workers #workers[cond].copy()
    # Create a binary "female" variable (assuming gender==2 indicates female)
    workers_final['female'] = (workers_final['gender'] == 2).astype(int)
   
    
    # Save to an output file (like "rais_for_earnings_premia{year}.dta" in SAS)
    workers_final.to_parquet(monopsas_path + f"/rais_for_earnings_premia{year}_gamma{_3states}.parquet", index=False)
    workers_final.to_stata(monopsas_path + f"/rais_for_earnings_premia{year}_gamma{_3states}.dta")
    print(f"  -> Output saved to {monopsas_path}/rais_for_earnings_premia{year}_gamma{_3states}.parquet")

   
    # Then, aggregate using the "cbo942d" grouping variable:
    # XX JSF we will want to edit this to not hardcode the market definition we are collapsing by
    firm_collapsed = workers_final.groupby(
        ['cnpj_raiz', 'cnae95', 'ibgesubsector', 'mmc', 'cbo942d']
    ).agg(
        emp=('cnpj_raiz', 'count'),
        totmearnmw=('earningsavgmw', 'sum'),   ## XXBM: these aggregations are for payroll shares?
        totdecearnmw=('earningsdecmw', 'sum'),
        avgmearn=('earningsavgmw', 'mean'),
        avgdecearn=('earningsdecmw', 'mean')
    ).reset_index()
    firm_collapsed['year'] = year
    firm_collapsed_list.append(firm_collapsed)


# This is a firm X market X year data set. CNAE and ibge_subsector are unique within these vars (I think unique within firm)

firm_collapsed_allyears = pd.concat(firm_collapsed_list, ignore_index=True)

assert firm_collapsed_allyears[['cnpj_raiz','year','cbo942d','mmc']].duplicated().sum()==0, "Duplicate Observations found in the dataset"

file_suffix = 'mmc_cbo942d'
collapsed_prefix = f"rais_collapsed_firm_{file_suffix}" 
out_file_parquet = f"{monopsas_path}/{collapsed_prefix}.parquet"
out_file_dta = f"{monopsas_path}/{collapsed_prefix}.dta"
firm_collapsed_allyears.to_parquet(out_file_parquet, index=False)
firm_collapsed_allyears.to_stata(out_file_dta)
print(f"Saved collapsed data to {out_file_dta}")



print("Processing complete.")