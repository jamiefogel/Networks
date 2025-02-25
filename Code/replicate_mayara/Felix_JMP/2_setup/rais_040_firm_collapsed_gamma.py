"""
This script aggregates the worker‐level earnings premia data (produced in earlier steps)
to create a firm‐level (or, in the original version, a market defined by the intersection of mmc and cbo942d)
dataset with market-level variables. The output from this script is used in subsequent estimation steps.

"""

import pandas as pd
import numpy as np
import os
from config import root

# -------------------------------------------------------------------
# Global variables that will be updated for each run.
# -------------------------------------------------------------------
USE_GAMMA = None    # Will be set in main()
market_var = None
level2_default = None
collapsed_prefix = None

# -------------------------------------------------------------------
# Paths from your preamble
# -------------------------------------------------------------------
base_path = root + "/Code/replicate_mayara"
monopsas_path = f"{base_path}/monopsonies/sas"

# -------------------------------------------------------------------
# 1. Emulate the %valid(i=) macro
# -------------------------------------------------------------------
def valid(year):
    """
    For the given year, this function:
      1) Reads in the RAIS data and selects relevant columns.
      2) Filters observations based on education, municipality, earnings, ibgesubsector, and age.
      3) Groups observations by unit and ibgesubsector to determine each unit’s largest establishment.
         In the original version the unit is fakeid_firm; in both versions we continue to use fakeid_firm.
      4) Merges with crosswalks and master files to build the final valid dataset.
 
    The final dataset contains key variables such as fakeid_worker, fakeid_firm, cnae95, ibgesubsector, and 
    the market variable. For the original version, the market variable is mmc; for the gamma version, it is gamma.
    """
    print(f"\n--- Running valid() for year={year} ---")
    
    # (A) Decide occupation variable based on year
    if (year < 1994) or (year == 2002):
        cboraw = "cbo"
        cboclean = "cbo94"
        validdata = "valid_cbo94"   
    elif year < 2010:
        cboraw = "cbo"  # renamed version
        cboclean = "cbo94"
        validdata = "valid_cbo94"
    else:
        cboraw = "cbo02"
        cboclean = "cbo02"
        validdata = "crosswalk_cbo02_cbo94"

    # (B) Read RAIS file
    rais_file = os.path.join(monopsas_path, f"rais{year}.parquet")
    if not os.path.exists(rais_file):
        print(f"  -> rais{year}.parquet not found; skipping.")
        return None
    df_rais = pd.read_parquet(
        rais_file,
        columns=[
            "fakeid_firm", "fakeid_worker",
            "ibgesubsector", "educ", "municipality",
            "earningsdecmw", "agegroup", 
            "jid", "gamma",
            "earningsavgmw",
            cboraw
        ]
    ).drop_duplicates()
    
    # (C) Filter observations
    mask = (
        df_rais["educ"].notna() & df_rais["educ"].between(1, 11, inclusive="both") &
        df_rais["municipality"].notna() &
        df_rais["earningsdecmw"].notna() & (df_rais["earningsdecmw"] > 0) &
        df_rais["ibgesubsector"].notna() & (df_rais["ibgesubsector"] != 24) &
        df_rais["agegroup"].between(3, 7, inclusive="both")
    )
    temp = df_rais[mask].copy()
    
    # (D) Group by fakeid_firm and ibgesubsector to count workers.
    group = temp.groupby(["fakeid_firm", "ibgesubsector"], as_index=False).agg(
        emp=("fakeid_worker", "count")
    )
    group["maxemp"] = group.groupby("fakeid_firm")["emp"].transform("max")
    ibge = group[group["emp"] == group["maxemp"]].drop_duplicates("fakeid_firm")
    del temp, group
    
    if ibge.empty:
        print("  -> No valid ibge data for this year after filtering.")
        return None
    
    # (E) Build final valid{year} table by merging with crosswalks and master files.
    cross_muni_file = os.path.join(monopsas_path, "crosswalk_muni_to_mmc_DK17.parquet")
    if not os.path.exists(cross_muni_file):
        print("  -> crosswalk_muni_to_mmc_DK17 missing.")
        return None
    df_cross_muni = pd.read_parquet(cross_muni_file).drop_duplicates()
    
    cnae95_master_file = os.path.join(monopsas_path, "rais_firm_cnae95_master_plus.parquet")
    if not os.path.exists(cnae95_master_file):
        print("  -> rais_firm_cnae95_master_plus missing.")
        return None
    df_cnae95 = pd.read_parquet(cnae95_master_file).drop_duplicates()
    
    valid_data_file = os.path.join(monopsas_path, f"{validdata}.parquet")
    if not os.path.exists(valid_data_file):
        print(f"  -> {validdata}.parquet missing.")
        return None
    df_valid_cbo = pd.read_parquet(valid_data_file).drop_duplicates()
    
    df_rais_full = pd.read_parquet(
        rais_file,
        columns=[
            "fakeid_worker", "fakeid_firm", "ibgesubsector", "educ",
            "municipality", "earningsdecmw", "earningsavgmw",
            "agegroup", cboraw, "jid", "gamma"
        ]
    ).drop_duplicates()
    
    mask_full = (
        df_rais_full["educ"].notna() & df_rais_full["educ"].between(1, 11, inclusive="both") &
        df_rais_full["municipality"].notna() &
        df_rais_full["earningsdecmw"].notna() & (df_rais_full["earningsdecmw"] > 0) &
        df_rais_full["ibgesubsector"].notna() & (df_rais_full["ibgesubsector"] != 24) &
        df_rais_full["agegroup"].between(3, 7, inclusive="both")
    )
    df_rais_filtered = df_rais_full[mask_full].copy()
    
    # Merge 1: with crosswalk_muni_to_mmc_DK17 on municipality=codemun
    merged1 = df_rais_filtered.merge(
        df_cross_muni[["codemun", "mmc"]],
        left_on="municipality",
        right_on="codemun",
        how="inner"
    )
    merged1.drop(columns=["codemun"], inplace=True)
    
    # Merge 2: with firm CNAE master on fakeid_firm
    merged2 = merged1.merge(
        df_cnae95[["fakeid_firm", "cnae95"]],
        on="fakeid_firm",
        how="inner"
    )
    
    # Merge 3: with ibge on (fakeid_firm, ibgesubsector)
    merged3 = merged2.merge(
        ibge[["fakeid_firm", "ibgesubsector"]],
        on=["fakeid_firm", "ibgesubsector"],
        how="inner"
    )
    
    # Merge 4: with valid occupation data on (cboraw) to obtain cbo942d
    merged4 = merged3.merge(
        df_valid_cbo[[cboclean, "cbo942d"]],
        left_on=cboraw,
        right_on=cboclean,
        how="left"
    )
    merged4["cbo942d"] = np.where(merged4["cbo942d"].notna(), merged4["cbo942d"], 99)
    
    # Drop occs and micros that are dropped in 3_1_eta_estimation.do
    merged4 = merged4.loc[~merged4['cbo942d'].isin([31, 22, 37])]
    merged4 = merged4.loc[~merged4['mmc'].isin([13007,15019,17001,17002,17003,17006,17007,17008,17901,23004,23014])]

    # Build final columns based on version.
    if not USE_GAMMA:
        # Original version: use mmc and level2 "cbo942d"
        dup_cols = ["fakeid_worker", "fakeid_firm", "cnae95", "ibgesubsector", "mmc", "cbo942d"]
        final_cols = ["fakeid_worker", "fakeid_firm", "cnae95", "ibgesubsector",
                      "earningsavgmw", "earningsdecmw", "mmc", "cbo942d"]
    else:
        # Gamma version: use gamma (alternative market definition)
        dup_cols = ["fakeid_worker", "fakeid_firm", "cnae95", "ibgesubsector", "gamma"]
        final_cols = ["fakeid_worker", "fakeid_firm", "cnae95", "ibgesubsector",
                      "earningsavgmw", "earningsdecmw", "gamma"]
    
    merged4 = merged4.drop_duplicates(subset=dup_cols)
    merged4["none"] = 1
    for col in final_cols:
        if col not in merged4.columns:
            merged4[col] = np.nan
    valid_df = merged4[final_cols].copy()
    valid_df.reset_index(drop=True, inplace=True)
    return valid_df

# -------------------------------------------------------------------
# 2. Emulate %inyears macro: run valid() for each year
# -------------------------------------------------------------------
def inyears(start=1985, end=2000):
    all_valid = {}
    for y in range(start, end + 1):
        df = valid(y)
        if df is not None:
            all_valid[y] = df
    return all_valid

# -------------------------------------------------------------------
# 3. Emulate %collapse(i=, level2=)
# -------------------------------------------------------------------
def collapse(valid_df, year, level2):
    """
    Collapses the valid{year} DataFrame by unit, cnae95, ibgesubsector, and market.
    For the original version, grouping is by:
       [fakeid_firm, cnae95, ibgesubsector, mmc, level2]
    For the gamma version, grouping is by:
       [fakeid_firm, cnae95, ibgesubsector, gamma]
    Computes:
      - emp: count(fakeid_worker)
      - totmearnmw: sum(earningsavgmw)
      - totdecearnmw: sum(earningsdecmw)
      - avgmearn: mean(earningsavgmw)
      - avgdecearn: mean(earningsdecmw)
    Adds the year.
    """
    if not USE_GAMMA:
        group_cols = ["fakeid_firm", "cnae95", "ibgesubsector", "mmc", level2]
    else:
        group_cols = ["fakeid_firm", "cnae95", "ibgesubsector", "gamma"]
    agg_df = valid_df.groupby(group_cols, as_index=False).agg(
        emp=("fakeid_worker", "count"),
        totmearnmw=("earningsavgmw", "sum"),
        totdecearnmw=("earningsdecmw", "sum"),
        avgmearn=("earningsavgmw", "mean"),
        avgdecearn=("earningsdecmw", "mean")
    )
    agg_df["year"] = year
    return agg_df

# -------------------------------------------------------------------
# 4. Emulate %append(level2=) and %master(occup=)
# -------------------------------------------------------------------
def master(all_valid, level2):
    """
    For each year in all_valid, collapse the data using the specified level2 option.
    Then append all years and save the final collapsed dataset.
    The output filename depends on the version selected.
    """
    collapsed_list = []
    for year, df_valid in sorted(all_valid.items()):
        collapsed = collapse(df_valid, year, level2)
        collapsed_list.append(collapsed)
    if len(collapsed_list) == 0:
        print(f"No data to append for level2={level2}.")
        return
    allyears = pd.concat(collapsed_list, ignore_index=True)
    out_file_parquet = f"{monopsas_path}/{collapsed_prefix}.parquet"
    out_file_dta = f"{monopsas_path}/{collapsed_prefix}.dta"
    allyears.to_parquet(out_file_parquet, index=False)
    allyears.to_stata(out_file_dta)
    print(f"Saved collapsed data to {out_file_dta}")

# -------------------------------------------------------------------
# 5. Putting it all together
# -------------------------------------------------------------------
def main():
    # Run the entire pipeline twice:
    # once for the original version (USE_GAMMA=False) and once for the gamma version (USE_GAMMA=True)
    global USE_GAMMA, market_var, level2_default, collapsed_prefix
    
    for use_gamma_value in [False, True]:
        USE_GAMMA = use_gamma_value
        if not USE_GAMMA:
            market_var = "mmc"
            level2_default = "cbo942d"
            collapsed_prefix = "rais_collapsed_firm_mmc_cbo942d"
        else:
            market_var = "gamma"
            level2_default = "none"
            collapsed_prefix = "rais_collapsed_firm_gamma"
            
        print(f"\n=== Processing for USE_GAMMA = {USE_GAMMA} ===\n")
        # Build valid{year} datasets for all years.
        all_valid = inyears(start=1985, end=2000)
        # For the original version, run with level2="cbo942d"; for gamma version, use level2="none".
        master(all_valid, level2=("cbo942d" if not USE_GAMMA else "none"))
    
    print("\nAll done!")

# -------------------------------------------------------------------
# Run main
# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
