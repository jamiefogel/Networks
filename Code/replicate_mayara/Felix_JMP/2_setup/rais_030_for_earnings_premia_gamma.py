# -*- coding: utf-8 -*-
"""
XX When we add additional market variables we need to add them to the list addl_market_vars in the loop at the bottom


This script, rais_030_for_earnings_premia.py, processes the raw RAIS data for a given year to create
a cleaned, worker-level dataset that contains earnings premia information. In detail, the script:
  - Reads in the RAIS data file for the specified year and selects only the necessary columns,
    including worker and firm identifiers, sector codes, education, municipality, earnings, age group,
    and the new market variables (job_blocks_level_0) and job ID (jid) if available.
  - Applies filters to retain observations that meet key criteria (e.g., education between 1 and 11,
    non-missing municipality, earnings greater than zero, a valid ibgesubsector, and an age group between 3 and 7).
  - Groups the data by firm (originally using fakeid_firm, but conceptually it identifies the primary
    sector by selecting the group with the maximum worker count) to determine each firm's dominant ibgesubsector.
      - XX IS THIS SOMETHING THAT WE WOULD EDIT WHEN USING GAMMAS? I DON'T THINK SO BUT WANT TO FLAG
  - Merges the filtered data with additional crosswalk and firm-level datasets (such as municipality-to-mmc
    and firm CNAE information) as well as a validated occupation crosswalk to construct the final set of
    variables (e.g., fakeid_worker, fakeid_firm, cnae95, ibgesubsector, mmc, cbo942d, and various earnings measures).
  - Saves the final output as both a Parquet file and a Stata (.dta) file, which are then used by later scripts
    (for example, in rais_040_firm_collapsed.py) to aggregate the data to the firm level and to compute market-level
    measures like the payroll Herfindahl index that are crucial for estimating the model parameters (eta and theta)
    in the replication of "Trade, Labor Market Concentration, and Wages".

In the overall analysis pipeline, this script is a key intermediate step that transforms raw RAIS data into
a format suitable for subsequent aggregation, market-level computation, and eventual econometric estimation.
"""

import pandas as pd
import numpy as np
import os
from config import root
import sys
from spec_parser import parse_spec


# Paths
base_path = root + "/Code/replicate_mayara"
monopsas_path = f"{base_path}/monopsonies/sas"


def get_demos(year, _3states, addl_market_vars = ["gamma","gamma1","gamma_mcmc","gamma1_mcmc"]):
    """
    Python equivalent of SAS macro %getdemos(i=...).
    Reads monopsas.rais{year}, merges with crosswalks, filters by certain conditions,
    and writes out a final file for that year.
    """
    print(f"Processing year = {year}...")

    # Determine cboraw vs cboclean
    '''
    if year < 1994:
        cboraw = "cbo"
        cboclean = "cbo94"
    else:
        cboraw = "cbo94"
        cboclean = "cbo94"
    '''
    #XX I edited rais_010_annual_files_20210802_w_sbm.py to rename cbo94 to cbo in 1994 and later
    cboraw = "cbo"
    cboclean = "cbo94"


    rais_file = os.path.join(monopsas_path, f"rais{year}{_3states}.parquet")
    if not os.path.exists(rais_file):
        print(f"  -> RAIS data for {year} not found. Skipping.")
        return  # no data => skip

    df_rais = pd.read_parquet(
        rais_file,
        columns=["fakeid_worker", "fakeid_firm", "ibgesubsector",
                 "educ", "municipality", "earningsdecmw", "agegroup"]
    ).drop_duplicates()

    # Filter
    mask = (
        df_rais["educ"].between(1, 11, inclusive="both") &
        df_rais["municipality"].notna() &
        df_rais["earningsdecmw"].notna() & (df_rais["earningsdecmw"] > 0) &
        df_rais["ibgesubsector"].notna() & (df_rais["ibgesubsector"] != 24) &
        df_rais["agegroup"].between(3, 7, inclusive="both")
    )
    temp = df_rais[mask].copy()

    # Count how many workers per (firm, ibgesubsector)
    temp_grouped = temp.groupby(["fakeid_firm", "ibgesubsector"], dropna=False, as_index=False)\
                       .agg(emp=("fakeid_worker", "count"))
    # We'll need the maximum emp per firm
    temp_grouped["maxemp"] = temp_grouped.groupby("fakeid_firm")["emp"].transform("max")

    # Keep only rows where emp == maxemp
    ibge = temp_grouped[temp_grouped["emp"] == temp_grouped["maxemp"]]
    ibge = ibge.drop_duplicates(subset=["fakeid_firm"])  # replicate "nodupkey by fakeid_firm"

    del temp, temp_grouped  # replicate "proc datasets library=work nolist nowarn; delete temp&i:; run;"

    # --------------------------------------------------
    # 2) Create workers{year} by joining:
    #    - monopsas.rais{year} (a)
    #    - monopsas.crosswalk_muni_to_mmc_DK17 (c)
    #    - monopsas.rais_firm_cnae95_master_plus (e)
    #    - ibge{year} (f)
    #    - monopsas.valid_cbo94 as k (left join)
    #
    #    Only for those with certain filters
    # --------------------------------------------------
    
    # Re-read RAIS for more columns (we need empmonths, earningsavgmw, etc.)
    # or we can just re-use df_rais if it has them. Let's assume we need to read them:
    needed_cols = [
        "fakeid_worker", "fakeid_firm", "municipality",
        "earningsdecmw", "ibgesubsector", "jid",
        "admmonth", "educ", "agegroup", "empmonths", "earningsavgmw",
        "gender", cboraw  # either "cbo" or "cbo94"
    ] + addl_market_vars
    df_rais_full = pd.read_parquet(rais_file, columns=needed_cols).drop_duplicates()

    # read crosswalk for municipality -> mmc
    cross_mmc_file = os.path.join(monopsas_path, "crosswalk_muni_to_mmc_DK17.parquet")
    if not os.path.exists(cross_mmc_file):
        print(f"  -> crosswalk_muni_to_mmc_DK17 missing. Cannot proceed.")
        return

    df_muni_mmc = pd.read_parquet(cross_mmc_file).drop_duplicates()
    # columns = ["codemun", "mmc"] or similar
    # rename to standard if needed:
    # must match 'on a.municipality = c.codemun' from SAS

    # read rais_firm_cnae95_master_plus
    cnae95_master_file = os.path.join(monopsas_path, f"rais_firm_cnae95_master_plus{_3states}.parquet")
    if not os.path.exists(cnae95_master_file):
        print(f"  -> rais_firm_cnae95_master_plus{_3states} missing. Cannot proceed.")
        return
    df_cnae95 = pd.read_parquet(cnae95_master_file).drop_duplicates()

    # read valid_cbo94
    valid_cbo94_file = os.path.join(monopsas_path, "valid_cbo94.parquet")
    if not os.path.exists(valid_cbo94_file):
        print("  -> valid_cbo94 missing. Cannot proceed.")
        return
    df_valid_cbo94 = pd.read_parquet(valid_cbo94_file).drop_duplicates()

    # Merge steps replicate the SAS logic:
    #   from monopsas.rais{year} as a
    #   inner join crosswalk_muni_to_mmc_DK17 as c
    #   inner join rais_firm_cnae95_master_plus as e
    #   inner join ibge{year} as f
    #   left join valid_cbo94 as k
    #
    #   with filters on municipality, earningsdecmw, ...
    #
    #   cnae95 from e
    #   ibgesubsector from f
    #   mmc from c
    #   case when k.cbo942d is not null then k.cbo942d else 99 end as cbo942d

    # We'll do each step in Python:

    # Step A: filter df_rais_full by the "where" conditions
    #  (same conditions as in SAS, plus the 'admmonth != 12')
    mask_full = (
        df_rais_full["municipality"].notna() &
        df_rais_full["earningsdecmw"].notna() & (df_rais_full["earningsdecmw"] > 0) &
        df_rais_full["ibgesubsector"].notna() & (df_rais_full["ibgesubsector"] != 24) &
        df_rais_full["educ"].between(1, 11, inclusive="both") &
        df_rais_full["agegroup"].between(3, 7, inclusive="both")
    )
    df_rais_filtered = df_rais_full[mask_full].copy()

    # Step B: inner join with ibge (f)
    #   Condition: a.fakeid_firm = f.fakeid_firm
    #   Keep only rows that appear in ibge
    df_rais_ibge = df_rais_filtered.merge(
        ibge[["fakeid_firm", "ibgesubsector"]],
        on=["fakeid_firm", "ibgesubsector"],
        how="inner"
    )

    # Step C: inner join with cnae95 master (e)
    #   Condition: a.fakeid_firm = e.fakeid_firm
    df_rais_cnae = df_rais_ibge.merge(
        df_cnae95[["fakeid_firm", "cnae95"]],
        on="fakeid_firm",
        how="inner"
    )

    # Step D: inner join with crosswalk for municipality->mmc (c)
    #   Condition: a.municipality = c.codemun
    #   Then rename columns as needed
    df_rais_cnae_mmc = df_rais_cnae.merge(
        df_muni_mmc[["codemun", "mmc"]],
        left_on="municipality",
        right_on="codemun",
        how="inner"
    )
    df_rais_cnae_mmc.drop(columns=["codemun"], inplace=True)

    # Step E: left join with valid_cbo94 (k)
    #   Condition: a.&cboraw = k.&cboclean
    #   Then create "cbo942d = if not null then k.cbo942d else 99"
    df_merged = df_rais_cnae_mmc.merge(
        df_valid_cbo94[[cboclean, "cbo942d"]],
        left_on=cboraw,
        right_on=cboclean,
        how="left"
    )

    # Build final column "cbo942d"
    df_merged["cbo942d"] = np.where(
        df_merged["cbo942d"].notna(),
        df_merged["cbo942d"],
        99
    )

    # Step F: create final DataFrame "workers{year}" with the desired columns
    # from the SAS SELECT list
    #   a.fakeid_worker,
    #   a.fakeid_firm,
    #   e.cnae95,
    #   f.ibgesubsector,
    #   a.empmonths,
    #   a.earningsavgmw,
    #   a.earningsdecmw,
    #   a.agegroup,
    #   (a.gender = 2) as female,
    #   a.educ,
    #   c.mmc,
    #   ...
    # We'll pick them from df_merged:

    workers_cols = [
        "fakeid_worker", "fakeid_firm",
        "cnae95",          # from df_cnae95
        "ibgesubsector",   # from ibge
        "empmonths", "earningsavgmw", "earningsdecmw", "agegroup",
        "gender",          # we'll convert to female = (gender == 2)
        "educ",
        "mmc",             # from crosswalk
        "cbo942d",
        "jid", 
    ] + addl_market_vars

    # If any columns don't exist in df_merged, fill with placeholders
    for col in workers_cols:
        if col not in df_merged.columns:
            df_merged[col] = np.nan

    # Create the "female" column
    df_merged["female"] = np.where(df_merged["gender"] == 2, 1, 0)

    # Subset final
    workers_df = df_merged[workers_cols + ["female"]].copy()

    # We'll reorder columns to match the SAS code order
    final_cols_order = [
        "fakeid_worker", "fakeid_firm", "cnae95", "ibgesubsector",
        "empmonths", "earningsavgmw", "earningsdecmw", "agegroup",
        "female", "educ", "mmc", "cbo942d", "jid"
    ] + addl_market_vars
    workers_df = workers_df[final_cols_order]
    
    # Drop occs and micros that are dropped in 3_1_eta_estimation.do
    workers_df = workers_df.loc[~workers_df['cbo942d'].isin([31, 22, 37])]
    workers_df = workers_df.loc[~workers_df['mmc'].isin([13007,15019,17001,17002,17003,17006,17007,17008,17901,23004,23014])]


    # Save to an output file (like "rais_for_earnings_premia{year}.dta" in SAS)
    workers_df.to_parquet(monopsas_path + f"/rais_for_earnings_premia{year}_gamma{_3states}.parquet", index=False)
    workers_df.to_stata(monopsas_path + f"/rais_for_earnings_premia{year}_gamma{_3states}.dta")
    print(f"  -> Output saved to {monopsas_path}/rais_for_earnings_premia{year}_gamma{_3states}.parquet")

    # Equivalent to SAS: proc datasets library=work kill nolist; quit;
    # In Python, we just discard local variables so that no large DataFrames remain in memory.

# ------------------------------------------------
# Execute function for each year
# ------------------------------------------------
for y in range(1985, 2001):  # 1985..2000

    chosen_spec, market_vars, file_suffix, _3states = parse_spec(root)
    if _3states=='_3states':
        addl_market_vars =["gamma","gamma1","gamma_mcmc","gamma1_mcmc","gamma_7500","gamma1_7500","gamma_7500_mcmc","gamma1_7500_mcmc"]
    elif _3states=='':
        addl_market_vars =["gamma","gamma1"]
    missing_vars = [var for var in market_vars if var not in addl_market_vars and var not in ['mmc','cbo942d']]
    if missing_vars:
        raise ValueError(f"The following market variables are missing from addl_market_vars: {', '.join(missing_vars)}")
        
    get_demos(y, _3states, addl_market_vars)
    

