import pandas as pd
import numpy as np
import os
from config import root

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
    Python equivalent of the %valid(i=...) macro.
    1) For the given year, find each firm's largest establishment
       by worker count (within valid filters).
    2) Join with crosswalks to produce a 'valid{year}' dataset.
    """

    print(f"\n--- Running valid() for year={year} ---")

    # -----------------------------------------------------------
    # (A) Decide occupation variable depending on year
    # -----------------------------------------------------------
    if (year < 1994) or (year == 2002):
        cboraw = "cbo"
        cboclean = "cbo94"
        validdata = "valid_cbo94"   
    elif year < 2010: 
        cboraw = "cbo" # XX I renamed cbo94 to cbo in rais_010_annual_files
        cboclean = "cbo94"
        validdata = "valid_cbo94"
    else:
        cboraw = "cbo02"
        cboclean = "cbo02"
        validdata = "crosswalk_cbo02_cbo94"

    # -----------------------------------------------------------
    # (B) Read monopsas.rais{year} for relevant columns
    # -----------------------------------------------------------
    # We need: fakeid_firm, fakeid_worker, ibgesubsector, educ, municipality,
    #          earningsdecmw, agegroup
    rais_file = os.path.join(monopsas_path, f"rais{year}.parquet")
    if not os.path.exists(rais_file):
        print(f"  -> rais{year}.parquet not found; skipping.")
        return None  # Return None to indicate no data

    df_rais = pd.read_parquet(
        rais_file,
        columns=[
            "fakeid_firm", "fakeid_worker",
            "ibgesubsector", "educ", "municipality",
            "earningsdecmw", "agegroup", 
            "jid", "gamma",
            # We will need these to build the final table:
            "earningsavgmw",  # total monthly earnings
            cboraw
        ]
    ).drop_duplicates()

    # -----------------------------------------------------------
    # (C) Filter (like SAS WHERE)
    #    educ in [1..11], municipality not null,
    #    earningsdecmw>0, ibgesubsector !=24 & not null,
    #    agegroup in [3..7]
    # -----------------------------------------------------------
    mask = (
        df_rais["educ"].notna() & df_rais["educ"].between(1, 11, inclusive="both") &
        df_rais["municipality"].notna() &
        df_rais["earningsdecmw"].notna() & (df_rais["earningsdecmw"] > 0) &
        df_rais["ibgesubsector"].notna() & (df_rais["ibgesubsector"] != 24) &
        df_rais["agegroup"].between(3, 7, inclusive="both")
    )
    temp = df_rais[mask].copy()

    # -----------------------------------------------------------
    # (D) Group by firm+ibgesubsector -> count workers
    # -----------------------------------------------------------
    
    #XX I could to this by jid instead of firm, but that's not obviously correct/better so I'll stick with this for now
    group = temp.groupby(["fakeid_firm", "ibgesubsector"], as_index=False).agg(
        emp=("fakeid_worker", "count")
    )
    # get max(emp) per firm
    group["maxemp"] = group.groupby("fakeid_firm")["emp"].transform("max")

    # keep only rows where emp == maxemp
    ibge = group[group["emp"] == group["maxemp"]].drop_duplicates("fakeid_firm")

    del temp, group  # analogous to dropping SAS temp files

    if ibge.empty:
        print("  -> No valid ibge data for this year after filtering.")
        return None

    # -----------------------------------------------------------
    # (E) Now build final "valid{year}" table, which merges:
    #     - rais{year} (again) with crosswalk data:
    #          crosswalk_muni_to_mmc_DK17
    #          rais_firm_cnae95_master_plus
    #          ibge
    #          &validdata
    # -----------------------------------------------------------
    # We'll re-read RAIS if needed, or reuse df_rais
    # We need additional merges with crosswalks:

    # crosswalk: crosswalk_muni_to_mmc_DK17
    cross_muni_file = os.path.join(monopsas_path, "crosswalk_muni_to_mmc_DK17.parquet")
    if not os.path.exists(cross_muni_file):
        print("  -> crosswalk_muni_to_mmc_DK17 missing.")
        return None
    df_cross_muni = pd.read_parquet(cross_muni_file).drop_duplicates()
    # Must contain columns ["codemun", "mmc", "cbo942d"]? 
    # The SAS code merges 'monopsas.&validdata as c' differently, so let's see:

    # cnae95 master
    cnae95_master_file = os.path.join(monopsas_path, "rais_firm_cnae95_master_plus.parquet")
    if not os.path.exists(cnae95_master_file):
        print("  -> rais_firm_cnae95_master_plus missing.")
        return None
    df_cnae95 = pd.read_parquet(cnae95_master_file).drop_duplicates()

    # validdata (like valid_cbo94 or crosswalk_cbo02_cbo94)
    valid_data_file = os.path.join(monopsas_path, f"{validdata}.parquet")
    if not os.path.exists(valid_data_file):
        print(f"  -> {validdata}.parquet missing.")
        return None
    df_valid_cbo = pd.read_parquet(valid_data_file).drop_duplicates()

    # We need an extended set of columns from RAIS for the final merge:
    #   (same as the final SELECT in SAS)
    #   plus cboraw so we can join
    df_rais_full = pd.read_parquet(
        rais_file,
        columns=[
            "fakeid_worker", "fakeid_firm", "ibgesubsector", "educ",
            "municipality", "earningsdecmw", "earningsavgmw",
            "agegroup", cboraw, "jid", "gamma"
        ]
    ).drop_duplicates()

    # Filter again with the same conditions
    mask_full = (
        df_rais_full["educ"].notna() & df_rais_full["educ"].between(1, 11, inclusive="both") &
        df_rais_full["municipality"].notna() &
        df_rais_full["earningsdecmw"].notna() & (df_rais_full["earningsdecmw"] > 0) &
        df_rais_full["ibgesubsector"].notna() & (df_rais_full["ibgesubsector"] != 24) &
        df_rais_full["agegroup"].between(3, 7, inclusive="both")
    )
    df_rais_filtered = df_rais_full[mask_full].copy()

    # Merges (like the SAS "inner join" statements)
    # 1) inner join with crosswalk_muni_to_mmc_DK17 on municipality=codemun
    merged1 = df_rais_filtered.merge(
        df_cross_muni[["codemun", "mmc"]],  # we assume that is the relevant data
        left_on="municipality",
        right_on="codemun",
        how="inner"
    )
    merged1.drop(columns=["codemun"], inplace=True)

    # 2) inner join with rais_firm_cnae95_master_plus on fakeid_firm
    merged2 = merged1.merge(
        df_cnae95[["fakeid_firm", "cnae95"]],
        on="fakeid_firm",
        how="inner"
    )

    # 3) inner join with ibge{year} on (fakeid_firm, ibgesubsector)
    merged3 = merged2.merge(
        ibge[["fakeid_firm", "ibgesubsector"]],
        on=["fakeid_firm", "ibgesubsector"],
        how="inner"
    )

    # 4) left join with validdata on (a.&cboraw = c.&cboclean)
    #    Then case when c.cbo942d is not null else 99
    merged4 = merged3.merge(
        df_valid_cbo[[cboclean, "cbo942d"]],
        left_on=cboraw,
        right_on=cboclean,
        how="left"
    )
    merged4["cbo942d"] = np.where(merged4["cbo942d"].notna(), merged4["cbo942d"], 99)

    # Build final columns: 
    # SELECT distinct 
    #   a.fakeid_worker, a.fakeid_firm, e.cnae95, f.ibgesubsector,
    #   c.mmc, 1 as none, case(...) as cbo942d, 
    #   a.earningsavgmw, a.earningsdecmw
    merged4 = merged4.drop_duplicates(
        subset=["fakeid_worker", "fakeid_firm", "cnae95", "ibgesubsector", "gamma"]
    )

    merged4["none"] = 1  # as in SAS

    final_cols = [
        "fakeid_worker", "fakeid_firm", "cnae95", "ibgesubsector",
        "gamma", "earningsavgmw", "earningsdecmw"
    ]
    # If any are missing, fill with NaN
    for col in final_cols:
        if col not in merged4.columns:
            merged4[col] = np.nan

    valid_df = merged4[final_cols].copy()
    valid_df.reset_index(drop=True, inplace=True)
    return valid_df


# -------------------------------------------------------------------
# 2. Emulate %inyears macro
#    We run valid(y) for y in [1985..2000]
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
#    Collapses "valid{i}" data by firm & mmc & {level2},
#    computing sums and means.
# -------------------------------------------------------------------
def collapse(valid_df, year, level2):
    """
    valid_df is the DataFrame from valid{year}.
    Returns a collapsed DataFrame akin to 'firm_{level2}{year}'.
    """
    # We group by: (fakeid_firm, cnae95, ibgesubsector, mmc, {level2}), year=year
    # Then:
    #   emp = count(fakeid_worker)
    #   totmearnmw = sum(earningsavgmw)
    #   totdecearnmw = sum(earningsdecmw)
    #   avgmearn = mean(earningsavgmw)
    #   avgdecearn = mean(earningsdecmw)

    #group_cols = ["fakeid_firm", "cnae95", "ibgesubsector", "mmc", level2]
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
#    We do it in Python by building all years, then combining them
#    into one DataFrame, and finally saving.
# -------------------------------------------------------------------
def master(all_valid, level2):
    """
    For each year in all_valid, call collapse(...).
    Then append all results, save them to a file named:
      'rais_collapsed_firm_mmc_{level2}.parquet'
    """
    collapsed_list = []
    for year, df_valid in sorted(all_valid.items()):
        collapsed = collapse(df_valid, year, level2)
        collapsed_list.append(collapsed)
    if len(collapsed_list) == 0:
        print(f"No data to append for {level2}.")
        return

    allyears = pd.concat(collapsed_list, ignore_index=True)

    # Save as parquet (SAS code exports .dta, you can adjust to .dta if needed)
    allyears.to_parquet(f"{monopsas_path}/rais_collapsed_firm_gamma.parquet", index=False)
    allyears.to_stata(  f"{monopsas_path}/rais_collapsed_firm_gamma.dta")
    print(f"Saved collapsed data for level2={level2} to {monopsas_path}/rais_collapsed_firm_gamma.dta")


# -------------------------------------------------------------------
# 5. Putting it all together
# -------------------------------------------------------------------
def main():
    # Emulate the final "Execute macros" block

    # 1) Run %inyears -> builds valid1985..valid2000
    all_valid = inyears(start=1985, end=2000)

    # XX Mayara does this in two ways: mmc only and mmc X cbo942d. The former corresponds to level2="none" and the latter to level2="cbo942d". Then it saves separate output files for each. What I should really by trying to do is create a 3rd option that does jid insteaf firmid_fake and gamma instead of her markets. Then I can process all 3 versions at once
    # 2) %master(occup=none)
    master(all_valid, level2="none")

    # 3) %master(occup=cbo942d)
    #master(all_valid, level2="cbo942d")

    print("\nAll done!")

# -------------------------------------------------------------------
# Run main
# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
