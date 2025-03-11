import pandas as pd
import glob
import os
import pyreadstat
from config import root, rais
import numpy as np
import pickle
import sys
from spec_parser import parse_spec

# Parameters
DATA_DIR = root + "/Code/replicate_mayara/raisdeidentified/dta/20191213"  # Directory where raw RAIS state-year DTA files are stored
OUTPUT_DIR = root + "/Code/replicate_mayara/monopsonies/sas"
START_YEAR = 1986
END_YEAR = 2000

# Conditions replicated from SAS code:
# Keep if:
# - emp1231 == 1 (employed on Dec 31)
# - educ in [1..11], not missing
# - agegroup in [3..7]
# - earningsdecmw > 0
# - earningsdecmw not missing

# Highest paying job selection logic:
# For each fakeid_worker in a year, keep only the row with maximum earningsdecmw.

def process_year(year, jblocks, wblocks, _3states):
    # List all files for that year
    # In SAS code: dir /proj/patkin/raisdeidentified/dta/20191213/*&i*.dta
    # Adjust the pattern below as needed. The SAS code seems to pick up all files with the year in the name.
    pattern = os.path.join(DATA_DIR, f"*{year}*.dta")
    file_list = glob.glob(pattern)

    # If files are stored differently, adjust pattern or directory structure accordingly.

    # Read and filter each file
    dfs = []
    for f in file_list:
        # Using pyreadstat to read .dta; if CSV or another format, adjust accordingly.
        # pip install pyreadstat
        df, meta = pyreadstat.read_dta(f)
        df = pd.read_stata(f, convert_categoricals=False)  # Avoid auto-converting categorical variables


        if not 'agegroup' in df.columns:
            df['age'] = year - df.birthdate%10000
            bins = [0, 14, 17, 24, 29, 39, 49, 64, np.inf]
            labels = range(1, 9)
            df['agegroup'] = pd.cut(df['age'], bins=bins, labels=labels, right=True).astype(float)
            
        # Apply filters
        # Check variable existence and names may differ from SAS code; adjust as necessary.
        # SAS code checks:
        # emp1231 == 1
        # 1 <= educ <= 11 and educ not missing
        # 3 <= agegroup <= 7 and agegroup not missing
        # earningsdecmw > 0 and not missing

        # We are sometimes getting slightly off compared to the counts in Mayara's SAS log file in our total counts in this step but I can't figure out why
        conditions = (
            (df["emp1231"] == 1) &
            (df["educ"].between(1, 11)) &
            (df["agegroup"].between(3, 7)) &
            (df["earningsdecmw"].notna()) & (df["earningsdecmw"] > 0)
        )

        df = df[conditions]
        
        if _3states=='_3states':
            df['state'] =  pd.to_numeric(df['municipality'].astype(str).str[:2], errors='coerce').astype('Int64')
            df = df.loc[df.state.isin([31,33,35])]

        # Drop duplicates if any (SAS: proc sort nodupkey)
        # Typically not needed if RAIS files are unique, but let's mirror SAS logic:
        df = df.drop_duplicates()

        dfs.append(df)

    if len(dfs) == 0:
        # No files for this year
        return pd.DataFrame()

    # Combine all state-level files for the year
    year_df = pd.concat(dfs, ignore_index=True)

    # Keep unique record per worker: highest earningsdecmw
    # Group by fakeid_worker, pick the row with max earningsdecmw
    # In SAS code: group by fakeid_worker and keep max(earningsdecmw)
    # If multiple rows tie for max, we’ll just pick the first occurrence.
    # If you need a deterministic tie-break, add sorting logic.
    #
    # This uses a groupby transform to find max earnings per worker
    max_earn = year_df.groupby("fakeid_worker")["earningsdecmw"].transform("max")
    year_df = year_df[year_df["earningsdecmw"] == max_earn]
    # Remove duplicates in case multiple jobs had same earnings
    # SAS does a "proc sort nodupkey by fakeid_worker"
    # We'll assume unique after this filtering:
    year_df = year_df.drop_duplicates(subset=["fakeid_worker"])
    print(year)
    if year >=1994: 
        year_df = year_df.rename(columns={'cbo94':'cbo'})
    # Merge on iotas and gammas
    year_df['occ4'] = pd.to_numeric(year_df['cbo'].astype(str).str[:4], errors='coerce')
    # XX I am doing an inner merge for gammas but not iotas since we aren't actually using iotas so no reason to drop missings 
    year_df = year_df.merge(jblocks, on=['fakeid_estab','occ4'], how='inner', validate='m:1', indicator='_merge_j' )
    print(year_df._merge_j.value_counts())
    year_df = year_df.merge(wblocks, on=['fakeid_worker'], how='left', validate='m:1', indicator='_merge_w' )
    print(year_df._merge_w.value_counts())
    year_df.drop(columns=['_merge_j','_merge_w'], inplace=True)
    return year_df


def import_years(jblocks, wblocks, _3states=""):
    for y in range(START_YEAR, END_YEAR + 1):
        print(f"Processing year {y}...")
        df = process_year(y, jblocks, wblocks, _3states)
        if not df.empty:
            # Save the result – in SAS code it saves to monopsas.raisYYYY
            # We'll just write to a parquet file
            out_path = os.path.join(OUTPUT_DIR, f"rais{y}{_3states}.parquet")
            df.to_parquet(out_path, index=False)
            print(f"Year {y} processed and saved to {out_path}")
        else:
            print(f"No data for year {y}.")



def read_blocks(jblocks_filename, wblocks_filename):
    jblocks = pd.read_csv(jblocks_filename)
    jblocks[['fakeid_estab', 'occ4']] = jblocks['jid'].str.split('_', expand=True)
    jblocks['fakeid_estab'] = jblocks['fakeid_estab'].astype(int)
    jblocks['occ4'] = jblocks['occ4'].astype(int)
    jblocks = jblocks[['fakeid_estab', 'occ4', 'jid', 'job_blocks_level_0', 'job_blocks_level_1']]
    jblocks = jblocks.rename(columns={'job_blocks_level_0':'gamma','job_blocks_level_1':'gamma1'})
    wblocks = pd.read_csv(wblocks_filename)
    wblocks = wblocks.rename(columns={'wid':'fakeid_worker'})
    wblocks = wblocks[['fakeid_worker', 'worker_blocks_level_0', 'worker_blocks_level_1']]
    wblocks = wblocks.rename(columns={'worker_blocks_level_0':'iota','worker_blocks_level_1':'iota1'})
    return jblocks, wblocks

# In the SAS code, the macro is defined and then called at the end:
# %import_years;
# In Python, we just call the function:
if __name__ == "__main__":
    chosen_spec, market_vars, file_suffix, _3states = parse_spec(root)
    
    # Load SBM that we ran on Mayara data
    if _3states=='_3states':
        spec_list =  ['', '_mcmc', '_7500', '_7500_mcmc']
    elif _3states=='':
        spec_list =  ['']
    for spec in spec_list:
        filename_stem = root + f'/Data/derived/sbm_output/model_sbm_mayara_1986_1990{_3states}{spec}_'
        jblocks_temp, wblocks_temp = read_blocks(filename_stem + 'jblocks.csv', filename_stem + 'wblocks.csv')
        jblocks_temp = jblocks_temp.rename(columns={'gamma':f'gamma{spec}','gamma1':f'gamma1{spec}'})
        wblocks_temp = wblocks_temp.rename(columns={'iota' :f'iota{spec}',  'iota1':f'iota1{spec}'})
    
        if spec==spec_list[0]:
            jblocks = jblocks_temp.copy()
            wblocks = wblocks_temp.copy()
        else:
            jblocks = jblocks.merge(jblocks_temp[['jid',f'gamma{spec}',f'gamma1{spec}']], on='jid', how='outer', validate='1:1', indicator=True)
            print(jblocks._merge.value_counts())
            jblocks.drop(columns=['_merge'], inplace=True)
            wblocks = wblocks.merge(wblocks_temp[['fakeid_worker',f'iota{spec}',f'iota1{spec}']], on='fakeid_worker', how='outer', validate='1:1', indicator=True)
            print(wblocks._merge.value_counts())
            wblocks.drop(columns=['_merge'], inplace=True)
            

    import_years(jblocks, wblocks, _3states = _3states)
