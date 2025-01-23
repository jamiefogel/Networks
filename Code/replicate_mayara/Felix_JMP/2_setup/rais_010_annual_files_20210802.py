import pandas as pd
import glob
import os
import pyreadstat
from config import root, rais
import numpy as np

# Parameters
DATA_DIR = root + "/Code/replicate_mayara/raisdeidentified/dta/20191213"  # Directory where raw RAIS state-year DTA files are stored
OUTPUT_DIR = root + "/Code/replicate_mayara/monopsonies/sas"
START_YEAR = 2009
END_YEAR = 2014

# Conditions replicated from SAS code:
# Keep if:
# - emp1231 == 1 (employed on Dec 31)
# - educ in [1..11], not missing
# - agegroup in [3..7]
# - earningsdecmw > 0
# - earningsdecmw not missing

# Highest paying job selection logic:
# For each fakeid_worker in a year, keep only the row with maximum earningsdecmw.

def process_year(year):
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
    
    # This uses a groupby transform to find max earnings per worker
    max_earn = year_df.groupby("fakeid_worker")["earningsdecmw"].transform("max")
    year_df = year_df[year_df["earningsdecmw"] == max_earn]

    # Remove duplicates in case multiple jobs had same earnings
    # SAS does a "proc sort nodupkey by fakeid_worker"
    # We'll assume unique after this filtering:
    year_df = year_df.drop_duplicates(subset=["fakeid_worker"])

    return year_df


def import_years():
    for y in range(START_YEAR, END_YEAR + 1):
        print(f"Processing year {y}...")
        df = process_year(y)
        if not df.empty:
            # Save the result – in SAS code it saves to monopsas.raisYYYY
            # We'll just write to a parquet file
            out_path = os.path.join(OUTPUT_DIR, f"rais{y}.parquet")
            df.to_parquet(out_path, index=False)
            print(f"Year {y} processed and saved to {out_path}")
        else:
            print(f"No data for year {y}.")


# In the SAS code, the macro is defined and then called at the end:
# %import_years;
# In Python, we just call the function:
if __name__ == "__main__":
    import_years()


for year in range(1985,2011):
    try:
        df = pd.read_parquet(f'/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit//Code/replicate_mayara/monopsonies/sas/rais{year}.parquet')
        print(year, df.shape)
    except:
        print(year, " not found")