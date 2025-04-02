import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import os
from datetime import datetime
from config import root, rais
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
import bisbm
import pickle


crosswalk_path =  root + "/Code/replicate_mayara/monopsonies/sas"
OUTPUT_DIR = root + "/Code/clean_replicate_mayara/monopsonies/sas"


# -------------------------------
# These files provide the necessary mappings:
# - crosswalk: maps municipality to mmc
# - firm_master: provides firm-level cnae95 information
# - df_valid_cbo94: provides valid occupation codes (for years < and >= 1994)
mmc_codemun_crosswalk   = pd.read_parquet(os.path.join(crosswalk_path, "crosswalk_muni_to_mmc_DK17.parquet"))  
firm_cnae_crosswalk     = pd.read_parquet(os.path.join(crosswalk_path, f"rais_firm_cnae95_master_plus.parquet")).drop_duplicates()
df_valid_cbo94          = pd.read_parquet(os.path.join(crosswalk_path, "valid_cbo94.parquet")).drop_duplicates()
df_trade                = pd.read_parquet(os.path.join(crosswalk_path, "cnae95_tariff_changes_1990_1994.parquet"))
df_cross_cnae95         = pd.read_parquet(os.path.join(crosswalk_path, "crosswalk_cnae95_ibgesubsector.parquet"))      


tariffs_maindataset_long = pd.read_pickle(f"{crosswalk_path}/tariffs_maindataset_long.pkl")
tariffs_maindataset_long = tariffs_maindataset_long.loc[tariffs_maindataset_long.year.isin([1990,1994]), ['year','cnae95','ibgesubsector','TRAINS']] # ,'ErpTRAINS'
tariffs_maindataset_wide = tariffs_maindataset_long.pivot(
    index=['cnae95','ibgesubsector'],
    columns='year',
    values='TRAINS'
    ).reset_index()
    # XX Assuming I don't need ErpTRAINS but maybe wrong
tariffs_maindataset_wide['chng_lnTRAINS'] = np.log(1+tariffs_maindataset_wide[1994]/100) -  np.log(1+tariffs_maindataset_wide[1990]/100) 
    
# XX These look basically identical except that the latter has 4 more rows. WTF???
df_trade[['cnae95','tradable','chng19941990TRAINS']]    
tariffs_maindataset_wide.chng_lnTRAINS


test = tariffs_maindataset_wide.merge(df_trade[['cnae95','tradable','chng19941990TRAINS']], on='cnae95', how='left')
(test.chng19941990TRAINS==test.chng_lnTRAINS).sum()
test.loc[(test.chng19941990TRAINS!=test.chng_lnTRAINS)]

'''
Workers - use PIS as ID
Firms - Use CNPJ or CEI as ID
'''

valid_columns = ['pis','cnpj_raiz','id_estab','genero','grau_instr','subs_ibge','subativ_ibge','codemun','mes_adm','cbo1994','rem_dez_sm', 'rem_med_sm','emp_31dez','temp_empr']


# rais_010_annual_files
def run_pull():
    for year in range(1986,2001):
        print(year)
        file_path = os.path.join(rais, f"parquet_novos/brasil{year}.parquet")
        using_uf = True
        codemun_crosswalk = {}
        if year < 1994:
            agevar = ['fx_etaria']
        if year>=1994:
            agevar = ['idade']
        if year < 1995:
            cnae = []
        if year >= 1995:
            cnae = ['clas_cnae']
        year_df = pq.read_table(file_path, columns=valid_columns + agevar + cnae).to_pandas()
        year_df['year'] = year
        if year >= 1994:
            bins = [0, 14, 17, 24, 29, 39, 49, 64, np.inf]
            #labels = ['0-14', '15-17', '18-24', '25-29', '30-39', '40-49', '50-64', '65+']
            labels = range(1, 9)
            year_df['fx_etaria'] = pd.cut(year_df['idade'], bins=bins, labels=labels, right=True).astype(float)
        
        year_df.rename(columns={'fx_etaria':'agegroup',
                           'grau_instr':'educ',   
                           'rem_dez_sm':'earningsdecmw',
                           'rem_med_sm':'earningsavgmw',
                           'subs_ibge':'ibgesubsector',
                           'genero':'gender',
                           'temp_empr':'empmonths',
                           'mes_adm':'admmonth'
                           }, inplace=True)
        
        # Some rows have missing cnpj_raiz but non-missing id_estab. Fill in cnpj_raiz for these
        mask = year_df['id_estab'].notna()
        year_df.loc[mask, 'temp'] = year_df.loc[mask, 'id_estab'].astype(int).astype(str).str.zfill(14).str[0:8].astype(int)
        year_df.loc[year_df.cnpj_raiz.isna(), 'cnpj_raiz'] = year_df.loc[year_df.cnpj_raiz.isna(),'temp']
        
        year_df['female'] = (year_df['gender'] == 2).astype(int)

        # We are sometimes getting slightly off compared to the counts in Mayara's SAS log file in our total counts in this step but I can't figure out why
        # PRINTING DESCRIPTIVE STATS BEFORE DROPPING OBSERVATIONS
        print('YEAR = ' + str(year))
        print(year_df["educ"].value_counts())
        print(year_df["agegroup"].value_counts())
        print((year_df["emp_31dez"] == 1).mean())
        
        conditions = (
            (year_df["emp_31dez"] == 1) &
            (year_df["educ"].between(1, 11)) &    
            (year_df["agegroup"].between(3, 7)) &
            (year_df["earningsdecmw"].notna()) & (year_df["earningsdecmw"] > 0) &
            (year_df["codemun"].notna()) &
            (year_df["ibgesubsector"] != 24)  
            # subs_ibge: 24 Adm publica direta e autarquica
            # subs_ibge: 14 Servicos industriais de utilidade publica (??). Maybe this is still industry responding to government demand
        )
        year_df = year_df[conditions]
        
        year_df['occ4'] = pd.to_numeric(year_df['cbo1994'].astype(str).str[:4], errors='coerce')
        year_df.loc[year_df['id_estab'].notna() & year_df['cbo1994'].notna(), 'jid'] = year_df['id_estab'].astype(str) + '_' + year_df['cbo1994'].astype(str).str[0:4]
        
        # Drop a small number of duplicate job contracts
        year_df = year_df.drop_duplicates() 
        
        # Keep unique record per worker: highest earningsdecmw
        # Group by fakeid_worker, pick the row with max earningsdecmw
        # In SAS code: group by fakeid_worker and keep max(earningsdecmw)
        # If multiple rows tie for max, we’ll just pick the first occurrence.
        # If you need a deterministic tie-break, add sorting logic.
        #
        # This uses a groupby transform to find max earnings per worker
        max_earn = year_df.groupby("pis")["earningsdecmw"].transform("max")
        year_df = year_df[year_df["earningsdecmw"] == max_earn]
        # Remove duplicates in case multiple jobs had earnings equal to max earnings. Mayara drops arbitrarily in this case
        # SAS does a "proc sort nodupkey by fakeid_worker"
        # We'll assume unique after this filtering:
        year_df = year_df.drop_duplicates(subset=["pis"])
        
        # XX This is where we would run the SBM
    
        # XX Should we be drpping Manaus and other microregions here? We drop certain micro regions in 3_1 but isn't that after we compute shares? But msybe it's ok since we are computing shares within markets and this would drop entire markets. But gammas will span mmcs so it may actually be important to do the drop here once we start doing gammas or other market definitions
        
        # There are some cases where firms don't have unique ibgeseubsector within a year. In these cases take the mode (taken from 030)
        counts = year_df.groupby(['cnpj_raiz','ibgesubsector']).size().reset_index(name='count')
        idx = counts.groupby('cnpj_raiz')['count'].idxmax()
        modes = counts.loc[idx, ['cnpj_raiz','ibgesubsector']]
        year_df = year_df.drop(columns='ibgesubsector')
        year_df = year_df.merge(modes, on='cnpj_raiz', how='left')
        
        # Merge on mmc and cbo942d
        year_df = pd.merge(year_df, mmc_codemun_crosswalk, left_on='codemun', right_on='codemun', how='inner')  
        year_df = pd.merge(year_df, df_valid_cbo94,       left_on='cbo1994', right_on='cbo94', how='left', suffixes=('', '_vd'))
        
        out_path = os.path.join(OUTPUT_DIR, f"rais_mayara_pull_{year}{_3states}.parquet")
        year_df.to_parquet(out_path, index=False)
        print(f"Year {year} processed and saved to {out_path}")
    
        print(year_df.columns)
        print(year_df.shape)
        
run_pull()        
        
dfs = []
for year in [1991,1997]:
    year_df = pd.read_parquet(os.path.join(OUTPUT_DIR, f"rais_mayara_pull_{year}{_3states}.parquet"))
    dfs.append(year_df)

worker = pd.concat(dfs, ignore_index=True)
 

# XX This is produced by _020 so we need to run that first (or extract relevant code and just run it here)
worker = pd.merge(worker, firm_cnae_crosswalk,  on='cnpj_raiz', how='left', indicator=False) 


# Merge on change in tariffs. Could also use df_trade cuz it has basically the same info
worker = worker.merge(tariffs_maindataset_wide[['cnae95', 'chng_lnTRAINS']], on='cnae95', how='left')


######
# Drop some MMCs and CBOs
 
mmcs_to_drop = pd.read_stata(root + 'Code/replicate_mayara/publicdata/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta')
worker = worker.merge(mmcs_to_drop, on='mmc', how='left', validate='m:1')
worker = worker.loc[(worker.mmc_drop!=1) & (worker.mmc!=13007)]
worker = worker.drop(columns='mmc_drop')

worker = worker.loc[~worker.cbo942d.isin([22,31,37])]

worker.to_stata(monopsas_path + f"/worker_level.dta")


# XX This is where we would merge on iotas and gammas


########################
# Create firm-market-level data set for eta regressions

- Need firm-market eanrings premia that we can then merge back on (then take log differences)
- Also need firm-market employment that we can take log differences on
- Those two plus chng_lnTRAINS should be enough to run the regression



########################
# Create firm-market-level collapse (040 and 050)
#   - This will need to be made flexible enough to accomodate different market definitions

# XX This is the remaining code from 030_040
XX THIS IS ACTUALLY DONE ONE YEAR AT A TIME. BUT 050 ONLY USES BASE YEAR (1991)
firm_mkt_year_collapsed = workers.groupby(
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









Run pull_raw

Keep only 1991 and 1997 and append them. This is the start of our "master" data set
Merge on earliest cnae for each cnpj_raiz from "rais_firm_cnae95_master_plus.parquet". This is unique on cnpj_raiz