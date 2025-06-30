# -*- coding: utf-8 -*-
"""
Created on Thu Mar 13 10:25:28 2025

@author: p13861161
"""


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
        
        year_df['occ4'] = pd.to_numeric(year_df['cbo1994'].astype(str).str[:4], errors='coerce')
        year_df.loc[year_df['id_estab'].notna() & year_df['cbo1994'].notna(), 'jid'] = year_df['id_estab'].astype(str) + '_' + year_df['cbo1994'].astype(str).str[0:4]
        
        year_df = year_df[conditions]
        
        '''
        XX
        if _3states=='_3states':
            year_df['state'] =  pd.to_numeric(year_df['codemun'].astype(str).str[:2], errors='coerce').astype('Int64')
            year_df = year_df.loc[year_df.state.isin([31,33,35])]
        '''
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
        
        # Merge on iotas and gammas
        # XX I am doing an inner merge for gammas but not iotas since we aren't actually using iotas so no reason to drop missings 
        
        # XX This is where we would run the SBM and merge on iotas and gammas
        
                
    
    
        # XX Should we be drpping Manaus and other microregions here? We drop certain micro regions in 3_1 but isn't that after we compute shares? But msybe it's ok since we are computing shares within markets and this would drop entire markets. But gammas will span mmcs so it may actually be important to do the drop here once we start doing gammas or other market definitions
        
        _3states=''
        OUTPUT_DIR = root + "/Code/clean_replicate_mayara/monopsonies/sas"
        out_path = os.path.join(OUTPUT_DIR, f"rais_mayara_pull_{year}{_3states}.parquet")
        year_df.to_parquet(out_path, index=False)
        print(f"Year {year} processed and saved to {out_path}")
    
    
        print(year_df.columns)
        print(year_df.shape)
        
        

# ------------------------------------------------------------------------------
# 1) LOADING DATA (original function, unchanged)
# ------------------------------------------------------------------------------

def create_edgelist(firstyear, lastyear):
    dfs = []
    # Should this start with 86?
    for year in range(firstyear, lastyear+1):
        print(year)
        OUTPUT_DIR = root + "/Code/clean_replicate_mayara/monopsonies/sas"
        _3states=''
        df = pd.read_parquet(os.path.join(OUTPUT_DIR, f"rais_mayara_pull_{year}{_3states}.parquet"))
        df = df[['pis','jid', 'codemun']]
        df['state'] =  pd.to_numeric(df['codemun'].astype(str).str[:2], errors='coerce').astype('Int64')
        # Create jid only for rows where both fakeid_estab and occ4 are not missing
        dfs.append(df)

    stacked = pd.concat(dfs, ignore_index=True)
    stacked.rename(columns={'pis':'wid'}, inplace=True)
    edgelist = stacked.loc[stacked.jid.notna(),['wid','jid']]
    edgelist_3states = stacked.loc[(stacked.jid.notna())& (stacked.state.isin([31,33,35])),['wid','jid']]
    edgelist.to_pickle(root + f'/Data/derived/clean_mayara_edgelist_{firstyear}_{lastyear}.p')
    edgelist_3states.to_pickle(root + f'/Data/derived/clean_mayara_edgelist_{firstyear}_{lastyear}_3states.p')
    
    del edgelist, edgelist_3states, stacked, dfs, df
    gc.collect()
    
    
def run_sbm_section(modelname, edgelist_path, fit_params=None, run_mcmc=True):
    """
    Run the SBM modeling section with the specified modelname and fitting parameters.
    
    Parameters:
        modelname (str): A unique name for the model run (used for output filenames).
        fit_params (dict, optional): Additional keyword arguments for model.fit.
        run_mcmc (bool, optional): Whether to run the MCMC sweeps.
    """

    print('Starting SBM section at', datetime.now())
    
    # Create and configure the model
    model = bisbm.bisbm()
    model.create_graph(filename=edgelist_path, min_workers_per_job=1, drop_giant=True)
    
    # Default fit parameters if none provided
    if fit_params is None:
        fit_params = {}
    model.fit(n_init=1, **fit_params)
    
    # Define output paths
    output_prefix = root + '/Data/derived/sbm_output/'
    blocks_csv = output_prefix + 'model_' + modelname + '_blocks.csv'
    jblocks_csv = output_prefix + 'model_' + modelname + '_jblocks.csv'
    wblocks_csv = output_prefix + 'model_' + modelname + '_wblocks.csv'
    edgelist_parquet = output_prefix + modelname + '_edgelist_w_blocks.p'
    model_pickle = output_prefix + 'model_' + modelname + '.p'
    # Save it before export_blocks() in case something goes wrong 
    pickle.dump(model, open(model_pickle, "wb"))

    # Export blocks and edgelist with blocks
    model.export_blocks(output=blocks_csv, joutput=jblocks_csv, woutput=wblocks_csv, max_level=2)
    model.edgelist_w_blocks[['wid', 'jid', 'workers_per_job', 'jobs_per_worker', 
                             'jid_py', 'wid_py', 
                             'job_blocks_level_0', 'job_blocks_level_1', 
                             'worker_blocks_level_0', 'worker_blocks_level_1']].to_parquet(edgelist_parquet)
    
    # Save the model
    pickle.dump(model, open(model_pickle, "wb"))
    print('SBM section complete at', datetime.now())
    
    # Optionally run MCMC sweeps
    if run_mcmc:
        mcmc_file = output_prefix + 'model_' + modelname + '_mcmc.p'
        model.mcmc_sweeps(mcmc_file, tempsavedir=output_prefix, numiter=1000, seed=734)
        pickle.dump(model, open(model_pickle, "wb"))

if 1==0:
    run_pull()


# Define a list of parameter dictionaries for each SBM run
sbm_runs = [
    {
        'modelname': 'clean_sbm_mayara_1986_1990',
        'fit_params': {},  # No additional parameters for this run
        'run_mcmc': True
    },
    {
        'modelname': 'clean_sbm_mayara_1986_1990_3states',
        'fit_params': {},  # No additional parameters for this run
        'run_mcmc': True
    }
]

run_create_edgelist = False
if run_create_edgelist==True:
    create_edgelist(1986, 1990)

run = sbm_runs[1]
#def run_sbm_section(modelname, edgelist_path, fit_params=None, run_mcmc=True):

run_sbm_section(
        modelname=run['modelname'],
        edgelist_path = root + '/Data/derived/clean_mayara_edgelist_1986_1990_3states.p',
        fit_params=run.get('fit_params', None),
        run_mcmc=run.get('run_mcmc', False)
    )

# Loop over each run configuration and execute the SBM section
'''
for run in sbm_runs :
    run_sbm_section(
        modelname=run['modelname'],
        edgelist_path = XX This needs to be updated/automated
        fit_params=run.get('fit_params', None),
        run_mcmc=run.get('run_mcmc', True)
    )
'''
    
    
  
