# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 18:28:33 2025

@author: p13861161
"""

import pandas as pd
import numpy as np
from config import root
import time
import psutil
import graph_tool.all as gt
import gc
import matplotlib.pyplot as plt
import pickle
import bisbm
from datetime import datetime


# ------------------------------------------------------------------------------
# 1) LOADING DATA (original function, unchanged)
# ------------------------------------------------------------------------------

def create_edgelist(firstyear, lastyear):
    dfs = []
    # Should this start with 86?
    for year in range(firstyear, lastyear+1):
        print(year)
        df = pd.read_parquet(f'/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies/sas/rais{year}.parquet')
        df = df[['fakeid_worker', 'fakeid_firm', 'fakeid_estab', 'agegroup', 'cbo', 'municipality']]
        df['state'] =  pd.to_numeric(df['municipality'].astype(str).str[:2], errors='coerce').astype('Int64')
        # Create jid only for rows where both fakeid_estab and occ4 are not missing
        df['fakeid_estab'] = df['fakeid_estab'].astype('Int64')
        df.loc[df['fakeid_estab'].notna() & df['cbo'].notna(), 'jid'] = df['fakeid_estab'].astype(str) + '_' + df['cbo'].astype(str).str[0:4]
        df.loc[df['fakeid_estab'].isna() | df['cbo'].isna(), 'jid'] = pd.NA
        dfs.append(df)

    stacked = pd.concat(dfs, ignore_index=True)
    stacked.rename(columns={'fakeid_worker':'wid'}, inplace=True)
    edgelist = stacked.loc[stacked.jid.notna(),['wid','jid']]
    edgelist_3states = stacked.loc[(stacked.jid.notna())& (stacked.state.isin([31,33,35])),['wid','jid']]
    edgelist.to_pickle(root + f'/Data/derived/mayara_edgelist_{firstyear}_{lastyear}.p')
    edgelist_3states.to_pickle(root + f'/Data/derived/mayara_edgelist_{firstyear}_{lastyear}_3states.p')
    
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

    # Define output paths
    output_prefix = root + '/Data/derived/sbm_output/'
    blocks_csv = output_prefix + 'model_' + modelname + '_blocks.csv'
    jblocks_csv = output_prefix + 'model_' + modelname + '_jblocks.csv'
    wblocks_csv = output_prefix + 'model_' + modelname + '_wblocks.csv'
    edgelist_parquet = output_prefix + modelname + '_edgelist_w_blocks.p'
    model_pickle = output_prefix + 'model_' + modelname + '.p'
    # Save it before export_blocks() in case something goes wrong
    model = pickle.load( open(model_pickle, "rb"))

    # Export blocks and edgelist with blocks
    model.export_blocks(output=blocks_csv, joutput=jblocks_csv, woutput=wblocks_csv, max_level=2)
    model.edgelist_w_blocks[['wid', 'jid', 'workers_per_job', 'jobs_per_worker', 
                             'jid_py', 'wid_py', 
                             'job_blocks_level_0', 'job_blocks_level_1', 'job_blocks_level_2', 
                             'worker_blocks_level_0', 'worker_blocks_level_1', 'worker_blocks_level_2']].to_parquet(edgelist_parquet)
    
    # Save the model
    model = pickle.load( open(model_pickle, "rb"))
    print('SBM section complete at', datetime.now())
    
    # Optionally run MCMC sweeps
    if run_mcmc:
        mcmc_file = output_prefix + 'model_' + modelname + '_mcmc.p'
        model.mcmc_sweeps(mcmc_file, tempsavedir=output_prefix, numiter=1000, seed=734)
        pickle.dump(model, open(model_pickle, "wb"))


edgelist_path = root + '/Data/derived/mayara_edgelist_1986_1990_3states.p'

# Define a list of parameter dictionaries for each SBM run
sbm_runs = [
    {
        'modelname': 'sbm_mayara_1986_1990',
        'fit_params': {},  # No additional parameters for this run
        'run_mcmc': True
    }
]

run_create_edgelist = False
if run_create_edgelist==True:
    create_edgelist(1986, 1990)

run = sbm_runs[0]
#def run_sbm_section(modelname, edgelist_path, fit_params=None, run_mcmc=True):

run_sbm_section(
        modelname=run['modelname'],
        edgelist_path = root + '/Data/derived/mayara_edgelist_1986_1990.p',
        fit_params=run.get('fit_params', None),
        run_mcmc=run.get('run_mcmc', True)
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
