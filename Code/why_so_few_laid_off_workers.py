# -*- coding: utf-8 -*-
"""
Created on Thu Aug 29 09:51:16 2024

@author: p13861161
"""


from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import platform
import sys
import getpass
from linearmodels.iv import AbsorbingLS
from linearmodels.panel import PanelOLS
import statsmodels.api as sm
import matplotlib.pyplot as plt
from tqdm import tqdm # to calculate progress of some operations for geolocation
import pyproj # for converting geographic degree measures of lat and lon to the UTM system (i.e. in meters)
#from scipy.integrate import simps # to calculate the AUC for the decay function
from dateutil.relativedelta import relativedelta

modelname = '3states_2013to2016_mcmc'
firstyear_panel = 2012 
lastyear_panel = 2019
nrows = None
pull_raw = True
run_load_iotas_gammas = True
create_worker_panel_balanced = True
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)

homedir = os.path.expanduser('~')
os_name = platform.system()
if getpass.getuser()=='p13861161':
    if os_name == 'Windows':
        print("Running on Windows") 
        root = "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/"
        rais = "//storage6/bases/DADOS/RESTRITO/RAIS/"
        sys.path.append(root + 'Code/Modules')
    elif os_name == 'Linux':
        print("Running on Linux") 
        root = "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/"
        rais = "~/rais/RAIS/"
        sys.path.append(root + 'Code/Modules')
        # These all require torch
        import torch
        from torch_mle import torch_mle
        import bisbm
        from mle_load_fulldata import mle_load_fulldata
        from normalization_k import normalization_k
        from alphas_func import load_alphas
        import solve_model_functions as smf
        from correlogram import correlogram

    
closure_df          = pd.read_pickle(root + "Data/derived/mass_layoffs_closure_df.p")
worker_panel        = pd.read_pickle(root + "Data/derived/mass_layoffs_worker_panel.p")
E_N_gamma_given_iota= pd.read_pickle(root + "Data/derived/mass_layoffs_E_N_gamma_given_iota.p")
mkt_size_df_worker  = pd.read_pickle(root + "Data/derived/mass_layoffs_mkt_size_df_worker.p")


# We have lots of firms that close, but the vast majority have 0 active employees as of Dec 31. I think this makes sense if they actually closed. But maybe it's indicative of a problem?

closure_df.id_estab.nunique()
#Out[29]: 418290

closure_df.qt_vinc_ativos.describe()
'''
Out[32]: 
count    418290.000000
mean          0.002831
std           1.160370
min           0.000000
25%           0.000000
50%           0.000000
75%           0.000000
max         748.000000
Name: qt_vinc_ativos, dtype: float64
'''


# 641,000 workers flagged as being employed at a firm that closed in the year that it closed. So where do all these people go?
possible_laid_off_worker_list = pd.read_pickle(root + "Data/derived/mass_layoffs_possible_laid_off_worker_list.p")

#
worker_panel = pd.read_parquet(root + "Data/derived/mass_layoffs_worker_panel.parquet")
# 281,000 unique workers left after restricting to our 3 largest states, private sector employment, dropping jobs with avg earnings < minimum wage in process_worker_data()
worker_panel.wid.nunique()
# Out[40]: 281086


# Down to only 1000 unique workers whenwe cut to the balanced panel
worker_panel_balanced = pd.read_pickle(root + "Data/derived/mass_layoffs_worker_panel_balanced.p")
worker_panel_balanced.wid.nunique()
# Out[42]: 1054


# Stuff from the section where we create worker_panel_balanced
if 1==1:
    # Step 1: Reshape to long format on wid, year, and month
    value_vars = [f'vl_rem_{i:02d}' for i in range(1, 13)]
    worker_panel_long = pd.melt(worker_panel, 
                                id_vars=['wid', 'id_estab', 'id_firm', 'data_adm', 'data_deslig', 'genero', 'iota','gamma',
                                         'causa_deslig', 'clas_cnae20', 'ind2', 'cbo2002', 'codemun', 'code_micro', 'yob',
                                         'grau_instr', 'salario', 'rem_med_sm', 'rem_med_r', 'year', 'raca_cor', 'nacionalidad'],
                                value_vars=value_vars, 
                                var_name='month', 
                                value_name='vl_rem')
    
    # Convert month from 'vl_rem_{i:02d}' to integer
    worker_panel_long['month'] = worker_panel_long['month'].str.extract('(\d+)').astype(int)
    
    # Restrict to people with at least 24 months of positive earnings
    valid_months = (worker_panel_long['vl_rem'] > 0)
    valid_month_counts = worker_panel_long[valid_months].groupby('wid').size()
    workers_to_keep = valid_month_counts[valid_month_counts >= 24].index
    
    # The 24 months restriction drops a non-trivial number of workers but still most remain
    worker_panel_long.wid.nunique()
    #Out[45]: 281086
    len(workers_to_keep)
    #Out[47]: 237739
    

    # Print the shape of the resulting dataframe
    print(f"Resulting dataframe shape: {worker_panel_long.shape}")
    
    
    # Step 2: Keep only the job with the highest earnings for each worker-month. But also compute the sum of earnings across all jobs. 
    worker_panel_long['total_earnings_month'] = worker_panel_long.groupby(['wid', 'year', 'month'])['vl_rem'].transform('sum')
    worker_panel_long['num_jobs_month'] = worker_panel_long.groupby(['wid', 'year', 'month'])['vl_rem'].transform('count')
    
    worker_panel_long = worker_panel_long.sort_values(by='vl_rem', ascending=False).drop_duplicates(subset=['wid', 'year', 'month'])
    worker_panel_long.sort_values(by=['wid', 'year', 'month'], inplace=True)
    
    # Combine 'year' and 'month' into a single date variable
    worker_panel_long['calendar_date'] = pd.to_datetime(worker_panel_long[['year','month']].assign(day=1))
    
    
    # Step 3: Flag months where the worker is employed at the given establishment
    worker_panel_long['employment_indicator'] = (
        (worker_panel_long['data_adm'] <= worker_panel_long['calendar_date']) & 
        ((worker_panel_long['data_deslig'].isna()) | (worker_panel_long['data_deslig'] >= worker_panel_long['calendar_date'])) 
    ).astype(int)
    
    # Still good here
    worker_panel_long.wid.nunique()
    #Out[51]: 237739

    # Step 4: Merge with mass layoff data and flag mass layoff months
    # Assuming closure_df contains the mass layoff data with columns 'id_estab' and 'data_encerramento'
    
    
    worker_panel_long = worker_panel_long.merge(closure_df[['id_estab', 'data_encerramento']], on='id_estab', how='left', validate='m:1', indicator=True)
    worker_panel_long.wid.nunique()
    #Out[53]: 237739

    worker_panel_long._merge.value_counts()
    #Out[54]: 
    #_merge
    #left_only     13311653
    #both           5824147
    #right_only           0
    #Name: count, dtype: int64
    #worker_panel_long.drop(columns='_merge', inplace=True)


    # Create flag for worker being employed at estab in month in which mass layoff occurred. This is defined as the layoff firm being one's primary (highest earnings) firm in the month in which the firm closure occurred. 
    worker_panel_long['mass_layoff_flag'] = (worker_panel_long['calendar_date'].dt.year == worker_panel_long['data_encerramento'].dt.year) & (worker_panel_long['calendar_date'].dt.month == worker_panel_long['data_encerramento'].dt.month) & (worker_panel_long['employment_indicator']==1)
    
    ###############
    # This is where we are losing a lot of people
    worker_panel_long['mass_layoff_flag'].value_counts()
    #Out[56]: 
    #mass_layoff_flag
    #False    19085204
    #True        50596
    
    # Maybe the year restriction here is too strict? But almost certainly the month restriction is too strict. 
    (worker_panel_long['calendar_date'].dt.year == worker_panel_long['data_encerramento'].dt.year).mean()
    #Out[57]: np.float64(0.07768303389458502)

    ((worker_panel_long['calendar_date'].dt.year == worker_panel_long['data_encerramento'].dt.year) & (worker_panel_long['employment_indicator']==1)).mean() 
    #Out[58]: np.float64(0.057233039642972855)

    (worker_panel_long['calendar_date'].dt.month == worker_panel_long['data_encerramento'].dt.month).mean()
    #Out[59]: np.float64(0.023807732104223496)

    ((worker_panel_long['calendar_date'].dt.year == worker_panel_long['data_encerramento'].dt.year) & (worker_panel_long['calendar_date'].dt.month == worker_panel_long['data_encerramento'].dt.month) & (worker_panel_long['employment_indicator']==1)).mean()
    #Out[60]: np.float64(0.0026440493734257255)
 
    
    # Create a variable within each wid containing the first month in which the worker was exposed to an establishment closure
    worker_panel_long['mass_layoff_month'] = worker_panel_long.loc[worker_panel_long['mass_layoff_flag']==1, 'calendar_date']
    worker_panel_long['mass_layoff_month'] = worker_panel_long.groupby('wid')['mass_layoff_month'].transform('min')
    
    # Flag and keep workers who were employed at a mass layoff estab when the mass layoff happened
    worker_panel_long['mass_layoff_ever'] = worker_panel_long.groupby('wid')['mass_layoff_flag'].transform('max')
    worker_panel_long = worker_panel_long.loc[worker_panel_long['mass_layoff_ever']==1]
    
    
    worker_panel_long['event_time'] = (worker_panel_long['calendar_date'].dt.to_period('M') -worker_panel_long['mass_layoff_month'].dt.to_period('M')).apply(lambda x: x.n if pd.notna(x) else np.nan)
    
    # Identify characteristics of the firm that closes
    pre_layoff_obs = worker_panel_long.loc[(worker_panel_long['mass_layoff_month']==worker_panel_long['calendar_date']), ['wid', 'id_estab', 'id_firm', 'clas_cnae20', 'cbo2002', 'codemun', 'code_micro', 'salario', 'calendar_date']]
    pre_layoff_obs = pre_layoff_obs.rename(columns={col: col + '_pre' for col in pre_layoff_obs.columns if col != 'wid'})
    
    
    
    
    
    ############################################################################################################
    # Create a balanced worker panel where time is measured by event time
    
    invariant = ['wid', 'iota', 'genero', 'grau_instr', 'yob', 'nacionalidad', 'raca_cor']
    variant = ['id_estab', 'id_firm', 'gamma', 'data_adm', 'data_deslig', 'causa_deslig', 'clas_cnae20', 'ind2', 'cbo2002', 'codemun', 'code_micro', 'salario', 'rem_med_sm', 'rem_med_r', 'vl_rem', 'total_earnings_month', 'num_jobs_month', 'calendar_date',  'employment_indicator', 'data_encerramento', 'mass_layoff_month']
    
    # Still have 50K people here. We lose 98% of these by the end
    unique_wids=worker_panel_long['wid'].unique()
    len(unique_wids)
    #Out[64]: 50399
    
    
    
    
    spine = pd.DataFrame({'wid':np.tile(unique_wids, 49), 'event_time':np.repeat(np.arange(-12,36+1),unique_wids.shape[0])})        
    worker_panel_balanced = spine.merge(worker_panel_long[['wid','event_time'] + variant], how='left', on=['wid','event_time'], indicator=True, validate='1:1')
    # create a calendar-year variable in the worker_panel_balance, label it as year
    worker_panel_balanced['calendar_date'] = worker_panel_balanced['mass_layoff_month'].dt.to_period('M') + worker_panel_balanced['event_time']
    worker_panel_balanced['year'] = worker_panel_balanced['calendar_date'].dt.year.replace(-1, pd.NA).astype('Int64') # For some reason NaTs are converted to -1 so fixing that
    
    # Merge on time-invariant variables to ensure they are defined when the person is not employed
    worker_panel_balanced = worker_panel_balanced.merge(worker_panel_long[invariant].drop_duplicates(subset='wid', keep='last'), how='left', on='wid', indicator='_merge_invariant', validate='m:1')
    
    # Use event_time to ensure we have a calendar date for all rows
    base_date = worker_panel_balanced.loc[worker_panel_balanced.event_time==0, ['wid','calendar_date']].rename(columns={'calendar_date':'base_date'})
    worker_panel_balanced = worker_panel_balanced.merge(base_date, how='left', on='wid', indicator='_merge_base_date', validate='m:1')
    dates = worker_panel_balanced['base_date'].values.astype('datetime64[M]')
    adjusted_dates = dates + np.array(worker_panel_balanced['event_time'], dtype='timedelta64[M]')
    worker_panel_balanced['calendar_date'] = adjusted_dates
    
    
    # Calculate tenure at event_time = -1 and merge back to all observations
    tenure_at_minus_one = worker_panel_balanced.loc[worker_panel_balanced['event_time'] == -1,['wid','mass_layoff_month','data_adm']]
    tenure_at_minus_one['tenure_years'] = (tenure_at_minus_one['mass_layoff_month'] - tenure_at_minus_one['data_adm']).dt.days / 365.25
    worker_panel_balanced = worker_panel_balanced.merge(tenure_at_minus_one[['wid', 'tenure_years']], on='wid', how='left')
    
    print(worker_panel_balanced.wid.nunique())
    # 50399
 
    
    
    # Some workers have multiple YOBs listed so I'll just take the mode
    worker_panel_balanced['age']    = (worker_panel_balanced['calendar_date'].dt.to_period('Y') - pd.to_datetime(worker_panel_balanced['yob'].astype(int), format='%Y').dt.to_period('Y') ).apply(lambda x: x.n if pd.notna(x) else np.nan)
    worker_panel_balanced['age_sq'] = worker_panel_balanced['age']**2
    worker_panel_balanced['foreign']= (worker_panel_balanced['nacionalidad']!=10)
    worker_panel_balanced['raca_cor'] = pd.Categorical(worker_panel_balanced['raca_cor'])
    worker_panel_balanced['genero'] = pd.Categorical(worker_panel_balanced['genero'])
    
   
    ####
    # Skipping the merge on geography stuff
    ####
    
    
    pre_layoff = worker_panel_balanced[worker_panel_balanced['event_time'] == -1].set_index('wid')
    
    # Get first post-layoff values
    post_layoff = worker_panel_balanced[(worker_panel_balanced['event_time'] > 0) & 
                                        (worker_panel_balanced['employment_indicator'] == 1)].groupby('wid').first()
    # Loop over the specified variables
    variables = ['id_firm', 'id_estab', 'cbo2002', 'clas_cnae20', 'ind2', 'code_micro', 'gamma'] #, 'utm_lat_estab', 'utm_lon_estab'
    for var in variables:
        pre_col = f'pre_layoff_{var}'
        post_col = f'post_layoff_{var}'
        same_first_col = f'same_first_{var}'
        same_col = f'same_{var}'
        
        worker_panel_balanced[pre_col] = worker_panel_balanced['wid'].map(pre_layoff[var])
        worker_panel_balanced[post_col] = worker_panel_balanced['wid'].map(post_layoff[var])
        worker_panel_balanced[same_first_col] = (worker_panel_balanced[pre_col] == worker_panel_balanced[post_col]).astype(int)
        worker_panel_balanced[same_col] = (worker_panel_balanced[pre_col] == worker_panel_balanced[var]).astype(int)
    
    # Get first reemployment time
    first_reemployment = worker_panel_balanced[(worker_panel_balanced['event_time'] > 0) & 
                                               (worker_panel_balanced['employment_indicator'] == 1)].groupby('wid')['event_time'].min()
    worker_panel_balanced['first_reemployment_time'] = worker_panel_balanced['wid'].map(first_reemployment)
    
    
    
    
    # Identify and workers who are employed at the "layoff firm after the layoff occurred and then drop them
    wids_to_drop = worker_panel_balanced.loc[(worker_panel_balanced['same_id_firm'] == 1) & (worker_panel_balanced['event_time'] > 0), 'wid'].unique()
    print(len(wids_to_drop))
    #15425
    worker_panel_balanced = worker_panel_balanced[~worker_panel_balanced['wid'].isin(wids_to_drop)]
    
    worker_panel_balanced.wid.nunique()
    #Out[73]: 34974
    
    
    
    # Supplementary steps to identify and drop firms where >50% of laid off workers end up employed at the same firm as each other post-layoff
    
    # Step 1: Count the number of workers moving to each post_layoff_id_firm for each pre_layoff_id_firm
    grouped = worker_panel_balanced.loc[worker_panel_balanced.event_time==0].groupby(['pre_layoff_id_firm', 'post_layoff_id_firm']).size().reset_index(name='counts')
    
    # Step 2: Calculate the total number of workers for each pre_layoff_id_firm
    total_workers = grouped.groupby('pre_layoff_id_firm')['counts'].sum().reset_index(name='total')
    
    # Step 3: Merge to calculate the share of each post_layoff_id_firm
    grouped = grouped.merge(total_workers, on='pre_layoff_id_firm')
    grouped['share'] = grouped['counts'] / grouped['total']
    
    # Step 4: Flag pre_layoff_id_firm where any post_layoff_id_firm has a share > 50%
    flagged_firms = grouped[grouped['share'] > 0.5]['pre_layoff_id_firm'].unique()
    
    # Display the flagged firms
    print("Firms where a single post-layoff firm has >50% share:")
    print(flagged_firms)
    
    # This is where we lose everyone else
    worker_panel_balanced[~worker_panel_balanced['pre_layoff_id_firm'].isin(flagged_firms)].wid.nunique()
    # Out[76]: 1054
    
    worker_panel_balanced = worker_panel_balanced[~worker_panel_balanced['pre_layoff_id_firm'].isin(flagged_firms)]
    
    
    
    