# -*- coding: utf-8 -*-
"""
Created on Tue Sep  3 12:44:24 2024

@author: p13861161
"""


from datetime import datetime
import pickle
import pandas as pd
import geopandas as gpd
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
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc

import seaborn as sns
import matplotlib.pyplot as plt

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
        

if getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'
    sys.path.append(root + 'Code/Modules')

from mass_layoffs_parquet_functions import load_iotas_gammas, process_iotas_gammas, pull_estab_geos, event_studies_by_mkt_size, calculate_distance, compute_skill_variance

years = range(2013, 2016 + 1)
file_paths = [f"{rais}/parquet_novos/brasil{year}.parquet" for year in years]



# Pull region codes
region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_micro_cw = pd.DataFrame({'code_micro':region_codes.code_micro,'codemun':region_codes.code_munic//10})

# Load iotas and gammas and compute the expected number of jobs for each iota

iotas, gammas = load_iotas_gammas(root)

run_load_iotas_gammas = True
if run_load_iotas_gammas==True:
    iotas, gammas = load_iotas_gammas(root)
    E_N_gamma_given_iota, appended = process_iotas_gammas(root, iotas, gammas)
    E_N_gamma_given_iota.to_pickle(root + "Data/derived/mass_layoffs_E_N_gamma_given_iota.p", protocol=4)   
    E_N_gamma_given_iota.to_parquet(root + "Data/derived/mass_layoffs_E_N_gamma_given_iota.parquet")   
    # Compute skill and spatial variance of each iota
    weighted_variances = compute_skill_variance(appended, root)
    weighted_variances.to_pickle(root + "Data/derived/mass_layoffs_weighted_variances.p", protocol=4)  

 


date_formats ={    
    2004:"%m/%d/%Y",
    2005:"%m/%d/%Y",
    2006:"%m/%d/%Y",
    2007:"%m/%d/%Y",
    2008:"%m/%d/%Y",
    2009:"%m/%d/%Y",
    2010:"%m/%d/%Y",
    2011:"%m/%d/%Y",
    2012:"%m/%d/%Y",
    2013:"%m/%d/%Y",
    2014:"%m/%d/%Y",
    2015:"%m/%d/%Y",
    2016:"%d/%m/%Y",
    2017:"%d%b%Y",
    2018:"%d/%m/%Y",
    2019:"%d/%m/%Y"}

identify_laid_off_workers=True
if identify_laid_off_workers==True:
    dfs = []
    for year in years:
        print(year)
        file_path = f"{rais}/parquet_novos/brasil{year}.parquet"
        # Read the Parquet file
        table = pq.read_table(file_path, columns=['pis','id_estab','cnpj_raiz','causa_deslig','data_adm','mes_deslig','rem_med_sm'])
        
        # Convert PyArrow Table to pandas DataFrame
        df = table.to_pandas()
        
        # Convert date columns to datetime
        df['data_adm'] = pd.to_datetime(df['data_adm'], format=date_formats[year], errors='raise')
        #df['data_deslig'] = pd.to_datetime(df['data_deslig'], format=date_formats[year], errors='raise')
        
        # Rename 'pis' column to 'wid'
        df.rename(columns={'pis': 'wid'}, inplace=True)
        
        # Add 'year' column
        df['year'] = year
        
        dfs.append(df)
    
    # Concatenate all DataFrames
    worker_panel = pd.concat(dfs, ignore_index=True)
    del dfs
    
    worker_panel = worker_panel.rename(columns={'year':'layoff_year','mes_deslig':'layoff_month'})
    worker_panel['layoff_quarter'] = (worker_panel['layoff_month'] - 1) // 3 + 1
    
    # Drop people with average earnings less than minimum wage
    worker_panel = worker_panel.loc[worker_panel.rem_med_sm>=1]
    
    # Group by establishment and month
    worker_panel['total_employees_year'] = worker_panel.groupby(['id_estab', 'layoff_year'])['wid'].transform('nunique')
    worker_panel['layoff_type_indicator'] = (worker_panel['causa_deslig'] == 11).astype(int)
    
    # Monthly lay offs
    layoffs_by_month = worker_panel.groupby(['id_estab', 'layoff_year', 'total_employees_year', 'layoff_month']).agg({
        'wid': 'nunique',
        'layoff_type_indicator': 'mean'
    })
    layoffs_by_month = layoffs_by_month.reset_index().rename(columns={'wid':'laid_off_count'})
    layoffs_by_month['laid_off_share'] = layoffs_by_month['laid_off_count'] / layoffs_by_month['total_employees_year']
    # 0 means not laid off
    layoffs_by_month = layoffs_by_month.loc[layoffs_by_month['layoff_month']!=0]
    
    # quarterly lay offs
    layoffs_by_quarter = worker_panel.groupby(['id_estab', 'layoff_year', 'total_employees_year', 'layoff_quarter']).agg({
        'wid': 'nunique',
        'layoff_type_indicator': 'mean'
    })
    layoffs_by_quarter = layoffs_by_quarter.reset_index().rename(columns={'wid':'laid_off_count'})
    layoffs_by_quarter['laid_off_share'] = layoffs_by_quarter['laid_off_count'] / layoffs_by_quarter['total_employees_year']
    # 0 means not laid off
    layoffs_by_quarter = layoffs_by_quarter.loc[layoffs_by_quarter['layoff_quarter']!=0]
    
    mass_layoffs = layoffs_by_month.loc[(layoffs_by_month['total_employees_year']>50) & (layoffs_by_month['laid_off_share']>.5) & (layoffs_by_month['layoff_type_indicator']>.8)]
    
    laid_off_workers = worker_panel.merge(mass_layoffs[['id_estab','layoff_month','layoff_year']], validate='m:1', how='inner',indicator=True)
    laid_off_workers['wid'] = laid_off_workers['wid'].astype('Int64')
    laid_off_workers_set = pa.array(laid_off_workers['wid'].unique().dropna())
    
    mass_layoffs.to_parquet(root + "Data/derived/mass_layoffs.parquet")   
    laid_off_workers.to_parquet(root + "Data/derived/laid_off_workers.parquet")   



columns_to_keep = [
    'pis', 'id_estab', 'cnpj_raiz', 'data_adm', 'data_deslig', 'mes_deslig', 'causa_deslig',
    'clas_cnae20', 'cbo2002', 'codemun', 'data_nasc', 'idade', 'genero', 'grau_instr', 
    'salario', 'rem_med_sm', 'rem_med_r','nacionalidad', 'raca_cor', 'tipo_vinculo'
] + [f'vl_rem_{i:02d}' for i in range(1, 13)]

build_worker_panel = True
if build_worker_panel==True:
    dfs = []
    for year in range(2012,2019+1):
        print(year)
        ###################
        # Apply filters and load the raw data
        # Make the following restrictions:
        # - Worker is in the set of workers flagged as laid off in the previous step
        # - Private sector employment (tipo_vinculo in 30, 31, 35)
        # - State is in Sao Paulo, Rio de Janeiro, or Minas Gerais
        file_path = f"{rais}/parquet_novos/brasil{year}.parquet"
        # Read the Parquet file
        table = pq.read_table(file_path, columns=columns_to_keep)
        mask_pis = pc.is_in(table['pis'], laid_off_workers_set)
        # Filter out tipo_vinculo 30, 31, 35
        mask_tipo_vinculo = pc.invert(pc.is_in(table['tipo_vinculo'], pa.array([30, 31, 35])))
        # Filter codemun to our 3 states but first, handle null values
        codemun_filled = pc.if_else(pc.is_null(table['codemun']), 
                                    pc.cast(pa.scalar(99), pa.int64()), 
                                    table['codemun'])
        # Convert to string, take first two characters, convert back to int
        codemun_prefix = pc.cast(
            pc.utf8_slice_codeunits(pc.cast(codemun_filled, pa.string()), 0, 2),
            pa.int64()
        )
        mask_codemun = pc.is_in(codemun_prefix, pa.array([31, 33, 35]))
        # Mask for rem_med_sm >= 1
        mask_rem_med_sm = pc.greater_equal(table['rem_med_sm'], 1.0)
        # Mask for age restriction (between 22 and 62 in 2012)
        age_in_2012 = pc.subtract(table['idade'], year - 2012)
        mask_age = pc.and_(pc.greater_equal(age_in_2012, 22), pc.less_equal(age_in_2012, 62))
        
        # Combine all masks
        final_mask = pc.and_(mask_pis, 
                             pc.and_(mask_tipo_vinculo, 
                                     pc.and_(mask_codemun, 
                                             pc.and_(mask_rem_med_sm, mask_age))))

        filtered_table = table.filter(final_mask)
        # Convert to pandas DataFrame
        df = filtered_table.to_pandas()
                
        
        ###################
        # Create necessary variables
         
        # Add 'year' column
        df['year'] = year
        
        # Need to make wid and jid
        df['wid'] = df.pis.astype('Int64').astype(str)
        df['occ4'] = df['cbo2002'].astype(str).str[0:4]
        df['jid'] = df['id_estab'].astype(str).str.zfill(14) + '_' + df['occ4']

        # Save DOBs in a separate df. I forgot if/why this is actually necessary
        year_dob = df[['wid', 'data_nasc']].drop_duplicates()
        year_dob['data_nasc']     = pd.to_datetime(year_dob['data_nasc'],   format=date_formats[year], errors='raise')
        # XX worker_dob = pd.concat([worker_dob, year_dob])
                   
        # Merge on iotas and gammas
        df = df.merge(gammas, how='left', validate='m:1', on='jid', indicator='_merge_gamma')
        # Merge on iotas
        df = df.merge(iotas,  how='left', validate='m:1', on='wid', indicator='_merge_iota')
        
        # Create a separate df for defining market size
        df['ind2']          = df['clas_cnae20'] // 1000
        df = df.merge(muni_micro_cw[['codemun','code_micro']], on='codemun', how='left', validate='m:1')
       
        # Date formats are not consistent across data sets so convert to dates here. 
        df['data_adm']      = pd.to_datetime(df['data_adm'],    format=date_formats[year], errors='raise')
        df['data_deslig']   = pd.to_datetime(df['data_deslig'], format=date_formats[year], errors='raise')              
        df['data_nasc']     = pd.to_datetime(df['data_nasc'],   format=date_formats[year], errors='raise')
        df.rename(columns={'mes_deslig':'layoff_month'}, inplace=True)
        
        # Sometimes id_estab is defined but cnpj_raiz is not. Therefore, replace cnpj_raiz with the first 8 digits of id_estab. XX I can actually just not load cnpj_raiz to not mess with this. 
        df['id_firm'] = df['id_estab'].astype(str).str.zfill(14).str[:8].astype(int)
        dfs.append(df)
        
    laid_off_worker_panel = pd.concat(dfs, ignore_index=True)
    laid_off_worker_panel['yob'] = laid_off_worker_panel['year'] - laid_off_worker_panel['idade']
    # YOB is non-unique for a small number of people. Keep the mode in these cases
    yob_mode = laid_off_worker_panel.groupby('wid')['yob'].agg(lambda x: x.mode().iloc[0])
    laid_off_worker_panel['yob'] = laid_off_worker_panel['wid'].map(yob_mode)

    laid_off_worker_panel.to_parquet(root + "Data/derived/laid_off_worker_panel.parquet")   
    del dfs

if create_worker_panel_balanced==True:
    
    mass_layoffs            = pd.read_parquet(root + "Data/derived/mass_layoffs.parquet")   
    laid_off_worker_panel   = pd.read_parquet(root + "Data/derived/laid_off_worker_panel.parquet")   

    # Step 1: Reshape to long format on wid, year, and month
    value_vars = [f'vl_rem_{i:02d}' for i in range(1, 13)]
    worker_panel_long = pd.melt(laid_off_worker_panel, 
                                id_vars=['wid', 'id_estab', 'id_firm', 'data_adm', 'data_deslig', 'layoff_month', 'genero', 'iota','gamma',
                                         'causa_deslig', 'clas_cnae20', 'ind2', 'cbo2002', 'codemun', 'code_micro',
                                         'grau_instr', 'salario', 'rem_med_sm', 'rem_med_r', 'year', 'yob', 'raca_cor', 'nacionalidad'],
                                value_vars=value_vars, 
                                var_name='month', 
                                value_name='vl_rem')
    
    # Convert month from 'vl_rem_{i:02d}' to integer
    worker_panel_long['month'] = worker_panel_long['month'].str.extract('(\d+)').astype(int)
    
    # Restrict to people with at least 24 months of positive earnings
    valid_months = (worker_panel_long['vl_rem'] > 0)
    valid_month_counts = worker_panel_long[valid_months].groupby('wid').size()
    workers_to_keep = valid_month_counts[valid_month_counts >= 24].index
    worker_panel_long = worker_panel_long[worker_panel_long['wid'].isin(workers_to_keep)]
    
    
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
    
    
    # Step 4: Merge with mass layoff data and flag mass layoff months
    # Assuming closure_df contains the mass layoff data with columns 'id_estab' and 'data_encerramento'
    worker_panel_long = worker_panel_long.merge(mass_layoffs[['id_estab','layoff_month','layoff_year']],  
                left_on=['id_estab','layoff_month','year'],  
                right_on=['id_estab','layoff_month','layoff_year'], 
                validate='m:1', how='left',indicator=True)

    ''' OLD but this is where I left off. Next step would be to define the mass layoff flag as _merge='both' from the previous step

    worker_panel_long = worker_panel_long.merge(closure_df[['id_estab', 'data_encerramento']], on='id_estab', how='left', validate='m:1', indicator=True)
    worker_panel_long.drop(columns='_merge', inplace=True)
    '''
    # Create flag for worker being employed at estab in month in which mass layoff occurred. This is defined as the layoff firm being one's primary (highest earnings) firm in the month in which the firm closure occurred. 
    worker_panel_long['mass_layoff_flag'] = worker_panel_long._merge=='both'
    
    
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
    
    invariant = ['wid', 'iota', 'genero', 'grau_instr', 'yob', 'nacionalidad', 'raca_cor', 'mass_layoff_month']
    variant = ['id_estab', 'id_firm', 'gamma', 'data_adm', 'data_deslig', 'causa_deslig', 'clas_cnae20', 'ind2', 'cbo2002', 'codemun', 'code_micro', 'salario', 'rem_med_sm', 'rem_med_r', 'vl_rem', 'total_earnings_month', 'num_jobs_month', 'employment_indicator']
    
    unique_wids=worker_panel_long['wid'].unique()
    spine = pd.DataFrame({'wid':np.tile(unique_wids, 49), 'event_time':np.repeat(np.arange(-12,36+1),unique_wids.shape[0])})        
    worker_panel_balanced = spine.merge(worker_panel_long[['wid','event_time'] + variant], how='left', on=['wid','event_time'], indicator=True, validate='1:1')
  
    # Merge on time-invariant variables to ensure they are defined when the person is not employed
    worker_panel_balanced = worker_panel_balanced.merge(worker_panel_long[invariant].drop_duplicates(subset='wid', keep='last'), how='left', on='wid', indicator='_merge_invariant', validate='m:1')
    
    # create a calendar-year variable in the worker_panel_balance, label it as year
    worker_panel_balanced['calendar_date'] = worker_panel_balanced['mass_layoff_month'].dt.to_period('M') + worker_panel_balanced['event_time']
    worker_panel_balanced['year'] = worker_panel_balanced['calendar_date'].dt.year.replace(-1, pd.NA).astype('Int64') # For some reason NaTs are converted to -1 so fixing that

    '''
    # I think this block is no longer necessary because the old way I was computing calendar dates was dumb. The key was moving mass_layoff_month to the invarinat variable list
    # Use event_time to ensure we have a calendar date for all rows
    base_date = worker_panel_balanced.loc[worker_panel_balanced.event_time==0, ['wid','calendar_date']].rename(columns={'calendar_date':'base_date'})
    worker_panel_balanced = worker_panel_balanced.merge(base_date, how='left', on='wid', indicator='_merge_base_date', validate='m:1')
    dates = worker_panel_balanced['base_date'].values.astype('datetime64[M]')
    adjusted_dates = dates + np.array(worker_panel_balanced['event_time'], dtype='timedelta64[M]')
    worker_panel_balanced['calendar_date'] = adjusted_dates
    '''
    
    # Calculate tenure at event_time = -1 and merge back to all observations
    tenure_at_minus_one = worker_panel_balanced.loc[worker_panel_balanced['event_time'] == -1,['wid','mass_layoff_month','data_adm']]
    tenure_at_minus_one['tenure_years'] = (tenure_at_minus_one['mass_layoff_month'] - tenure_at_minus_one['data_adm']).dt.days / 365.25
    worker_panel_balanced = worker_panel_balanced.merge(tenure_at_minus_one[['wid', 'tenure_years']], on='wid', how='left')
    
    # Filter the sample to include only workers with at least 1 year of tenure
    
    # Check for people who are employed at the pre-layoff firm post-layoff. Drop these people. Also also identify each worker's first post-layoff firm and identify layoff firms where >50% of laid off workers end up at the same post-layoff firm. These should not be counted as true layoffs. 
    
    
    # Merge on iotas and then E_N_gamma_given_iota
    # XX This should no longer be necessary as of 7/11 because I am keeping iota and gamma when creating the panel now
    #worker_panel_balanced['wid'] = worker_panel_balanced.wid.astype('Int64').astype(str)
    #worker_panel_balanced = worker_panel_balanced.merge(iotas, how='inner', validate='m:1', on='wid',indicator='_merge_iotas')
    #worker_panel_balanced = worker_panel_balanced.merge(E_N_gamma_given_iota, how='inner', validate='m:1', on='iota')
    
    
    ''' I don't see what the point of this code is. Probably delete it
    # XX need to do this for everyone in the 3 states in 2013-2016, not just the mass layoff sample
    worker_panel['wid'] = worker_panel.wid.astype('Int64').astype(str)
    worker_panel = worker_panel.merge(iotas, how='inner', validate='m:1', on='wid',indicator='_merge_iotas')
    print('Merge stats for iotas')
    print(worker_panel._merge_iotas.value_counts())
    worker_panel.drop(columns=['_merge_iotas'], inplace=True)
    '''
    
    
    ######
    # Create variables for regression in Moretti and Yi's equation 3
    
    # Some workers have multiple YOBs listed so I'll just take the mode
    worker_panel_balanced['age']    = worker_panel_balanced['year'] - worker_panel_balanced['yob']
    worker_panel_balanced['age_sq'] = worker_panel_balanced['age']**2
    worker_panel_balanced['foreign']= (worker_panel_balanced['nacionalidad']!=10)
    worker_panel_balanced['raca_cor'] = pd.Categorical(worker_panel_balanced['raca_cor'])
    worker_panel_balanced['genero'] = pd.Categorical(worker_panel_balanced['genero'])
    
    # XX Could merge on coordinates here and then compute distances below, once we have pre-layoff and post-layoff id_estab
    ###########################
    # GEOLOCATION FOR RAIS ESTABLISHMENTS
    years = list(range(firstyear_panel,lastyear_panel+1))

    geo_estab = pull_estab_geos(years, rais)
    # The municipality geolocation comes from mkt_geography_stats.py that was run in the linux server because we don't have geobr on Windows
    # This is created in Code/pull_munis.py which I ran on Linux python3 but doesnt work here b/c geobr is not installed
    munis = gpd.read_file(root + f'/Data/derived/munis_{modelname}.geojson')
    ###########################################
    
    ## XXBM: Merge geolocation to the worker balanced panel
        
    # Merge municipality geolocation to worker_balanced_panel
    worker_panel_balanced = worker_panel_balanced.merge(munis[['lon_munic', 'lat_munic', 'codemun', 'utm_lon_munic', 'utm_lat_munic']], on='codemun',how='left', indicator='_merge_munics')
    worker_panel_balanced['_merge_munics'].value_counts()

    # Merge establishment geolocation to worker_balanced_panel
    worker_panel_balanced['id_estab'] = worker_panel_balanced['id_estab'].astype(str).str.zfill(14)
    worker_panel_balanced = worker_panel_balanced.merge(geo_estab[['id_estab', 'year', 'Addr_type', 'lon_estab', 'lat_estab', 'utm_lat_estab', 'utm_lon_estab', 'h3_res7']], on=['year', 'id_estab'], how='left', validate='m:1', indicator='_merge_geo_estab')
    worker_panel_balanced.columns
    worker_panel_balanced._merge_geo_estab.value_counts()
    
    del geo_estab # Save memory

    # Stats on establishments with missing geolocation
    for l in ['lat_', 'lon_', 'utm_lat_', 'utm_lon_']:
        print('----------------------------------------')
        print('MISSING VALUES FOR ' + l + 'munic')
        print(worker_panel_balanced[l + 'munic'].isna().sum(),'/',worker_panel_balanced.shape[0])

    # Replace NaN values in *_estab columns with values from their *_munic counterparts
    for l in ['lat_', 'lon_', 'utm_lat_', 'utm_lon_']:
        print('----------------------------------------')
        print('MISSING VALUES FOR ' + l + 'estab, BEFORE INPUTATION')
        print(worker_panel_balanced[l + 'estab'].isna().sum(),'/',worker_panel_balanced.shape[0])
        worker_panel_balanced[l + 'estab'] = worker_panel_balanced[l + 'estab'].fillna(worker_panel_balanced[l + 'munic'])
        print('Missings after replacement with ' + l + 'munic, after inputation')
        print(worker_panel_balanced[l + 'estab'].isna().sum(),'/',worker_panel_balanced.shape[0])


    ###########################################
    
    ###################
    # Create pre- and post-layoff variables as well as indicators for changing jobs, occs, industries, etc
    # Get pre-layoff values
    pre_layoff = worker_panel_balanced[worker_panel_balanced['event_time'] == -1].set_index('wid')
    
    # Get first post-layoff values
    post_layoff = worker_panel_balanced[(worker_panel_balanced['event_time'] > 0) & 
                                        (worker_panel_balanced['employment_indicator'] == 1)].groupby('wid').first()
    # Loop over the specified variables
    variables = ['id_firm', 'id_estab', 'cbo2002', 'clas_cnae20', 'ind2', 'code_micro', 'gamma', 'utm_lat_estab', 'utm_lon_estab']
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
    
    
    worker_panel_balanced = worker_panel_balanced.sort_values(by=['wid','event_time'])
    worker_panel_balanced.to_pickle(root + "Data/derived/mass_layoffs_worker_panel_balanced_temp.p", protocol=4)
    worker_panel_balanced.to_parquet(root + "Data/derived/mass_layoffs_worker_panel_balanced_temp.parquet")
    
    # Identify and workers who are employed at the "layoff firm after the layoff occurred and then drop them
    wids_to_drop = worker_panel_balanced.loc[(worker_panel_balanced['same_id_firm'] == 1) & (worker_panel_balanced['event_time'] > 0), 'wid'].unique()
    worker_panel_balanced = worker_panel_balanced[~worker_panel_balanced['wid'].isin(wids_to_drop)]
    
    
    for a in  worker_panel_balanced.dtypes:
        print(a)
    
    
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
    
    worker_panel_balanced = worker_panel_balanced[~worker_panel_balanced['pre_layoff_id_firm'].isin(flagged_firms)]
    
    
    
    
    # Recode the 'grau_instr' variable into coarser bins
    bins = [0, 4, 6, 7, 8, 11]
    labels = ['Less than middle school', 'Less than HS', 'HS', 'Some college', 'College']
    worker_panel_balanced['educ'] = pd.cut(worker_panel_balanced['grau_instr'], bins=bins, labels=labels, right=True)
    
    
    worker_panel_balanced['ind2'] = worker_panel_balanced['clas_cnae20'] // 1000
    t_minus_1 = worker_panel_balanced[worker_panel_balanced['event_time'] == -1][['wid', 'code_micro','ind2']]
    worker_panel_balanced = worker_panel_balanced.merge(t_minus_1, on='wid', suffixes=('', '_t_minus_1'))
    
    worker_panel_balanced['educ_micro']         = pd.Categorical(worker_panel_balanced.educ.astype(str) + '_' + worker_panel_balanced.code_micro_t_minus_1.astype(str)) 
    worker_panel_balanced['educ_ind2']          = pd.Categorical(worker_panel_balanced.educ.astype(str) + '_' + worker_panel_balanced.ind2_t_minus_1.astype(str))
    worker_panel_balanced['educ_layoff_date']   = pd.Categorical(worker_panel_balanced.educ.astype(str) + '_' + worker_panel_balanced.mass_layoff_month.astype(str)) 
    
    
    # Replace NAs with 0 for earnings and employment
    worker_panel_balanced['employment_indicator'].fillna(0, inplace=True)
    worker_panel_balanced['total_earnings_month'].fillna(0, inplace=True)
    worker_panel_balanced['vl_rem'].fillna(0, inplace=True)
    
    # Compute the ratio of actual wages to wages in terms of the minimum wage to get a deflator to convert vl_rem to vl_rem_sm
    worker_panel_balanced['ratio'] = worker_panel_balanced['rem_med_r'] / worker_panel_balanced['rem_med_sm']
    deflator = worker_panel_balanced.groupby('calendar_date')['ratio'].mean().reset_index(name='deflator')
    worker_panel_balanced.drop(columns='ratio', inplace=True)
    worker_panel_balanced = worker_panel_balanced.merge(deflator, on='calendar_date', how='left', validate='m:1')
    
    worker_panel_balanced['vl_rem_sm'] = worker_panel_balanced['vl_rem']/worker_panel_balanced['deflator']
    worker_panel_balanced['total_earnings_month_sm'] = worker_panel_balanced['total_earnings_month']/worker_panel_balanced['deflator']
    worker_panel_balanced['total_earnings_month_sm'].fillna(0, inplace=True)
    
    
    ############################################################################
    # Cutting on market size
    '''
    I didn't run the code to create these data sets being merged on
    # Merge on P_ig and E_N
    E_N_gamma_given_iota['market_size_tercile'] = pd.qcut(E_N_gamma_given_iota['E_N_gamma_given_iota'], q=3, labels=['low', 'medium', 'high'])
    # Drop markets with fewer than 10 workers
    mkt_size_df_worker = mkt_size_df_worker.loc[mkt_size_df_worker['count']>=10]
    mkt_size_df_worker['market_size_tercile_ind2_micro'] = pd.qcut(mkt_size_df_worker['count'], q=3, labels=['low', 'medium', 'high'])
    
    worker_panel_balanced = worker_panel_balanced.merge(E_N_gamma_given_iota, on='iota', how='left', validate='m:1')
    worker_panel_balanced = worker_panel_balanced.merge(mkt_size_df_worker.drop(columns='count'), left_on=['ind2','code_micro'], right_on=['ind2','code_micro'], how='left', validate='m:1')
    '''
    
    worker_panel_balanced.to_pickle(root + "Data/derived/mass_layoffs_worker_panel_balanced.p", protocol=4)
    #worker_panel_balanced.to_parquet(root + "Data/derived/mass_layoffs_worker_panel_balanced.parquet")
    with open(root + "Data/derived/mass_layoffs_worker_panel_balanced_dtypes.p", 'wb') as f:
        pickle.dump(worker_panel_balanced.dtypes.to_dict(), f)
        








# What to do next
# - We need a list of all establishments in our period (2013-2016) in our 3 states of interest, with a mass layoff indicator for each worker-job match (was the worker laid off in a mass layoff at this firm?), establishment geolocation (We have the code for this), iotas and gammas










'''
# As a reference, here is the tabulation of worker-weighted layoff reasons from a previous version of the build.
# Most likely we will want to restrict to causa_deslig==11
causa_deslig
31.0    1247285    Employee transfer between establishments of the same company or to another company, without burden to the assignor
11.0     967810    Termination of employment contract without cause at the employer's initiative or dismissal of an effective position server or dismissal of office in committee
12.0     453173    Termination of the employment contract
30.0     114279    Employee transfer between establishments of the same company or to another company, with burden on the assignor
21.0      38623    Termination without cause at the initiative of the employee or dismissal of effective position at the request of the server
34.0       6732
10.0       5689
70.0       3836
33.0       3041
20.0       2911
76.0       1006
60.0        223
40.0        167
32.0         43
72.0         12
62.0         10
50.0          8
75.0          6
74.0          6
63.0          2
73.0          2
79.0          2
22.0          1
64.0          1
Name: count, dtype: int64



    
    # Cutting on whether causa_deslig==1 does a pretty good job. In events we flagged as mass layoffs the share of workers with causa_deslig==11 is typically close to 0 or close to 1
    plt.figure(figsize=(10, 6))
    sns.histplot(data=layoffs_by_month.loc[(layoffs_by_month['total_employees_year']>50) & (layoffs_by_month['laid_off_share']>.5)], x='layoff_type_indicator', bins=20, kde=True)
    plt.title('Histogram of Layoff Type Indicator')
    plt.xlabel('Layoff Type Indicator')
    plt.ylabel('Frequency')
    plt.grid(True, alpha=0.3)
    plt.show()
'''
