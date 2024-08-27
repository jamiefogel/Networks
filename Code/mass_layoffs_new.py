# -*- coding: utf-8 -*-
"""
Created on Mon May 13 20:40:18 2024

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


nrows = None
pull_raw = True
load_iotas_gammas = True
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


from merge_aguinaldo_onet import merge_aguinaldo_onet



def load_iotas_gammas(root):
    # Load iotas and gammas                    
    jcolumns = ['jid']     
    wcolumns = ['wid']
    jrename = {}
    wrename = {}
    for l in range(0,1):
        jcolumns = jcolumns + ['job_blocks_level_'+str(l)]
        jrename['job_blocks_level_'+str(l)] = 'gamma'
        wcolumns = wcolumns + ['worker_blocks_level_'+str(l)]
        wrename['worker_blocks_level_'+str(l)] = 'iota'
    
    gammas = pd.read_csv(root + '/Data/derived/sbm_output/model_3states_2013to2016_mcmc_jblocks.csv', usecols=jcolumns).rename(columns=jrename)
    iotas  = pd.read_csv(root + '/Data/derived/sbm_output/model_3states_2013to2016_mcmc_wblocks.csv', usecols=wcolumns).rename(columns=wrename)
    iotas['wid'] = iotas['wid'].astype(str)
    return iotas, gammas

def process_iotas_gammas(root, iotas, gammas):

    # Load data that was used for the SBM so I can compute P_gi
    # Available columns: ['cbo2002', 'clas_cnae20', 'codemun', 'data_adm', 'data_deslig', 'data_nasc', 'horas_contr', 'id_estab', 'idade', 'ind2', 'jid', 'occ4', 'rem_dez_r', 'sector_IBGE', 'tipo_salario', 'tipo_vinculo', 'wid', 'year', 'yob']
    appended = pd.read_pickle(root + 'Data/derived/appended_sbm_3states_2013to2016_new.p')
    appended.to_parquet(root + 'Data/derived/appended_sbm_3states_2013to2016_new.parquet')
    appended = appended[['wid','jid','year','ind2','codemun','cbo2002','clas_cnae20']]
    
    
    ###################################################################################
    ## XXBM: it seems like we need gammas for the command below 
    iotas, gammas = load_iotas_gammas(root)
    ###################################################################################
    
    # XX Should I be using 2013to2016_new or 2013to2016_mcmc
    # Merge on gammas
    appended = appended.merge(gammas, how='inner', validate='m:1', on='jid')
    # Merge on iotas
    appended = appended.merge(iotas,  how='inner', validate='m:1', on='wid')
    
    
    # Calculate probabilites
    N_gamma = appended['gamma'].value_counts().reset_index().rename(columns={'count': 'N_gamma'})
    N_iota  = appended['iota'].value_counts().reset_index().rename(columns={'count': 'N_iota'})
    N_iota_gamma = appended.groupby(['iota', 'gamma']).size()
    
    # Create a DataFrame for N_iota_gamma
    N_iota_gamma_df = N_iota_gamma.reset_index(name='N_iota_gamma')
    # Merge N_iota and N_iota_gamma to calculate P[gamma | iota]
    N_iota_gamma_df = N_iota_gamma_df.merge(N_iota, on='iota')
    N_iota_gamma_df['P_gamma_given_iota'] = N_iota_gamma_df['N_iota_gamma'] / N_iota_gamma_df['N_iota']
    # Merge N_gamma to calculate E[N_gamma | iota]
    N_iota_gamma_df = N_iota_gamma_df.merge(N_gamma, on='gamma')
    N_iota_gamma_df['E_N_gamma_given_iota'] = N_iota_gamma_df['N_gamma'] * N_iota_gamma_df['P_gamma_given_iota']
    
    # Sum up E[N_gamma | iota] for each worker type
    E_N_gamma_given_iota = N_iota_gamma_df.groupby('iota')['E_N_gamma_given_iota'].sum()
   
    E_N_gamma_given_iota = E_N_gamma_given_iota.reset_index()
    E_N_gamma_given_iota['iota'] = E_N_gamma_given_iota['iota'].astype(int)
    return E_N_gamma_given_iota, appended
  
    
  
''' Exploratory code for computing spatial variances. This is taken from lines 230-300 of mkt_geography_stats.py



# Converting coordinates to UTM, so the units are in meters
# Function to convert geographic coordinates to UTM using a fixed zone 23S for Sao Paulo
# Create the transformer object for UTM zone 23S
transformer = pyproj.Transformer.from_crs("epsg:4326", "epsg:32723", always_xy=True)
# Function to convert geographic coordinates to UTM
def convert_to_utm(lon, lat):
    return transformer.transform(lon, lat)


GEOLOCATION FOR RAIS ESTABLISHMENTS

# filepath: \\storage6\bases\DADOS\RESTRITO\RAIS\geocode
# file of type: rais_geolocalizada_2009
years = appended.year.unique().tolist()

high_precision_cat = ['POI',
                        'PointAddress',
                        'StreetAddress',
                        'StreetAddressExt',
                        'StreetName',
                        'street_number',
                        'route',
                        'airport',
                        'amusement_park',
                        'intersection',
                        'premise',
                        'town_square']

# Initialize an empty list to hold the DataFrames
geo_list = []

for year in years:
    print(str(year))
    # Read the parquet file for the given year and rename columns
    geo = pd.read_parquet(f'{rais}/geocode/rais_geolocalizada_{year}.parquet')
    geo['year'] = year
    geo.rename(columns={'lon': 'lon_estab', 'lat': 'lat_estab'}, inplace=True)
    # Drop large columns that we don't need to save memory
    geo.drop(columns=['Match_addr','Match_type','h3_res8','h3_res9'], inplace=True)
    # Append the DataFrame to the list
    geo_list.append(geo)

# Concatenate all the DataFrames in the list into a single DataFrame
geo_estab = pd.concat(geo_list, ignore_index=True)
del geo, geo_list 


# Initialize lists to store the UTM coordinates
utm_lon = []
utm_lat = []

# Convert latitude and longitude to UTM with progress indicator
for lon, lat in tqdm(zip(geo_estab['lon_estab'].values, geo_estab['lat_estab'].values), total=len(geo_estab)):
    utm_x, utm_y = convert_to_utm(lon, lat)
    utm_lon.append(utm_x)
    utm_lat.append(utm_y)

# Assign the UTM coordinates back to the DataFrame
geo_estab['utm_lon_estab'] = utm_lon
geo_estab['utm_lat_estab'] = utm_lat

# Drop duplicates based on the specified columns in place
geo_estab.drop_duplicates(subset=['year', 'id_estab', 'lat_estab', 'lon_estab', 'Addr_type', 'h3_res7'], inplace=True)
# Optionally, reset the index in place
geo_estab.reset_index(drop=True, inplace=True)

# Group by 'year' and 'id_estab' and count the occurrences
duplicates = geo_estab.groupby(['year','id_estab']).size().reset_index(name='count')
print((duplicates.iloc[:,2].value_counts() /  duplicates.iloc[:,2].sum()).round(4))
geo_estab = geo_estab.merge(duplicates, on=['year', 'id_estab'], how='left')

geo_estab['is_high_precision'] = geo_estab['Addr_type'].isin(high_precision_cat).astype(int)

geo_estab[(geo_estab['count'] > 1) & (geo_estab['is_high_precision'] == 0)].shape[0] / (geo_estab['count'] > 1).sum()

geo_estab = geo_estab.sort_values('is_high_precision', ascending=False).drop_duplicates(subset=['year', 'id_estab'], keep='first')
geo_estab = geo_estab.drop_duplicates(subset=['year', 'id_estab'])



'''
  
    
  
    
  
    
  
    
  
def compute_skill_variance(appended, root):
    # Merge on Aguinaldo's O*NET factors
    appended['cbo2002'] = appended['cbo2002'].astype(int)
    appended = merge_aguinaldo_onet(appended, root + 'Data/raw/' )    
    
    skills_columns = [
        'Cognitive skills', 'Operational skills', 'Social and emotional skills', 'Management skills',
        'Physical skills', 'Transportation skills', 'Social sciences skills', 'Accuracy skills',
        'Design & engineering skills', 'Artistic skills', 'Life sciences skills',
        'Information technology skills', 'Sales skills', 'Self-reliance skills',
        'Information processing skills', 'Teamwork skills'
    ]
    # 1. Compute the variance of each of the 16 types of skills within each value of iota
    variances = appended.groupby('iota')[skills_columns].var()
    # 2. Compute the average of each of the 16 types of skills within each value of iota
    averages = appended.groupby('iota')[skills_columns].mean()
    # 3. Normalize these averages by summing all 16 averages and dividing each individual average by the overall sum
    normalized_averages = averages.div(averages.sum(axis=1), axis=0)
    # 4. Compute the weighted averages of the variances within each iota using the normalized averages as weights
    weighted_variances = (variances * normalized_averages).sum(axis=1)

    return weighted_variances


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



# Pull region codes
region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_micro_cw = pd.DataFrame({'code_micro':region_codes.code_micro,'codemun':region_codes.code_munic//10})

# Load iotas and gammas and compute the expected number of jobs for each iota
if load_iotas_gammas==True:
    iotas, gammas = load_iotas_gammas(root)
    E_N_gamma_given_iota, appended = process_iotas_gammas(root, iotas, gammas)
    E_N_gamma_given_iota.to_pickle(root + "Data/derived/mass_layoffs_E_N_gamma_given_iota.p", protocol=4)   
    E_N_gamma_given_iota.to_parquet(root + "Data/derived/mass_layoffs_E_N_gamma_given_iota.parquet")   
    # Compute skill and spatial variance of each iota
    weighted_variances = compute_skill_variance(appended, root)
    weighted_variances.to_pickle(root + "Data/derived/mass_layoffs_weighted_variances.p", protocol=4)  
    weighted_variances.to_parquet(root + "Data/derived/mass_layoffs_weighted_variances.parquet")  

 

# XX Need to compute spatial variance


    
##########################################################
# Pulling firm data to replicate Moretti and Yi
#
# - Identify establishment closures using data_encerramento in the establishment data. Also extract the year of the closure. Then create variables for the year before and the year after closure. We will use these to merge to worker employment dates to allow for some messiness of the timing of firm closure and endings of job spells, especially those that occurred at the beginning or end of a year.
    
def process_estab_data(rais):
    keepcols = ['id_estab','cnpj_raiz', 'clas_cnae20', 'data_abertura', 'data_baixa', 'data_encerramento', 'ind_atividade', 'ind_rais_neg', 'qt_vinc_ativos', 'qt_vinc_clt', 'qt_vinc_estat', 'regiao_metro', 'codemun', 'subativ_ibge', 'subs_ibge', 'tamestab', 'tipo_estab', 'uf']
    
    dfs = []
    for year in range(2013,2016+1):
        print(year)
        df = pd.read_csv(rais + f'/csv/estab{year}.csv', delimiter=';', nrows=nrows, usecols=keepcols, encoding='latin1')   
        df['data_abertura']         = pd.to_datetime(df['data_abertura'],       format='%d/%m/%Y', errors='raise')
        df['data_baixa']            = pd.to_datetime(df['data_baixa'],          format='%d/%m/%Y', errors='raise')
        df['data_encerramento']     = pd.to_datetime(df['data_encerramento'],   format='%d/%m/%Y', errors='raise')
        df['year'] = year
        df = df.merge(muni_micro_cw[['codemun','code_micro']], on='codemun', how='left', validate='m:1')
        dfs.append(df)
     
    estab_df = pd.concat(dfs)
    
    # Create a data set of establishment closures
    closure_df = estab_df.loc[estab_df.data_encerramento.notna(),['id_estab','cnpj_raiz','data_encerramento','qt_vinc_ativos']]
    
    # Drop establishments that close more than once. XX Can eventually do this over a longer time window b/c this data is small
    # Identify all rows that are duplicates in the 'id_estab' column
    duplicated_rows = closure_df[closure_df.duplicated(subset='id_estab', keep=False)]
    closure_df = closure_df.drop(duplicated_rows.index)
    
    # Convert estab end date to python date format
    closure_df['data_encerramento'] = pd.to_datetime(closure_df['data_encerramento'], dayfirst=True)
    closure_df['closure_year']      = closure_df['data_encerramento'].dt.year
    closure_df['closure_year_lag']  = closure_df['closure_year'] - 1
    closure_df['closure_year_lead'] = closure_df['closure_year'] + 1
    
    
    
    # Create a data set of market sizes
    mkt_size_df = estab_df[['clas_cnae20','code_micro','qt_vinc_ativos']]
    mkt_size_df['ind2'] = mkt_size_df['clas_cnae20'] // 1000
    mkt_size_df = mkt_size_df.groupby(['ind2','code_micro'])['qt_vinc_ativos'].sum()
    
    return closure_df, mkt_size_df


    
##########################################################
# Pulling worker data to replicate Moretti and Yi
#
#   - Pull all workers in the relevant states for the years 2013-2019. Keep wid, id_estab, cnpj_raiz, wid, jid, data_deslig, causa_deslig, clas_cnae20, cbo2002, codemun, idade, genero, grau_instr, salario, rem_med_sm, rem_med_r, vl_rem_01, ..., vl_rem_12. Then add a variable for year. 
#   - Merge to the firm layoff data above on id_estab and year (merging to year_t-1, year_t, year_t+1) and keep only workers possibly mass laid-off
#   - Then among this subset of workers create a monthly panel that uses data_adm and data_deslig to identify employment spells, also keep monthly earnings. Then we narrow down laid off workers to those who were employed within 3 months of the data_encerramento. 
#
# Variables we need:
#   - Measures of market size (CZ-industry and gamma and probability-weighted averages of gamma) 
#   - From the firm data we need to identify a list of firms that had a closure or mass layoff along with the date of the closure. Then we can merge this to worker data. 
#   - Worker panel that has employment and earnings and firm and market in each period. But we only need this for workers hit by mass layoff. So we can first identify workers hit by a mass layoff and then build the balanced panel on that subset. (Moretti and Yi only study workers who have been displaced.)
##########################################################
  
############################################################################
# Identify workers employed at closed estab in year it closed, or year before or after  

def identify_laid_off_workers(rais, date_formats, closure_df):

    dfs = []
    for year in range(2013,2016+1):
        print(year)
        if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
            sep = ';'
        else:
            sep = ','
        df = pd.read_csv(rais + f'/csv/brasil{year}.csv', delimiter=sep, usecols=['pis', 'id_estab','data_adm','data_deslig'], encoding='latin1', nrows=nrows)
        df['data_adm']      = pd.to_datetime(df['data_adm'],    format=date_formats[year], errors='raise')
        df['data_deslig']   = pd.to_datetime(df['data_deslig'], format=date_formats[year], errors='raise')
        df.rename(columns={'pis':'wid'}, inplace=True)
        df['year'] = year
        dfs.append(df)
        
    worker_estab_panel = pd.concat(dfs)
    
    worker_estab_panel = worker_estab_panel.merge(closure_df[['id_estab','closure_year']],      left_on=['id_estab','year'], right_on=['id_estab','closure_year'],      how='left', validate='m:1', indicator='_merge_year')
    
    # Create a flag for rows where any of the _merge variables are 'both'. Keep only the rows where the flag is True.
    worker_estab_panel['flag'] = (worker_estab_panel['_merge_year'] == 'both') 
    worker_estab_panel = worker_estab_panel[worker_estab_panel['flag']]
    
    # Drop the _merge variables and any columns from the right DataFrame
    columns_to_drop = ['_merge_year', 'closure_year', 'flag']
    worker_estab_panel = worker_estab_panel.drop(columns=columns_to_drop)
    possible_laid_off_worker_list = worker_estab_panel.wid.drop_duplicates()
    return possible_laid_off_worker_list
    

############################################################################
# Load data for workers at risk of layoff, make a balanced monthly panel based on data_adm, data_deslig, and monthly earnings. 
# Extend to 2019 sowe have at least 3 years of a post-period for all layoffs occurring in 2013-2016
 
def process_worker_data(rais, date_formats, possible_laid_off_worker_list, nrows=None):    
    # List of columns to keep
    # Initialize df to store worker bdays
    worker_dob = pd.DataFrame()
    columns_to_keep = [
        'pis', 'id_estab', 'cnpj_raiz', 'data_adm', 'data_deslig', 'causa_deslig',
        'clas_cnae20', 'cbo2002', 'codemun', 'data_nasc', 'idade', 'genero', 'grau_instr', 
        'salario', 'rem_med_sm', 'rem_med_r','nacionalidad', 'raca_cor', 'tipo_vinculo'
    ] + [f'vl_rem_{i:02d}' for i in range(1, 13)]
    
    # XX Can I make this more efficient by loading through 2019 in the loop above but then subsetting to 2013-2016 to come up with the possibly laid off worker data? Or actually should we be looking only at layoffs for 2014-2016 so we have a year of pre-period for everyone? 
    dfs = []
    dfs_mkt_size = []
    for year in range(2012,2019+1):
        print(year)
        if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
            sep = ';'
        else:
            sep = ','
        df = pd.read_csv(rais + f'/csv/brasil{year}.csv', delimiter=sep, usecols=columns_to_keep, encoding='latin1', nrows=nrows)
        
        # Need to make wid and jid
        df['wid'] = df.pis.astype(str)
        df['wid'] = df.pis.astype('Int64').astype(str)
        df['occ4'] = df['cbo2002'].astype(str).str[0:4]
        df['jid'] = df['id_estab'].astype(str).str.zfill(14) + '_' + df['occ4']

        # Save DOBs in a separate df
        year_dob = df[['wid', 'data_nasc']].drop_duplicates()
        year_dob['data_nasc']     = pd.to_datetime(year_dob['data_nasc'],   format=date_formats[year], errors='raise')
        worker_dob = pd.concat([worker_dob, year_dob])
        
        df = df[~df['tipo_vinculo'].isin([30,31,35])]
        df = df[df['codemun'].fillna(99).astype(str).str[:2].astype('int').isin([31, 33, 35])]
        
        ###################################################################################
        ## XXBM: it seems like we need gammas for the command below 
        iotas, gammas = load_iotas_gammas(root)
        ###################################################################################
        
        # Merge on iotas and gammas
        df = df.merge(gammas, how='left', validate='m:1', on='jid')
        # Merge on iotas
        df = df.merge(iotas,  how='left', validate='m:1', on='wid', indicator='_merge_iota')
        
        # Create a separate df for defining market size
        df['ind2']          = df['clas_cnae20'] // 1000
        df['year'] = year
        df = df.merge(muni_micro_cw[['codemun','code_micro']], on='codemun', how='left', validate='m:1')
        # Store a few variables that we can use for defining markets before subsetting to laid-off workers
        df_mkt_size = df[['year','wid','jid','iota','gamma','ind2','code_micro','codemun']]
        df = df[df['pis'].isin(possible_laid_off_worker_list)]
        # Date formats are not consistent across data sets so convert to dates here. 
        df['data_adm']      = pd.to_datetime(df['data_adm'],    format=date_formats[year], errors='raise')
        df['data_deslig']   = pd.to_datetime(df['data_deslig'], format=date_formats[year], errors='raise')
        df['idade']         = df['idade'].astype(int)
        # Drop job-years with average monthly earnings less than the minimum wage
        df = df[df['rem_med_sm'] >= 1]
        # Sometimes id_estab is defined but cnpj_raiz is not. Therefore, replace cnpj_raiz with the first 8 digits of id_estab. XX I can actually just not load cnpj_raiz to not mess with this. 
        df['id_firm'] = df['id_estab'].astype(str).str.zfill(14).str[:8].astype(int)
        dfs.append(df)
        dfs_mkt_size.append(df_mkt_size)
    
    # Group by worker ID and get the most common non-null date of birth
    worker_dob['yob']           = worker_dob.data_nasc.dt.year
    worker_dob = worker_dob[['wid','yob']]
    worker_dob = worker_dob.groupby('wid').agg({'yob': lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NaT})
    # Check for workers with multiple unique non-null dates of birth
    multiple_dob = worker_dob.groupby('wid').filter(lambda x: x['yob'].nunique() > 1)
    if not multiple_dob.empty:
        print(f"Warning: {len(multiple_dob)} workers have multiple unique dates of birth.")
        # You might want to handle these cases separately
    # Remove workers with no date of birth
    worker_dob = worker_dob.dropna()
        
    # XX Edit this to add returns in the function and move pickling outside
    worker_panel_mkt_size = pd.concat(dfs_mkt_size)
    mkt_size_df_worker = worker_panel_mkt_size.groupby(['ind2','code_micro']).size()
    mkt_size_df_worker = mkt_size_df_worker.reset_index(name='count')
    mkt_size_df_worker.to_pickle(root + "Data/derived/mass_layoffs_mkt_size_df_worker.p", protocol=4)
    mkt_size_df_worker.to_parquet(root + "Data/derived/mass_layoffs_mkt_size_df_worker.parquet")
    worker_panel = pd.concat(dfs)
    # data_nasc is missing for some years so process these separately
    # Restrict to people who are between 22 and 62 in 2012
    worker_panel = worker_panel.merge(worker_dob, on='wid', validate='m:1', how='left',indicator='_merge_yob')
    worker_panel = worker_panel.loc[worker_panel['yob'].between(1950, 1990)]
    worker_panel.to_pickle(root + "Data/derived/mass_layoffs_worker_panel.p", protocol=4)
    worker_panel.to_parquet(root + "Data/derived/mass_layoffs_worker_panel.parquet")
    del worker_panel_mkt_size

if pull_raw==True:
    closure_df, mkt_size_df = process_estab_data(rais)
    closure_df.to_pickle(root + "Data/derived/mass_layoffs_closure_df.p", protocol=4)
    closure_df.to_parquet(root + "Data/derived/mass_layoffs_closure_df.parquet")
    
    possible_laid_off_worker_list = identify_laid_off_workers(rais, date_formats, closure_df)
    possible_laid_off_worker_list.to_pickle(root + "Data/derived/mass_layoffs_possible_laid_off_worker_list.p", protocol=4)
    pd.DataFrame(possible_laid_off_worker_list).to_parquet(root + "Data/derived/mass_layoffs_possible_laid_off_worker_list.parquet")
    
    process_worker_data(rais, date_formats, possible_laid_off_worker_list)
    
closure_df          = pd.read_pickle(root + "Data/derived/mass_layoffs_closure_df.p")
worker_panel        = pd.read_pickle(root + "Data/derived/mass_layoffs_worker_panel.p")
E_N_gamma_given_iota= pd.read_pickle(root + "Data/derived/mass_layoffs_E_N_gamma_given_iota.p")
mkt_size_df_worker  = pd.read_pickle(root + "Data/derived/mass_layoffs_mkt_size_df_worker.p")


if create_worker_panel_balanced==True:
    
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
    
    worker_panel_long = worker_panel_long.merge(closure_df[['id_estab', 'data_encerramento']], on='id_estab', how='left', validate='m:1', indicator=True)
    worker_panel_long.drop(columns='_merge', inplace=True)
    
    # Create flag for worker being employed at estab in month in which mass layoff occurred. This is defined as the layoff firm being one's primary (highest earnings) firm in the month in which the firm closure occurred. 
    worker_panel_long['mass_layoff_flag'] = (worker_panel_long['calendar_date'].dt.year == worker_panel_long['data_encerramento'].dt.year) & (worker_panel_long['calendar_date'].dt.month == worker_panel_long['data_encerramento'].dt.month) & (worker_panel_long['employment_indicator']==1)
    
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
    
    unique_wids=worker_panel_long['wid'].unique()
    spine = pd.DataFrame({'wid':np.tile(unique_wids, 49), 'event_time':np.repeat(np.arange(-12,36+1),unique_wids.shape[0])})        
    worker_panel_balanced = spine.merge(worker_panel_long[['wid','event_time'] + variant], how='left', on=['wid','event_time'], indicator=True, validate='1:1')
    
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
    worker_panel_balanced['age']    = (worker_panel_balanced['calendar_date'].dt.to_period('Y') - pd.to_datetime(worker_panel_balanced['yob'].astype(int), format='%Y').dt.to_period('Y') ).apply(lambda x: x.n if pd.notna(x) else np.nan)
    worker_panel_balanced['age_sq'] = worker_panel_balanced['age']**2
    worker_panel_balanced['foreign']= (worker_panel_balanced['nacionalidad']!=10)
    worker_panel_balanced['raca_cor'] = pd.Categorical(worker_panel_balanced['raca_cor'])
    worker_panel_balanced['genero'] = pd.Categorical(worker_panel_balanced['genero'])
    
    
    
    ###################
    # Create pre- and post-layoff variables as well as indicators for changing jobs, occs, industries, etc
    
    # Get pre-layoff values
    pre_layoff = worker_panel_balanced[worker_panel_balanced['event_time'] == -1].set_index('wid')
    
    # Get first post-layoff values
    post_layoff = worker_panel_balanced[(worker_panel_balanced['event_time'] > 0) & 
                                        (worker_panel_balanced['employment_indicator'] == 1)].groupby('wid').first()
    # Loop over the specified variables
    variables = ['id_firm', 'cbo2002', 'clas_cnae20', 'ind2', 'code_micro', 'gamma']
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
    worker_panel_balanced_filtered = worker_panel_balanced[~worker_panel_balanced['wid'].isin(wids_to_drop)]
    
    
    
    
    
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
    
    # Merge on P_ig and E_N
    E_N_gamma_given_iota['market_size_tercile'] = pd.qcut(E_N_gamma_given_iota['E_N_gamma_given_iota'], q=3, labels=['low', 'medium', 'high'])
    # Drop markets with fewer than 10 workers
    mkt_size_df_worker = mkt_size_df_worker.loc[mkt_size_df_worker['count']>=10]
    mkt_size_df_worker['market_size_tercile_ind2_micro'] = pd.qcut(mkt_size_df_worker['count'], q=3, labels=['low', 'medium', 'high'])
    
    worker_panel_balanced = worker_panel_balanced.merge(E_N_gamma_given_iota, on='iota', how='left', validate='m:1')
    worker_panel_balanced = worker_panel_balanced.merge(mkt_size_df_worker.drop(columns='count'), left_on=['ind2','code_micro'], right_on=['ind2','code_micro'], how='left', validate='m:1')
    
    
    worker_panel_balanced.to_pickle(root + "Data/derived/mass_layoffs_worker_panel_balanced.p", protocol=4)
    worker_panel_balanced.to_parquet(root + "Data/derived/mass_layoffs_worker_panel_balanced.parquet")
    with open(root + "Data/derived/mass_layoffs_worker_panel_balanced_dtypes.p", 'wb') as f:
        pickle.dump(worker_panel_balanced.dtypes.to_dict(), f)
        



#worker_panel_balanced = pd.read_pickle(root + "Data/derived/mass_layoffs_worker_panel_balanced.p")
worker_panel_balanced = pd.read_parquet(root + "Data/derived/mass_layoffs_worker_panel_balanced.parquet")
with open(root + 'Data/derived/mass_layoffs_worker_panel_balanced_dtypes.p', 'rb') as f:
    worker_panel_balanced_dtypes = pickle.load(f)
worker_panel_balanced = worker_panel_balanced.astype(worker_panel_balanced_dtypes)
weighted_variances = pd.read_pickle(root + "Data/derived/mass_layoffs_weighted_variances.p")  




#######################################
# Preliminary figures


run_preliminary_figures=False
if run_preliminary_figures==True:
    
    # Collapse by event_time and take the mean of employment_indicator
    collapsed = worker_panel_balanced.groupby('event_time')[['employment_indicator','total_earnings_month','total_earnings_month_sm','vl_rem','vl_rem_sm','calendar_date']].mean().reset_index()
    
    # Create a time series plot of employment rates
    plt.figure(figsize=(10, 6))
    plt.plot(collapsed['event_time'], collapsed['employment_indicator'], marker='o', linestyle='-')
    plt.xlabel('Event Time (Months)')
    plt.ylabel('Mean Employment Indicator')
    plt.title('Mean Employment Indicator by Event Time')
    plt.grid(True)
    plt.savefig(root + 'Results/employment_after_layoff.pdf', format='pdf')
    plt.show()
    
    
    
    
    # Create a time series plot
    plt.figure(figsize=(10, 6))
    plt.plot(collapsed['event_time'], collapsed['total_earnings_month_sm'], marker='o', linestyle='-')
    plt.xlabel('Event Time (Months)')
    plt.ylabel('Mean Total Monthly Earnings')
    plt.title('Mean Total Monthly Earnings by Event Time')
    plt.grid(True)
    plt.savefig(root + 'Results/earnings_after_layoff.pdf', format='pdf')
    plt.show()
    
    
    # Create a time series plot
    plt.figure(figsize=(10, 6))
    plt.plot(collapsed['event_time'], collapsed['vl_rem_sm'], marker='o', linestyle='-')
    plt.xlabel('Event Time (Months)')
    plt.ylabel('Mean Primary Job Monthly Earnings')
    plt.title('Mean Primary Job Monthly Earnings by Event Time')
    plt.grid(True)
    plt.savefig(root + 'Results/primary_job_earnings_after_layoff.pdf', format='pdf')
    plt.show()
    
    
    # Check for changes in composition of dates. WHY DON'T WE SEE ROUGHLY SMOOTH CALENDAR DATES W.R.T. EVENT TIME. WHY DO WE SEE A SLOPE OF 1??
    # Create a time series plot
    plt.figure(figsize=(10, 6))
    plt.plot(collapsed['event_time'], collapsed['calendar_date'], marker='o', linestyle='-')
    plt.xlabel('Event Time (Months)')
    plt.ylabel('Mean Calendar Date')
    plt.title('Mean Calendar Date by Event Time')
    plt.grid(True)
    plt.show()
    
    
    
    # Histogram of calendar_date for event_time = 0
    plt.subplot(1, 2, 1)
    plt.hist(worker_panel_balanced.loc[worker_panel_balanced.event_time==0,'calendar_date'], bins=30, alpha=0.7, color='blue')
    plt.title('Histogram of calendar_date for event_time = 0')
    plt.xlabel('Calendar Date')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.grid(True)
        
        
    
    # Define the range of years
    years = range(2013, 2017)
    
    # Create a plot
    plt.figure(figsize=(12, 8))
    
    for year in years:
        # Filter data for the current year
        year_data = worker_panel_balanced[worker_panel_balanced['mass_layoff_month'].dt.year == year]
        
        # Replace NAs with 0 for earnings and employment
        year_data['employment_indicator'].fillna(0, inplace=True)
        year_data['total_earnings_month'].fillna(0, inplace=True)
        year_data['total_earnings_month_sm'].fillna(0, inplace=True)
    
    
        # Collapse by event_time and take the mean of employment_indicator
        collapsed = year_data.groupby('event_time')[['employment_indicator', 'total_earnings_month_sm']].mean().reset_index()
        
        # Plot the employment rates
        plt.plot(collapsed['event_time'], collapsed['total_earnings_month_sm'], marker='o', linestyle='-', label=f'Year {year}')
    
    # Customize the plot
    plt.xlabel('Event Time (Months)')
    plt.ylabel('Mean Employment Indicator')
    plt.title('Mean Employment Indicator by Event Time for Each Year')
    plt.legend(title='Year')
    plt.grid(True)
    plt.show()
    
    
    	
    
    ###################################################################################
    # Moretti and Yi equation 3
    #
    # Controls: age, age^2, gender, race, foreign-born. 
    # Vector of education-specific indicators for the CZ of residence at t=-1
    # Vector of education-specific indicators for the industry of employment at t=-1. 2-digit.
    # Vector of education-time dummies defined by the quarter-year of closure. 
    # SEs clustered at CZ-level
    
    
    
    
    # Create dummy variables for event_time
    event_time_dummies = pd.get_dummies(worker_panel_balanced['event_time'], prefix='event_time')
    event_time_dummies.drop(columns='event_time_0', inplace=True)
    
    ####################################################################
    # Employment
    
    # Define the dependent variable and the independent variables
    y = worker_panel_balanced['employment_indicator']
    X = pd.concat([event_time_dummies, worker_panel_balanced[['age','age_sq']]], axis=1)  # Include the event_time dummies
    
    # Define the fixed effects to absorb
    fixed_effects =worker_panel_balanced[['educ_micro','educ_ind2','educ_layoff_date','foreign','genero','raca_cor']]
    
    print(fixed_effects.shape)
    print(y.shape)
    print(X.shape)
    
    # There are a few missing values of X
    missing_indices = X.isna().any(axis=1)
    y = y.loc[~missing_indices]
    X = X.loc[~missing_indices]
    fixed_effects = fixed_effects.loc[~missing_indices]
    
    # Run the model with AbsorbingLS
    model_absorbed = AbsorbingLS(y, X, absorb=fixed_effects, drop_absorbed=True)
    #results_absorbed = model_absorbed.fit(cov_type='clustered', clusters=df.index.get_level_values('wid'))  # Clustered standard errors by entity
    results_absorbed = model_absorbed.fit(cov_type='unadjusted') 
    print(results_absorbed)
    
    
    # Extract coefficients and standard errors for event_time dummies
    coefficients = results_absorbed.params.filter(like='event_time')
    conf_int = results_absorbed.conf_int().filter(like='event_time', axis=0)
    conf_int.columns = ['lower', 'upper']
    standard_errors = results_absorbed.std_errors.filter(like='event_time')
    
    # Set pandas display options for 4 decimal points
    pd.options.display.float_format = '{:.4f}'.format
    
    # Create a DataFrame for plotting
    coefficients_df = pd.DataFrame({'coef': coefficients, 'std_err': standard_errors})
    coefficients_df.index = coefficients_df.index.str.replace('event_time_', '').astype(int)
    coefficients_df = coefficients_df.sort_index()
    
    
    # Plot the coefficients
    plt.figure(figsize=(10, 6))
    plt.errorbar(coefficients_df.index, coefficients_df['coef'], yerr=coefficients_df['std_err'], fmt='o', linestyle='-', capsize=5)
    plt.axhline(0, color='black', linewidth=1, linestyle='--')
    plt.xlabel('Event Time')
    plt.ylabel('Coefficient')
    plt.title('Employment')
    plt.grid(True)
    plt.show()
    
    
    ####################################################################
    # Earnings
    
    
    # Define the dependent variable and the independent variables
    y = worker_panel_balanced['total_earnings_month_sm']
    X = pd.concat([event_time_dummies, worker_panel_balanced[['age','age_sq']]], axis=1)  # Include the event_time dummies
    
    # Define the fixed effects to absorb
    fixed_effects =worker_panel_balanced[['educ_micro','educ_ind2','educ_layoff_date','foreign','genero','raca_cor']]
    
    
    # There are a few missing values of X
    missing_indices = X.isna().any(axis=1)
    y = y.loc[~missing_indices]
    X = X.loc[~missing_indices]
    fixed_effects = fixed_effects.loc[~missing_indices]
    
    # Run the model with AbsorbingLS
    model_absorbed = AbsorbingLS(y, X, absorb=fixed_effects, drop_absorbed=True)
    #results_absorbed = model_absorbed.fit(cov_type='clustered', clusters=df.index.get_level_values('wid'))  # Clustered standard errors by entity
    results_absorbed = model_absorbed.fit(cov_type='unadjusted') 
    print(results_absorbed)
    
    
    # Extract coefficients and standard errors for event_time dummies
    coefficients = results_absorbed.params.filter(like='event_time')
    conf_int = results_absorbed.conf_int().filter(like='event_time', axis=0)
    conf_int.columns = ['lower', 'upper']
    standard_errors = results_absorbed.std_errors.filter(like='event_time')
    
    # Set pandas display options for 4 decimal points
    pd.options.display.float_format = '{:.4f}'.format
    
    # Create a DataFrame for plotting
    coefficients_df = pd.DataFrame({'coef': coefficients, 'std_err': standard_errors})
    coefficients_df.index = coefficients_df.index.str.replace('event_time_', '').astype(int)
    coefficients_df = coefficients_df.sort_index()
    
    
    # Plot the coefficients
    plt.figure(figsize=(10, 6))
    plt.errorbar(coefficients_df.index, coefficients_df['coef'], yerr=coefficients_df['std_err'], fmt='o', linestyle='-', capsize=5)
    plt.axhline(0, color='black', linewidth=1, linestyle='--')
    plt.xlabel('Event Time')
    plt.ylabel('Coefficient')
    plt.title('Earnings')
    plt.grid(True)
    plt.show()
    



def event_studies_by_mkt_size(worker_panel_balanced, y_var, continuous_controls, fixed_effects_cols, 
                              market_size_var, omitted_category, baseline_category = 'low', print_regression=False, savefig=None, title=None):
    """
    Perform event studies by market size.
    
    Parameters:
    - worker_panel_balanced: DataFrame containing the worker panel data
    - y_var: str, name of the dependent variable column
    - continuous_controls: list of str, names of continuous control variable columns
    - fixed_effects_cols: list of str, names of columns to use as fixed effects
    - market_size_var: str, name of the market size variable column
    - omitted_category: str, category to be omitted from the market size dummies
    - E_N_gamma_given_iota: DataFrame, optional. If provided, will be merged with worker_panel_balanced
    
    Returns:
    - results_absorbed: regression results
    - coefficients_df_low: DataFrame of coefficients for low market size
    - coefficients_df_high: DataFrame of coefficients for high market size
    """
    
    # Exclude rows where event_time = 0
    worker_panel_filtered = worker_panel_balanced[worker_panel_balanced['event_time'] != 0].copy()
    
    # Create dummy variables for market size and event_time
    market_size_dummies = pd.get_dummies(worker_panel_filtered[market_size_var], prefix='market_size', drop_first=False)
    market_size_dummies = market_size_dummies.drop(columns=[f'market_size_{omitted_category}'])
    event_time_dummies = pd.get_dummies(worker_panel_filtered['event_time'], prefix='event_time')
    
    # Get the names of the non-omitted categories
    non_omitted_categories = [col.replace('market_size_', '') for col in market_size_dummies.columns]
    
    # Interact event_time dummies with market size dummies
    interaction_terms = {category: event_time_dummies.multiply(market_size_dummies[f'market_size_{category}'], axis=0)
                         for category in non_omitted_categories}
    
    # Rename the interaction columns for clarity
    for category in non_omitted_categories:
        interaction_terms[category] = interaction_terms[category].add_prefix(f'{category}_')
    
    # Combine all independent variables
    X = pd.concat([*interaction_terms.values(), worker_panel_filtered[continuous_controls]], axis=1)
    
    # Add a constant
    X = sm.add_constant(X)
    
    # Define the dependent variable
    y = worker_panel_filtered[y_var]
    
    # Define the fixed effects to absorb
    fixed_effects = worker_panel_filtered[fixed_effects_cols]
    
    # Handle missing values
    missing_indices = X.isna().any(axis=1)
    y = y.loc[~missing_indices]
    X = X.loc[~missing_indices]
    fixed_effects = fixed_effects.loc[~missing_indices]
    
    # Run the model with AbsorbingLS
    model_absorbed = AbsorbingLS(y, X, absorb=fixed_effects, drop_absorbed=True)
    results_absorbed = model_absorbed.fit(cov_type='unadjusted')
    if print_regression:
        print(results_absorbed)
    
    # Extract coefficients and standard errors for event_time dummies
    coefficients = {category: results_absorbed.params.filter(like=f'{category}_event_time')
                    for category in non_omitted_categories}
    standard_errors = {category: results_absorbed.std_errors.filter(like=f'{category}_event_time')
                       for category in non_omitted_categories}
    
    # Create DataFrames for plotting
    coefficients_df = {category: pd.DataFrame({'coef': coefficients[category], 
                                               'std_err': standard_errors[category]})
                       for category in non_omitted_categories}
    
    # Normalize coefficients
    baseline_coef = coefficients_df[baseline_category].loc[f'{baseline_category}_event_time_-1', 'coef']
    
    for category in non_omitted_categories:
        coefficients_df[category].index = coefficients_df[category].index.str.replace(f'{category}_event_time_', '').astype(int)
        coefficients_df[category] = coefficients_df[category].sort_index()
        # Normalize coefficients
        coefficients_df[category]['coef'] -= baseline_coef
    
    # Plot the coefficients
    plt.figure(figsize=(10, 6))
    for category in non_omitted_categories:
        plt.errorbar(coefficients_df[category].index, coefficients_df[category]['coef'], 
                     yerr=coefficients_df[category]['std_err'], fmt='o', linestyle='-', 
                     capsize=5, label=f'{category.capitalize()}')
    
    plt.axhline(0, color='black', linewidth=1, linestyle='--')
    plt.xlabel('Months Since Firm Closure')
    plt.ylabel('Coefficient')
    if title is None:
        plt.title(f'{y_var} by {market_size_var}')
    else:
        plt.title(title)
    plt.grid(True)
    plt.legend()
    if savefig is not None:
        plt.savefig(savefig, format='pdf')
    else:
        plt.savefig(f'{root}Results/{y_var}_after_layoff_by_{market_size_var}.pdf', format='pdf')
    plt.show()
    
    return results_absorbed, coefficients_df


# Make figure for employment:
results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced,
    y_var='employment_indicator',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium'
)

results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced.loc[worker_panel_balanced.tenure_years>=.5],
    y_var='employment_indicator',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium',
    savefig=f'{root}Results/employment_indicator_after_layoff_by_market_size_tercile_tenure6.pdf'
)

results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced.loc[worker_panel_balanced.tenure_years>=1],
    y_var='employment_indicator',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium',
    savefig=f'{root}Results/employment_indicator_after_layoff_by_market_size_tercile_tenure12.pdf'
)



# Make figure for total earnings:
results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced,
    y_var='total_earnings_month_sm',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium'
)

results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced.loc[worker_panel_balanced.tenure_years>=.5],
    y_var='total_earnings_month_sm',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium',
    savefig=f'{root}Results/total_earnings_month_sm_after_layoff_by_market_size_tercile_tenure6.pdf'
)

results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced.loc[worker_panel_balanced.tenure_years>=1],
    y_var='total_earnings_month_sm',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium',
    savefig=f'{root}Results/total_earnings_month_sm_after_layoff_by_market_size_tercile_tenure12.pdf'
)


# Make figure for employment (MY mkt size def'n):
results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced,
    y_var='employment_indicator',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile_ind2_micro',
    omitted_category='medium'
)

# Make figure for earnings at primary job, restricting to those with earnings between 1 and 100x the minimum wage
worker_panel_balanced['earnings_primary_job'] = worker_panel_balanced['vl_rem_sm']
#mask = worker_panel_balanced['total_earnings_month_sm'].between(1, 100, inclusive='both')
mask = worker_panel_balanced['total_earnings_month_sm']<20

results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced[mask],
    y_var='earnings_primary_job',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium'
)



# Make figures for staying in the same market, ind, occ, micro, etc:
vars = ['same_cbo2002', 'same_clas_cnae20', 'same_ind2', 'same_code_micro', 'same_gamma']
for v in vars:
    print(v)
    results, coef_df = event_studies_by_mkt_size(
        worker_panel_balanced.loc[( worker_panel_balanced.tenure_years>=.5)],
        y_var=v,
        continuous_controls=['age', 'age_sq'],
        fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
        market_size_var='market_size_tercile',
        omitted_category='medium'
    )

# Restricted to college-educated workers only
results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced.loc[( worker_panel_balanced.tenure_years>=.5) & (worker_panel_balanced['educ']=='College')],
    y_var='employment_indicator',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium',
    title='employment_indicator by market size tercile (College only)',
    savefig=root + 'Results/employment_indicator_after_layoff_by_market_size_tercile_College.pdf'
)

results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced.loc[( worker_panel_balanced.tenure_years>=.5) & (worker_panel_balanced['educ']=='College')],
    y_var='earnings_primary_job',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium',
    title='earnings_primary_job by market size tercile (College only)',
    savefig=root + 'Results/earnings_primary_job_after_layoff_by_market_size_tercile_College.pdf'
)

# Restricted to high school-educated workers only
results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced.loc[( worker_panel_balanced.tenure_years>=.5) & (worker_panel_balanced['educ']=='HS')],
    y_var='employment_indicator',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium',
    title='employment_indicator by market size tercile (HS only)',
    savefig=root + 'Results/employment_indicator_after_layoff_by_market_size_tercile_HS.pdf'
)

results, coef_df = event_studies_by_mkt_size(
    worker_panel_balanced.loc[(worker_panel_balanced.tenure_years>=.5) & (worker_panel_balanced['educ']=='HS')],
    y_var='earnings_primary_job',
    continuous_controls=['age', 'age_sq'],
    fixed_effects_cols=['educ_micro', 'educ_ind2', 'educ_layoff_date', 'foreign', 'genero', 'raca_cor'],
    market_size_var='market_size_tercile',
    omitted_category='medium',
    title='earnings_primary_job by market size tercile (HS only)',
    savefig=root + 'Results/earnings_primary_job_after_layoff_by_market_size_tercile_HS.pdf'
)




###########################################################################
# Summary stats for the weighted skill variances

# Plotting the histogram of the weighted variances
plt.figure(figsize=(10, 6))
plt.hist(weighted_variances, bins=30, edgecolor='k', alpha=0.7)
plt.title('Histogram of Weighted Variances')
plt.xlabel('Weighted Variances')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()

# Note that occ_counts starts iota indexing at 1 so we will need to adjust this
occ_counts = pd.read_csv(root + 'Data/derived/occ_counts/panel_3states_2013to2016_new_occ_counts_by_i_level_0.csv')
min_iota = appended.iota.min()
occ_counts = occ_counts.loc[occ_counts.iota!=-1]   # Drop missing iota codes
occ_counts['iota'] = occ_counts.iota + (min_iota - occ_counts.iota.min())

# Step 1: Identify the iota values with the 5 lowest and 5 highest weighted variances
lowest_iotas = weighted_variances.nsmallest(5).index
highest_iotas = weighted_variances.nlargest(5).index
selected_iotas = lowest_iotas.union(highest_iotas)

# Step 2: Filter the occ_counts DataFrame to include only these iota values
filtered_occ_counts = occ_counts[occ_counts['iota'].isin(selected_iotas)]

# Step 3: Get the top 10 most common occupations for each iota
top_occupations = filtered_occ_counts.groupby('iota').apply(
    lambda x: x.nlargest(10, 'counts')[['description', 'counts']]
).reset_index(level=0, drop=True)

# Step 4: Print the results
for iota in selected_iotas:
    print(f"\nIota: {iota}")
    print(top_occupations[top_occupations.index == iota][['description', 'counts']])

# Step 1: Identify the iota values with the 5 lowest and 5 highest weighted variances
lowest_iotas = weighted_variances.nsmallest(5).index
highest_iotas = weighted_variances.nlargest(5).index
selected_iotas = lowest_iotas.union(highest_iotas)

# Step 2: Filter the occ_counts DataFrame to include only these iota values
filtered_occ_counts = occ_counts[occ_counts['iota'].isin(selected_iotas)]

# Step 3: Print the top 10 most common occupations for each iota
for iota in highest_iotas:
    top_occupations = filtered_occ_counts[filtered_occ_counts['iota'] == iota].nlargest(10, 'counts')
    print(f"\nIota: {iota}")
    print(top_occupations[['description', 'counts']])
    

# Step 3: Print the top 10 most common occupations for each iota
for iota in lowest_iotas:
    top_occupations = filtered_occ_counts[filtered_occ_counts['iota'] == iota].nlargest(10, 'counts')
    print(f"\nIota: {iota}")
    print(top_occupations[['description', 'counts']])
      




# Characterize the distribution of calendar_date
def characterize_distribution(df, date_col):
    print(f"Characterizing distribution for {date_col}:")
    print(f"Minimum date: {df[date_col].min()}")
    print(f"Maximum date: {df[date_col].max()}")
    print(f"Number of unique dates: {df[date_col].nunique()}")
    
    # Plot the distribution
    df[date_col].value_counts().sort_index().plot(kind='bar', figsize=(12, 6))
    plt.xlabel('Date')
    plt.ylabel('Frequency')
    plt.title(f'Distribution of {date_col}')
    plt.show()

# Characterize the distribution of calendar_date
characterize_distribution(worker_panel_long, 'calendar_date')

# Filter the DataFrame where vl_rem > 0
filtered_df = worker_panel_long[worker_panel_long['vl_rem'] > 0]

# Characterize the distribution of calendar_date where vl_rem > 0
characterize_distribution(filtered_df, 'calendar_date')






##########################################################################################
##########################################################################################
# Old code I'm not currently using but want to keep as a reference
##########################################################################################
##########################################################################################


# Old code for exloring the data
if 1==0:
    
    '''
    keepcols = ['id_estab','cnpj_raiz','data_abertura', 'data_baixa', 'data_encerramento', 'ind_atividade', 'ind_rais_neg', 'qt_vinc_ativos', 'qt_vinc_clt', 'qt_vinc_estat', 'regiao_metro',  'subativ_ibge', 'subs_ibge', 'tamestab', 'tipo_estab', 'uf']
    
    # Pull establishment data to flag mass layoffs
    nrows = None
    df2013 = pd.read_csv(rais + '/csv/estab2013.csv', delimiter=';', nrows=nrows, usecols=keepcols, encoding='latin1')
    df2014 = pd.read_csv(rais + '/csv/estab2014.csv', delimiter=';', nrows=nrows, usecols=keepcols, encoding='latin1')
    df2015 = pd.read_csv(rais + '/csv/estab2015.csv', delimiter=';', nrows=nrows, usecols=keepcols, encoding='latin1')
    df2016 = pd.read_csv(rais + '/csv/estab2016.csv', delimiter=';', nrows=nrows, usecols=keepcols, encoding='latin1')
    
    df2013['year'] = 2013
    df2014['year'] = 2014
    df2015['year'] = 2015
    df2016['year'] = 2016
    
    
    # 58% of establishments with 0 active employees on Dec 31
    # tamestab seems to be categorical indicator for establishment size. 
    (df2013.qt_vinc_ativos==0).mean()
    (df2013.tamestab==0).mean()
    (df2013.tamestab==df2013.qt_vinc_ativos).mean()
    df2013[['tamestab','qt_vinc_ativos']].corr()
    df2013[['tamestab','qt_vinc_ativos']]
    df2013.loc[df2013.tamestab==1].qt_vinc_ativos.value_counts().sort_index() #1-4
    df2013.loc[df2013.tamestab==2].qt_vinc_ativos.value_counts().sort_index() #5-9
    df2013.loc[df2013.tamestab==3].qt_vinc_ativos.value_counts().sort_index() #10-19
    df2013.loc[df2013.tamestab==4].qt_vinc_ativos.value_counts().sort_index() #20-49
    df2013.loc[df2013.tamestab==8].qt_vinc_ativos.value_counts().sort_index() #500-999
    df2013.loc[df2013.tamestab==9].qt_vinc_ativos.value_counts().sort_index() #>=1000
    
    estab_df = pd.concat([df2013, df2014, df2015, df2016], ignore_index=True)
    
    #####################
    # Diagnostics
    
    # 10% of firms have 0 employees on December 31
    # (a.emp_31dez==0).mean()
    #Out[13]: 0.10501055345281181
    
    # There are some id_estabs that are duplicated and have different counts but the counts sum to the value of emp_31dez in the corresponding firm data. THerefore I am collapsing as below. For example:
    estab_df.loc[estab_df.id_estab==16058000123]
    
    # Group by id_estab and year, summing specific columns and taking the first value for others
    estab_df = estab_df.groupby(
        ['id_estab', 'year'],
        as_index=False).agg({
        'tipo_estab': 'first',
        'cnpj_raiz': 'first',
        'uf': 'first',
        'regiao_metro': 'first',
        'subs_ibge': 'first',
        'subativ_ibge': 'first',
        'data_abertura': 'first',
        'data_baixa': 'first',
        'data_encerramento': 'first',
        'ind_atividade': 'first',
        'ind_rais_neg': 'first',
        'qt_vinc_ativos': 'sum',
        'qt_vinc_clt': 'sum',
        'qt_vinc_estat': 'sum'
    })
    
    
    #########################
    # Check if counts from firm and worker data align
    
    
    merged = pd.merge(estab_df[['id_estab','year','qt_vinc_ativos']], estab_df_from_worker_data[['id_estab','year','emp_31dez']], on=['id_estab','year'], how = 'outer', validate='1:1', indicator=True)
    pd.crosstab(merged['_merge'], merged['year'])
    
    # Almost all non-merged observations are those with 0 employment
    merged.groupby('_merge').qt_vinc_ativos.describe()
    (merged.loc[merged._merge=='left_only'].qt_vinc_ativos>0).sum()
    
    # There is still some zero employment in the worker DF. I'm guessing these are for employment spells in small firms that ended before the end of the year
    (estab_df_from_worker_data.emp_31dez==0).mean()
    
    # The employment counts from the worker and firm data agree in 99.8% of observations with a successful match
    (merged.loc[merged._merge=='both'].qt_vinc_ativos==merged.loc[merged._merge=='both'].emp_31dez).mean()
    
    # Most of the other 0.2% have emp_31dez = 0
    merged.loc[(merged._merge=='both') & (merged.qt_vinc_ativos!=merged.emp_31dez)].emp_31dez.value_counts()
                  
     
    # XX Next step merge to dfYYYY to understand why so many rows have 0 employees
    
    
    df2013 = df2013.merge(id_estab_counts[0], on='id_estab', how='outer',validate='m:1', indicator=True)
    (df2013.loc[df2013._merge=='left_only'].qt_vinc_ativos==0).mean()   # Every firm that employs someone on Dec 31 in the employee data also shows up in the firm data
    (df2013.loc[df2013._merge=='both'].qt_vinc_ativos==0).mean()        # 10% of firms that show up in employee data don't have an employee on Dec 31
    
                    
    
    #########################
    # Make a balanced panel
    
    # Identify the range of years and unique cnpj_raiz
    years = range(firm_df['year'].min(), firm_df['year'].max() + 1)
    cnpj_raiz_unique = firm_df['cnpj_raiz'].unique()
    # Create a multi-index dataframe using all combinations of cnpj_raiz and years
    index = pd.MultiIndex.from_product([cnpj_raiz_unique, years], names=['cnpj_raiz', 'year'])
    balanced_panel = pd.DataFrame(index=index).reset_index()
    # Merge the original dataframe with the multi-index dataframe
    firm_df = pd.merge(balanced_panel, firm_df, on=['cnpj_raiz', 'year'], how='left')
    # Fill missing emp_31dez values with 0
    firm_df['emp_31dez'] = firm_df['emp_31dez'].fillna(0)
    
    
    #########################
    # Different definitions of mass layoffs
    
    # Assuming firm_df is your dataframe
    firm_df = firm_df.sort_values(by=['cnpj_raiz', 'year'])
    # Compute year-over-year changes
    firm_df['abs_change'] = firm_df.groupby('cnpj_raiz')['emp_31dez'].diff()
    firm_df['pct_change'] = firm_df.groupby('cnpj_raiz')['emp_31dez'].pct_change()
    
    # Replace NaNs resulting from the diff and pct_change methods with appropriate values
    firm_df['abs_change'] = firm_df['abs_change'].fillna(0)
    firm_df['pct_change'] = firm_df['pct_change'].fillna(0)
    
    # Display the resulting dataframe
    firm_df
    
    
    
    '''

    
    


# Absorbed FEs example to understand the difference between PanelOLS and AbsorbingLS
# The coefs align between the two methods but the standard errors, F-stats, etc. do not
if 1==0:

   '''
   import pandas as pd
   import numpy as np
   from linearmodels.iv import AbsorbingLS
   from linearmodels.panel import PanelOLS
   import statsmodels.api as sm
   # Example DataFrame setup with 20 rows
   data = {
       'wid': [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5],
       'year': [2010, 2011, 2012, 2013, 2010, 2011, 2012, 2013, 2010, 2011, 2012, 2013, 2010, 2011, 2012, 2013, 2010, 2011, 2012, 2013],
       'event_time': [-1, 0, 1, 2, -1, 0, 1, 2, -1, 0, 1, 2, -1, 0, 1, 2, -1, 0, 1, 2],
       'educ': ['College', 'College', 'College', 'College', 'HS', 'HS', 'HS', 'HS', 'Some college', 'Some college', 'Some college', 'Some college', 'College', 'College', 'College', 'College', 'HS', 'HS', 'HS', 'HS'],
       'CZ': ['A', 'C', 'C', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'B', 'C', 'C', 'A', 'A', 'A', 'B', 'B', 'B', 'B'],
       'industry': ['Manufacturing', 'Manufacturing', 'Services', 'Services', 'Retail', 'Retail', 'Manufacturing', 'Manufacturing', 'Services', 'Services', 'Retail', 'Retail', 'Manufacturing', 'Manufacturing', 'Services', 'Services', 'Retail', 'Retail', 'Manufacturing', 'Manufacturing']
   }

   df = pd.DataFrame(data)

   # Use RNG to generate the income variable
   np.random.seed(0)  # Set seed for reproducibility
   df['income'] = np.random.normal(loc=50000, scale=10000, size=df.shape[0])

   # Convert to a MultiIndex DataFrame suitable for panel data analysis
   df = df.set_index(['wid', 'year'])

   # Convert educ, CZ, and industry to categorical
   df['educ'] = df['educ'].astype('category')
   df['CZ'] = df['CZ'].astype('category')
   df['industry'] = df['industry'].astype('category')

   # Create dummy variables for event_time
   event_time_dummies = pd.get_dummies(df['event_time'], prefix='event_time', drop_first=True)

   # Define the dependent variable and the independent variables
   y = df['income']
   X = event_time_dummies  # Include the event_time dummies

   # Define the fixed effects to absorb
   fixed_effects = df[['educ', 'CZ', 'industry']]

   # Run the model with AbsorbingLS
   model_absorbed = AbsorbingLS(y, X, absorb=fixed_effects)
   #results_absorbed = model_absorbed.fit(cov_type='clustered', clusters=df.index.get_level_values('wid'))  # Clustered standard errors by entity
   results_absorbed = model_absorbed.fit(cov_type='unadjusted') 


   # Print the results from AbsorbingLS
   print("Results from AbsorbingLS:")
   print(results_absorbed)

   # Create dummy variables for the fixed effects
   fe_educ = pd.get_dummies(df['educ'], prefix='educ', drop_first=True)
   fe_CZ = pd.get_dummies(df['CZ'], prefix='CZ', drop_first=True)
   fe_industry = pd.get_dummies(df['industry'], prefix='industry', drop_first=True)

   # Combine X and fixed effects dummies
   X_dummies = pd.concat([X, fe_educ, fe_CZ, fe_industry], axis=1)
   X_dummies = sm.add_constant(X_dummies)

   # Run the model with PanelOLS excluding entity effects
   model_dummies = PanelOLS(y, X_dummies, entity_effects=False)
   #results_dummies = model_dummies.fit(cov_type='clustered', cluster_entity=True)  # Clustered standard errors
   results_dummies = model_dummies.fit(cov_type='unadjusted')  # Clustered standard errors

   # Print the results from PanelOLS with dummies
   print("\nResults from PanelOLS with dummies:")
   print(results_dummies)
   '''

   
