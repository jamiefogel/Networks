# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 09:30:57 2024

@author: p13861161
"""

import pandas as pd
import numpy as np
import os
import platform
import sys
import getpass
import matplotlib.pyplot as plt
from tqdm import tqdm # to calculate progress of some operations for geolocation
import pyproj # for converting geographic degree measures of lat and lon to the UTM system (i.e. in meters)
#from scipy.integrate import simps # to calculate the AUC for the decay function
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc
import geopandas as gpd



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


if getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'
    sys.path.append(root + 'Code/Modules')

from mass_layoffs_parquet_functions import load_iotas_gammas, process_iotas_gammas, pull_estab_geos, event_studies_by_mkt_size, calculate_distance, compute_skill_variance, compute_mkt_sizes_ind2_micro, merge_aguinaldo_onet

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




years = range(2013, 2016 + 1)

# Pull region codes
region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_meso_micro_cw = pd.DataFrame({'code_meso':region_codes.code_meso, 'code_micro':region_codes.code_micro,'codemun':region_codes.code_munic//10})


state_codes = [31, 33, 35]
rio_codes = [330045, 330170, 330185, 330190, 330200, 330227, 330250, 330285, 330320, 330330, 330350, 330360, 330414, 330455, 330490, 330510, 330555, 330575]
region_codes = pd.read_csv(root + 'Data/raw/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})


#maxrows = 10000

iotas, gammas = load_iotas_gammas(root)
  

mkt_size_df = compute_mkt_sizes_ind2_micro(rais, muni_meso_micro_cw)
mkt_size_df = mkt_size_df.reset_index(name='count')


run_pull=True
if run_pull==True:
    dfs = []
    for year in years:
        print(year)
        file_path = f"{rais}/parquet_novos/brasil{year}.parquet"
        # Read the Parquet file
        table = pq.read_table(file_path, columns=['pis','id_estab','cnpj_raiz','cbo2002','clas_cnae20','rem_med_sm','codemun','tipo_vinculo','data_adm'])
        
    
        # Keep only tipo_vinculo 30, 31, 35
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
        # Combine all masks
        final_mask = pc.and_(mask_tipo_vinculo, pc.and_(mask_codemun, mask_rem_med_sm))


        filtered_table = table.filter(final_mask)
        # Convert PyArrow Table to pandas DataFrame
        df = filtered_table.to_pandas()
        
        # Add relevant variables
        df['year'] = year
        df['wid'] = df.pis.astype('Int64').astype(str)
        df['occ4'] = df['cbo2002'].astype(str).str[0:4]
        df['id_estab'] = df['id_estab'].astype(str).str.zfill(14)
        df['jid'] = df['id_estab'] + '_' + df['occ4']
        df['start_date'] = pd.to_datetime(df['data_adm'], format=date_formats[year], errors='raise')
                          
        # Merge on iotas and gammas
        df = df.merge(gammas, how='left', validate='m:1', on='jid', indicator='_merge_gamma')
        df = df.merge(iotas,  how='left', validate='m:1', on='wid', indicator='_merge_iota')
        
        # Create a separate df for defining market size
        df['ind2']          = df['clas_cnae20'] // 1000
        df = df.merge(muni_meso_micro_cw, on='codemun', how='left', validate='m:1')
        df.loc[df.cbo2002.isna(), 'cbo2002'] = 0  # Set missing values to 0 as a flag
        df['occ2Xmeso'] = df.cbo2002.astype(int).astype(str).str[0:2] + '_' + df['code_meso'].astype('str')
        
        # XX Could merge on coordinates here and then compute distances below, once we have pre-layoff and post-layoff id_estab
        ###########################
        # GEOLOCATION FOR RAIS ESTABLISHMENTS
        years = list(range(firstyear_panel,lastyear_panel+1))

        geo_estab = pull_estab_geos([year], rais)
        

        # Merge establishment geolocation to worker_balanced_panel
        df = df.merge(geo_estab[['id_estab', 'year', 'Addr_type', 'lon_estab', 'lat_estab', 'utm_lat_estab', 'utm_lon_estab', 'h3_res7']], on=['year', 'id_estab'], how='left', validate='m:1', indicator='_merge_geo_estab')
        df._merge_geo_estab.value_counts()

        dfs.append(df)
    
    worker_panel = pd.concat(dfs, ignore_index=True)



#####################
# Create market-to-market transition probabilities

df_trans = worker_panel[['wid','iota','start_date','jid','gamma','occ2Xmeso']].sort_values(by=['wid','start_date'])
df_trans['jid_prev']        = df_trans.groupby('wid')['jid'].shift(1)
df_trans['gamma_prev']      = df_trans.groupby('wid')['gamma'].shift(1)
df_trans['occ2Xmeso_prev']  = df_trans.groupby('wid')['occ2Xmeso'].shift(1)
    
# Restrict to obs with non-missing current and [revious gammas, occ2Xmesos, and iotas.
# XX should I actually be cutting on non-missing jid_prev? I think I should actually wait to do that until making the unipartite transition matrices below. For the bipartite there is no reason why we need to have observed a previous jid. 
df_trans = df_trans.loc[(df_trans['iota'] != -1)                                                \
                  & (df_trans['gamma'].notnull())     & (df_trans['gamma_prev'].notnull())      \
                  & (df_trans['gamma'] != -1)         & (df_trans['gamma_prev'] != -1)          \
                  & (df_trans['occ2Xmeso'].notnull()) & (df_trans['occ2Xmeso_prev'].notnull())  \
                  & (df_trans['jid'].notnull())       & (df_trans['jid_prev'].notnull())        \
                  & (df_trans['jid']!=df_trans['jid_prev'])                                     \
            ][['jid','jid_prev','wid','iota','gamma','gamma_prev','occ2Xmeso','occ2Xmeso_prev']]

# Function to create a transition matri
def create_transition_matrix(df, origin, destination):
    # Get unique values from both columns
    all_values = sorted(set(df[origin].unique()) | set(df[destination].unique()))
    # Create a DataFrame to store counts
    matrix = pd.crosstab(df[origin], df[destination], normalize='index')
    # Reindex to ensure all values are present
    matrix = matrix.reindex(index=all_values, columns=all_values, fill_value=0)
    return matrix

gamma_to_gamma_probs         = create_transition_matrix(df_trans, 'gamma_prev', 'gamma')
occ2Xmeso_to_occ2Xmeso_probs = create_transition_matrix(df_trans, 'occ2Xmeso_prev', 'occ2Xmeso')

gamma_to_gamma_probs.to_parquet(         root + "Data/derived/gamma_to_gamma_probs_for_rafa.parquet")   
occ2Xmeso_to_occ2Xmeso_probs.to_parquet( root + "Data/derived/occ2Xmeso_to_occ2Xmeso_probs_for_rafa.parquet")  

# Pobability of gamma for each iota
grouped = worker_panel.groupby(['iota', 'gamma']).size().unstack(fill_value=0)
p_gi = grouped.div(grouped.sum(axis=1), axis=0)
p_gi.to_parquet( root + "Data/derived/p_gi_for_rafa.parquet")  


# Next I need to produce match probabilities. I don't think I want to actually merge these on because they would be insanely wide. 
# Also need to merge on Aguinaldo's factors
    
    
worker_panel = merge_aguinaldo_onet(worker_panel, root + 'Data/raw/')
    
    
# Keep only relevant columns
worker_panel = worker_panel[['pis', 'id_estab', 'cnpj_raiz', 'cbo2002', 'clas_cnae20', 'codemun', 'tipo_vinculo', 'year', 'wid', 'occ4', 'jid', 'gamma', 'iota', 'occ2Xmeso', 'ind2', 'code_micro', 'Addr_type', 'lat_estab', 'lon_estab', 'utm_lat_estab', 'utm_lon_estab', 'Cognitive skills', 'Operational skills', 'Social and emotional skills', 'Management skills', 'Physical skills', 'Transportation skills', 'Social sciences skills', 'Accuracy skills', 'Design & engineering skills', 'Artistic skills', 'Life sciences skills', 'Information technology skills', 'Sales skills', 'Self-reliance skills', 'Information processing skills', 'Teamwork skills']]
   
# Identify layoffs and merge on a flag to the main data
worker_panel_balanced = pd.read_pickle(root + "Data/derived/mass_layoffs_worker_panel_balanced.p")
worker_panel_balanced['id_estab'] = worker_panel_balanced['id_estab'].str.replace('.0', '', regex=False)
layoffs_only = worker_panel_balanced.loc[worker_panel_balanced['event_time']==0, ['wid','year','id_estab']]

worker_panel = worker_panel.merge(layoffs_only, on=['wid','year','id_estab'], validate='m:1', how='left', indicator='mass_layoff_flag_temp')
print(worker_panel.mass_layoff_flag_temp.value_counts())
worker_panel['mass_layoff_flag'] = worker_panel['mass_layoff_flag_temp']=='both'
worker_panel.drop(columns='mass_layoff_flag_temp', inplace=True)


worker_panel.to_parquet(        root + "Data/derived/worker_panel_w_layoff_flags_for_rafa.parquet")  

root =  '//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/'
worker_panel = pd.read_parquet( root + "Data/derived/worker_panel_w_layoff_flags_for_rafa.parquet")   
gamma_to_gamma_probs         = pd.read_parquet( root + "Data/derived/gamma_to_gamma_probs_for_rafa.parquet")   
occ2Xmeso_to_occ2Xmeso_probs = pd.read_parquet( root + "Data/derived/occ2Xmeso_to_occ2Xmeso_probs_for_rafa.parquet")  
p_gi                         = pd.read_parquet( root + "Data/derived/p_gi_for_rafa.parquet")  



