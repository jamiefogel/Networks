
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import graph_tool.all as gt
import scipy.sparse as sp
import copy
import sys
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pathlib import Path

homedir = os.path.expanduser('~')
root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
os.chdir(root)

import bisbm
from create_df_trans import create_df_trans
from create_unipartite_adjacency_and_degrees import create_unipartite_adjacency_and_degrees
from pull_one_year import pull_one_year
from mass_layoffs_parquet_functions import pull_estab_geos


run_pull= True 

state_codes = [31, 33, 35]
rio_codes = [330045, 330170, 330185, 330190, 330200, 330227, 330250, 330285, 330320, 330330, 330350, 330360, 330414, 330455, 330490, 330510, 330555, 330575]
region_codes = pd.read_csv('Data/raw/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})


#maxrows = 10000
maxrows=None


# 2004 appears to be the first year in which we have job start end dates (data_adm and data_deslig)

# CPI: 06/2015=100
cpi = pd.read_csv('./Data/raw/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
cpi['cpi'] = cpi.cpi/100
cpi['date'] = cpi['date'].dt.to_period('M')



estimated_sbm_mcmc = pickle.load( open('./Data/derived/sbm_output/model_'+modelname+'.p', "rb" ) )
gammas = pd.read_csv('./Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
gammas['gamma'] = gammas.gamma.fillna(-1)
iotas = pd.read_csv('./Data/derived/sbm_output/model_'+modelname+'_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid': object}).rename(columns={'worker_blocks_level_0':'iota'})
iotas['iota'] = iotas.iota.fillna(-1)
    
########################################################################################
########################################################################################
# Create a wid-jid-month panel
########################################################################################
########################################################################################


factor_info = {
    "Factor1": {
        "name": "Cognitive skills",
        "description": "Verbal, linguistic and logical abilities"
    },
    "Factor2": {
        "name": "Operational skills",
        "description": "Maintenance, repair and operation skills"
    },
    "Factor3": {
        "name": "Social and emotional skills",
        "description": "Interpersonal skills"
    },
    "Factor4": {
        "name": "Management skills",
        "description": "Manage resources; build teams; coordinate, motivate and guide subordinates"
    },
    "Factor5": {
        "name": "Physical skills",
        "description": "Bodily-Kinesthetic abilities: body strength; equilibrium; stamina; flexibility"
    },
    "Factor6": {
        "name": "Transportation skills",
        "description": "Visual-spatial skills: Spatial orientation; far and night vision; geography and transportation knowledge"
    },
    "Factor7": {
        "name": "Social sciences skills",
        "description": "Social sciences, education and foreign language skills"
    },
    "Factor8": {
        "name": "Accuracy skills",
        "description": "Being exact and accurate; paying attention to detail; work under pressure and in repetitive settings"
    },
    "Factor9": {
        "name": "Design & engineering skills",
        "description": "Design, engineering and construction skills"
    },
    "Factor10": {
        "name": "Artistic skills",
        "description": "Artistic skills, creativity, unconventional; communications and media"
    },
    "Factor11": {
        "name": "Life sciences skills",
        "description": "Biology, chemistry and medical sciences skills"
    },
    "Factor12": {
        "name": "Information technology skills",
        "description": "Telecommunications, computer operation and programming skills"
    },
    "Factor13": {
        "name": "Sales skills",
        "description": "Sales and Marketing, deal with customers, work under competition"
    },
    "Factor14": {
        "name": "Self-reliance skills",
        "description": "Independence, initiative, innovation"
    },
    "Factor15": {
        "name": "Information processing skills",
        "description": "Retrieve, process and pass-on information"
    },
    "Factor16": {
        "name": "Teamwork skills",
        "description": "Work with colleagues, coordinate others"
    }
}
factor_rename_map = {f"Factor{i}": factor_info[f"Factor{i}"]["name"] for i in range(1, 17)}

# Extract all the names and descriptions into lists (for future reference)
factor_descriptions = [info["description"] for info in factor_info.values()]
factor_names        = [info["name"]        for info in factor_info.values()]
    

onet_scores = pd.read_sas(root + 'Data/raw/scores_o_br.sas7bdat')
# This is a mapping of O*NET occs to Brazilian occ-sector pairs. Some Brazilian occs are mapped to different O*NET occs depending on the sector. Otherwise, this would just be a mapping between Brazilian and O*NET occs.
spine =  pd.read_sas(root + 'Data/raw/cbo2002_cnae20_o.sas7bdat').astype(float)

# Merge the spine of Brazilian occ-sector pairs to the O*NET scores
onet_merged = onet_scores.merge(spine, on=['O'], how="left")

# Convert columns in onet_merged to string if they are not already
onet_merged['cbo2002'] = onet_merged['cbo2002'].astype('Int64')
onet_merged['cla_cnae20'] = onet_merged['cla_cnae20'].astype('Int64')

# Rename columns using Aguinaldo's labels 
onet_merged.rename(columns=factor_rename_map, inplace=True)

# Assuming raw and onet_merged are your DataFrames
onet_merge_keys = {'left': ['cbo2002', 'clas_cnae20'], 'right': ['cbo2002', 'cla_cnae20']}

dups = onet_merged.groupby(['cbo2002', 'cla_cnae20']).size().reset_index(name='dup')
onet_merged = onet_merged.drop_duplicates(subset=['cbo2002', 'cla_cnae20'])
onet_merged.dropna(subset=onet_merge_keys['right'], inplace=True)


########################################
# Pull geography data

years = list(range(firstyear_sbm,lastyear_sbm+1))
geo_estab = pull_estab_geos(years, rais)
    











"""
Efficiently processes RAIS data from multiple years using PyArrow's dataset capabilities.
"""

base_path = rais + 'parquet_novos'
othervars=['data_adm', 'data_deslig', 'clas_cnae20']
age_lower=25
age_upper=55
occvar = 'cbo2002'



print(f'Processing years {min(years)} to {max(years)}')
now = datetime.now()
print(f'Starting at {now.strftime("%H:%M:%S")}')

# Define required columns
vars = ['pis', 'id_estab', occvar, 'codemun', 'tipo_vinculo', 'idade']
if othervars is not None:
    vars.extend(othervars)

base_path = Path(base_path).expanduser()

# Create a unified dataset from all year files
file_paths = [base_path / f'brasil{year}.parquet' for year in years]
if not all(p.exists() for p in file_paths):
    missing = [p for p in file_paths if not p.exists()]
    raise FileNotFoundError(f"Missing parquet files: {missing}")

# Create partitioning to extract year from filenames
partitioning = ds.FilenamePartitioning(
    pa.schema([("year", pa.int32())]), 
    flavor="python",
    segment_regex="brasil(?P<year>[0-9]+)\.parquet"
)

# Create dataset with partitioning
dataset = ds.dataset(
    file_paths,
    partitioning=partitioning
)

# Build filter conditions
filters = []
if municipality_codes is not None:
    filters.append(('codemun', 'in', municipality_codes))
if age_lower is not None:
    filters.append(('idade', '>', age_lower))
if age_upper is not None:
    filters.append(('idade', '<', age_upper))
filters.append(('tipo_vinculo', 'not_in', [30, 31, 35]))

# Read data with pushdown predicates
scanner = dataset.scanner(
    columns=vars,
    filter=ds.dataset.AndExpression(filters) if filters else None
)

# Convert entire dataset to pandas
test_df = scanner.to_table().to_pandas()

# Apply state code filtering if needed (can't be pushed down)
if state_codes is not None:
    test_df = test_df[test_df['codemun'].fillna(99).astype(str).str[:2].astype('int').isin(state_codes)]

# Drop rows with missing values in key columns
test_df = test_df.dropna(subset=['pis', 'id_estab', occvar])

# Add computed columns
test_df['yob'] = test_df['year'] - test_df['idade']
test_df['occ4'] = test_df[occvar].str[0:4]
test_df['jid'] = test_df['id_estab'] + '_' + test_df['occ4']
test_df.rename(columns={'pis': 'wid'}, inplace=True)


# Handle date parsing
if parse_dates:
    for date_col in parse_dates:
        if date_col in test_df.columns:
            test_df[date_col] = pd.to_datetime(test_df[date_col], errors='coerce')


test_df['ind2'] = np.floor(test_df['clas_cnae20']/1000).astype(int)
test_df['sector_IBGE'] = pd.NA

sector_ranges = [
    ((1, 3), 1), ((5, 9), 2), ((10, 33), 3), ((35, 39), 4),
    ((41, 43), 5), ((45, 47), 6), ((49, 53), 7), ((55, 56), 8),
    ((58, 63), 9), ((64, 66), 10), ((68, 68), 11), ((69, 82), 12),
    ((84, 84), 13), ((85, 88), 14), ((90, 97), 15)
]

for (start, end), sector in sector_ranges:
    mask = (test_df['ind2'] >= start) & (test_df['ind2'] <= end)
    test_df.loc[mask, 'sector_IBGE'] = sector


# Post-processing steps
test_df['start_date'] = pd.to_datetime(test_df['data_adm'], errors='coerce')
test_df['codemun'] = test_df['codemun'].astype(int)
test_df = test_df.merge(muni_meso_cw, how='left', on='codemun', copy=False)
test_df['occ2Xmeso'] = test_df.cbo2002.str[0:2] + '_' + test_df['code_meso'].astype('str')

# Perform merges
test_df = test_df.merge(gammas, on='jid', how='left')
test_df['gamma'] = test_df.gamma.fillna(-1)
test_df = test_df.merge(iotas, on='wid', how='left')
test_df['iota'] = test_df.iota.fillna(-1)

test_df = test_df.merge(
    geo_estab[['id_estab', 'year', 'Addr_type', 'lon_estab', 'lat_estab', 
               'utm_lat_estab', 'utm_lon_estab', 'h3_res7']], 
    on=['year', 'id_estab'], 
    how='left', 
    validate='m:1', 
    indicator='_merge_geo_estab'
)

test_df['cbo2002'] = test_df['cbo2002'].astype(int)
test_df = test_df.merge(
    onet_merged[onet_merge_keys['right'] + factor_names], 
    left_on=onet_merge_keys['left'], 
    right_on=onet_merge_keys['right'], 
    how='left', 
    suffixes=[None, '_y'], 
    validate='m:1', 
    indicator='_merge_ONET'
)
        



if run_pull==True:
    df_list
    for year in years:
        print(year)
        raw = pull_one_year(year, 'cbo2002', othervars=['data_adm', 'clas_cnae20'], state_codes=state_codes, age_lower=25, age_upper=55, parse_dates=['data_adm'], nrows=maxrows, filename=rais_filename_stub + str(year) + '.csv')
        # Because dates aren't stored correctly in some years. Also we had a very small number of invalid dates (5 out of hundreds of millions) and this sets them to missing rather than failing.
        raw['start_date'] = pd.to_datetime(raw['data_adm'], errors='coerce')
        raw['codemun'] = raw['codemun'].astype(int)
        raw = raw.merge(muni_meso_cw, how='left', on='codemun', copy=False) # validate='m:1', indicator=True)
        raw['occ2Xmeso'] = raw.cbo2002.str[0:2] + '_' + raw['code_meso'].astype('str')
        raw = raw.merge(gammas, on='jid', how='left')
        raw['gamma'] = raw.gamma.fillna(-1)
        raw = raw.merge(iotas, on='wid', how='left')
        raw['iota'] = raw.iota.fillna(-1)
        raw = raw.merge(geo_estab[['id_estab', 'year', 'Addr_type', 'lon_estab', 'lat_estab', 'utm_lat_estab', 'utm_lon_estab', 'h3_res7']], on=['year', 'id_estab'], how='left', validate='m:1', indicator='_merge_geo_estab')
        raw['cbo2002'] = raw['cbo2002'].astype(int)
        raw = raw.merge(onet_merged[onet_merge_keys['right'] + factor_names], left_on=onet_merge_keys['left'], right_on=onet_merge_keys['right'], how='left', suffixes=[None, '_y'], validate='m:1', indicator='_merge_ONET')
        raw = raw.drop(columns=['yob','occ4','tipo_vinculo','idade','codemun','id_estab'])
        raw.to_pickle('./Data/derived/' + modelname + '_pred_flows_raw_' + str(year) + '.p')
        gc.collect()
        df_list.append(raw)
        

#####################
# Create data frame of transitions
df = pd.concat(df_list, ignore_index=True)

# Save memory.
# 4 decimal points of lat/lon is accurate to about 11 meters. Good enough
coord_columns = ['lat_estab', 'lon_estab', 'utm_lat_estab', 'utm_lon_estab']
df[coord_columns] = df[coord_columns].round(4).astype('float32')

# Find all columns containing "skills" (case-insensitive) and convert them to float32
skills_columns = df.columns[df.columns.str.contains('skills', case=False)]
df[skills_columns] = df[skills_columns].astype('float32')



##############################################################################################
##############################################################################################
# Create job-to-job transitions df
##############################################################################################
##############################################################################################

required_vars = ['jid','gamma','occ2Xmeso']
vars_to_shift = ['lat_estab','lon_estab'] + list(skills_columns)

df = df.sort_values(by=['wid','start_date'])

shifted_vals = {}
grouped = df.groupby('wid')

for var in required_vars + vars_to_shift:
    shifted_vals[f'{var}_prev'] = grouped[var].shift(1)

df = df.assign(**shifted_vals)
    
# Restrict to obs with non-missing current and [revious gammas, occ2Xmesos, and iotas.
# XX should I actually be cutting on non-missing jid_prev? I think I should actually wait to do that until making the unipartite transition matrices below. For the bipartite there is no reason why we need to have observed a previous jid. 
df_trans = df[      (df['iota'] != -1)                                              \
                  & (df['gamma'].notnull())     & (df['gamma_prev'].notnull())      \
                  & (df['gamma'] != -1)         & (df['gamma_prev'] != -1)          \
                  & (df['occ2Xmeso'].notnull()) & (df['occ2Xmeso_prev'].notnull())  \
                  & (df['jid'].notnull())       & (df['jid_prev'].notnull())        \
                  & (df['jid']!=df['jid_prev'])                                     \
            ][['jid','jid_prev','wid','iota','gamma','gamma_prev','occ2Xmeso','occ2Xmeso_prev'] + vars_to_shift + [f'{var}_prev' for var in vars_to_shift]]

df_trans['wid_num_transitions'] = df_trans.groupby('wid')['wid'].transform('count')
