
# XX Eventually this will be defined in whichever version of do_all calls this file
modelname = '3states_2009to2011'
rais = "/home/DLIPEA/p13861161/rais/RAIS/"
firstyear_sbm = 2009
lastyear_sbm  = 2011
state_codes = [31, 33, 35]


from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import scipy.sparse as sp
import copy
import sys
from scipy.spatial.distance import cdist, euclidean, cosine, cityblock
from math import radians, sin, cos, sqrt, asin
import matplotlib.pyplot as plt
import binsreg

homedir = os.path.expanduser('~')
root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
os.chdir(root)

from create_df_trans import create_df_trans
from create_unipartite_adjacency_and_degrees import create_unipartite_adjacency_and_degrees
from pull_one_year import pull_one_year
from pull_one_year_parquet import pull_one_year_parquet
from mass_layoffs_parquet_functions import pull_estab_geos


run_pull= True 

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



#estimated_sbm_mcmc = pickle.load( open(root + 'Data/derived/sbm_output/model_'+modelname+'.p', "rb" ) )
gammas = pd.read_csv(root + 'Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
gammas['gamma'] = gammas.gamma.fillna(-1)
iotas = pd.read_csv(root + 'Data/derived/sbm_output/model_'+modelname+'_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid': object}).rename(columns={'worker_blocks_level_0':'iota'})
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
    

if run_pull==True:
    df_list = []
    for year in years:
        print(year)
        raw = pull_one_year_parquet(year, 'cbo2002', othervars=['data_adm', 'data_deslig', 'clas_cnae20'], state_codes=state_codes, age_lower=25, age_upper=55, parse_dates=['data_adm','data_deslig'], nrows=maxrows, filename=rais + 'parquet_novos/brasil'  + str(year) + '.parquet')
        # Because dates aren't stored correctly in some years. Also we had a very small number of invalid dates (5 out of hundreds of millions) and this sets them to missing rather than failing.
        raw['start_date'] = pd.to_datetime(raw['data_adm'], errors='coerce')
        raw['end_date'] = pd.to_datetime(raw['data_deslig'], errors='coerce')
        raw['codemun'] = raw['codemun'].astype(int)
        raw = raw.merge(muni_meso_cw, how='left', on='codemun', copy=False) # validate='m:1', indicator=True)
        raw['occ2Xmeso'] = raw.cbo2002.str[0:2] + '_' + raw['code_meso'].astype('str')
        raw = raw.merge(gammas, on='jid', how='left')
        raw['gamma'] = raw.gamma.fillna(-1)
        raw = raw.merge(iotas, on='wid', how='left')
        raw['iota'] = raw.iota.fillna(-1)
        raw = raw.merge(geo_estab[['id_estab', 'year', 'Addr_type', 'lon_estab', 'lat_estab', 'h3_res7']], on=['year', 'id_estab'], how='left', validate='m:1', indicator='_merge_geo_estab')
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
coord_columns = ['lat_estab', 'lon_estab']
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

def haversine_vectorized(lat1, lon1, lat2, lon2):
    """
    Vectorized haversine distance calculation
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula components
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    # Vectorized calculations
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6371 * c  # Earth's radius in kilometers
    
    return km


df_trans['move_dist_km'] = haversine_vectorized(
    df_trans['lat_estab'].values,
    df_trans['lon_estab'].values,
    df_trans['lat_estab_prev'].values,
    df_trans['lon_estab_prev'].values
)
df_trans['ln_move_dist_km'] = np.log(df_trans['move_dist_km'])

prev_skills_columns = [f"{col}_prev" for col in skills_columns]

curr_skills = df_trans[skills_columns].values
prev_skills = df_trans[prev_skills_columns].values

df_trans['skill_distance_euclidean'] = np.sqrt(np.sum((curr_skills - prev_skills)**2, axis=1))



plt.figure(figsize=(10, 6))
# Create binned scatter plot with binsreg
yvar = 'skill_distance_euclidean'
xvar = 'move_dist_km'
est = binsreg.binsreg(
    y=yvar,
    x=xvar,
    data=df_trans,
    nbins=20,      # Number of bins
    line=(2, 2),   # Degree of polynomial for regression and CI/CB
    ci=(2, 2),     # Confidence interval polynomial degree
    dots=(0, 0)    # Include binned observation dots
)
# Display the plot
#plot = est.bins_plot + ggtitle(f'Binned Scatter Plot of {y_var} vs. {x_var}') + theme_bw() + theme(legend_position='none')
# Save the plot
#plot_filename = f'{y_var}_vs_{x_var}_{modelname}.png'
plt.show()
plt.savefig(root + 'Results/test_binscatter.png')
plt.close()
