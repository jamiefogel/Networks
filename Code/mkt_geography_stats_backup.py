<<<<<<< HEAD
=======
from datetime import datetime
import pandas as pd
import numpy as np
import os
import sys
import matplotlib.pyplot as plt
import geobr
import pyproj # for converting geographic degree measures of lat and lon to the UTM system (i.e. in meters)
from scipy.integrate import simps # to calculate the AUC for the decay function
import binsreg
import statsmodels.api as sm
from tqdm import tqdm # to calculate progress of some operations for geolocation
import gc
import glob

rcounts = []
rc = 0

# DIRECTORY WHERE THE DATA IS
# Automatically chooses the folder between windows vs linux servers
if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')


root = homedir + '/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
os.chdir(root)

from pull_one_year import pull_one_year

state_codes = [31, 33, 35]
region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})
region_codes['codemun'] = region_codes.code_munic//10

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

#modelname = '3states_2009to2011_mcmc'
modelname = '3states_2009to2011'

gammas = pd.read_csv(root + '/Data/derived/sbm_output/model_' + modelname + '_jblocks.csv', usecols=jcolumns).rename(columns=jrename)
iotas  = pd.read_csv(root + '/Data/derived/sbm_output/model_' + modelname + '_wblocks.csv', usecols=wcolumns).rename(columns=wrename)
iotas['wid'] = iotas['wid'].astype('str')
#model = pickle.load(open(root + '/Data/derived/sbm_output/model_'+modelname+'.p', "rb" ))
                       
################################################
# Pull other variables like education for a specific year
run_sbm = False
run_sbm_mcmc = False
run_pull=False
run_append = False
run_create_earnings_panel = False
maxrows=None
modelname = '3states_2009to2011'
filename_stub = "panel_"+modelname
rais_filename_stub =  '~/rais/RAIS/csv/brasil' 
#rais_filename_stub = root + './Data/raw/synthetic_data_'

firstyear_sbm = 2009
lastyear_sbm  = 2011
firstyear_panel = firstyear_sbm
lastyear_panel  = lastyear_sbm
state_codes = [31, 33, 35]

for year in range(firstyear_panel,lastyear_panel+1):
    print(year, ' ', datetime.now())
    df = pull_one_year(year, 'cbo2002', othervars=['grau_instr','rem_med_r','clas_cnae20','codemun'], state_codes=state_codes, age_lower=25, age_upper=55, nrows=None, filename=rais_filename_stub + str(year) + '.csv')
    if year==firstyear_panel:
        raw = df
    else:
        raw = df.append(raw, sort=True)
    del df

#raw = raw.iloc[:300000,:]

##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################


raw = raw.merge(gammas, how='left', validate='m:1', on='jid',indicator=True)
print('Merge stats for gammas')
print(raw._merge.value_counts())
raw.drop(columns=['_merge'], inplace=True)

raw = raw.merge(iotas, how='left', validate='m:1', on='wid',indicator=True)
print('Merge stats for iotas')
print(raw._merge.value_counts())
raw.drop(columns=['_merge'], inplace=True)

del iotas, gammas


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################

raw = raw.merge(muni_meso_cw, on='codemun')


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################



raw['occ2'] = raw['cbo2002'].str[0:2]
raw['occ4'] = raw['cbo2002'].str[0:4]
raw['ind2'] = raw['clas_cnae20'].astype('str').str[0:2].astype('int')
raw['sector_IBGE'] = np.nan
raw['sector_IBGE'].loc[( 1<=raw['ind2']) & (raw['ind2'] <= 3)] = 1  
raw['sector_IBGE'].loc[( 5<=raw['ind2']) & (raw['ind2'] <= 9)] = 2 
raw['sector_IBGE'].loc[(10<=raw['ind2']) & (raw['ind2'] <=33)] = 3 
raw['sector_IBGE'].loc[(35<=raw['ind2']) & (raw['ind2'] <=39)] = 4 
raw['sector_IBGE'].loc[(41<=raw['ind2']) & (raw['ind2'] <=43)] = 5 
raw['sector_IBGE'].loc[(45<=raw['ind2']) & (raw['ind2'] <=47)] = 6 
raw['sector_IBGE'].loc[(49<=raw['ind2']) & (raw['ind2'] <=53)] = 7 
raw['sector_IBGE'].loc[(55<=raw['ind2']) & (raw['ind2'] <=56)] = 8 
raw['sector_IBGE'].loc[(58<=raw['ind2']) & (raw['ind2'] <=63)] = 9 
raw['sector_IBGE'].loc[(64<=raw['ind2']) & (raw['ind2'] <=66)] = 10
raw['sector_IBGE'].loc[(68<=raw['ind2']) & (raw['ind2'] <=68)] = 11
raw['sector_IBGE'].loc[(69<=raw['ind2']) & (raw['ind2'] <=82)] = 12
raw['sector_IBGE'].loc[(84<=raw['ind2']) & (raw['ind2'] <=84)] = 13
raw['sector_IBGE'].loc[(85<=raw['ind2']) & (raw['ind2'] <=88)] = 14
raw['sector_IBGE'].loc[(90<=raw['ind2']) & (raw['ind2'] <=97)] = 15

# Recode education variable to approximately reflect years of schooling
raw['grau_instr'] = raw['grau_instr'].replace({1:1, 2:3, 3:5, 4:7, 5:9, 6:10, 7:12, 8:14, 9:16, 10:18, 11:21})

###raw.to_pickle(root + '/Data/derived/mkt_geography_raw.p')
#raw = pd.read_pickle(root + '/Data/derived/mkt_geography_raw.p')

# Dropping NaNs iotas or gammas
# 1st, printing the % of the observations with missing iota or gamma
print('Proportion of obs with missing iota or gamma:')
print(raw[['iota', 'gamma']].isnull().sum() / raw.shape[0])
# Loop through each unique year in the dataset
for year in raw['year'].unique():
    # Filter the DataFrame for the current year
    raw_year = raw[raw['year'] == year]
    
    # Calculate the proportion of missing values for 'iota' and 'gamma' columns
    missing_proportions = raw_year[['iota', 'gamma']].isnull().sum() / raw_year.shape[0]
    
    # Print the year and the corresponding missing proportions
    print(f"Year: {year}")
    print(missing_proportions)
    print("\n")

# Drop rows with NaN values in either 'iota' or 'gamma' columns
raw = raw.dropna(subset=['iota', 'gamma'])
print('Proportion of obs with missing iota or gamma:')
print(raw[['iota', 'gamma']].isnull().sum() / raw.shape[0])


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])

###########################################
# GETTING LATITUDE AND LONGITUDE FOR ALL MUNICS IN THE DATA & MERGING THEM BACK TO THE MAIN DATASET called 'raw'

# Pull meso codes for our states of interest. Choosing 2010 b/c this isn't available for all years and 2010 is in the middle of our sample
muni_sp = geobr.read_municipality(code_muni="SP", year=2010)
muni_rj = geobr.read_municipality(code_muni='RJ', year=2010)
muni_mg = geobr.read_municipality(code_muni='MG', year=2010)
munis = pd.concat([muni_sp, muni_rj, muni_mg], ignore_index=True)
munis['lon_munic'] = munis.geometry.centroid.x
munis['lat_munic'] = munis.geometry.centroid.y
munis['codemun'] = munis.code_muni//10

# Converting coordinates to UTM, so the units are in meters
# Function to convert geographic coordinates to UTM using a fixed zone 23S for Sao Paulo
# Create the transformer object for UTM zone 23S
transformer = pyproj.Transformer.from_crs("epsg:4326", "epsg:32723", always_xy=True)
# Function to convert geographic coordinates to UTM
def convert_to_utm(lon, lat):
    return transformer.transform(lon, lat)

# Initialize lists to store the UTM coordinates
utm_lon = []
utm_lat = []

# Convert latitude and longitude to UTM with progress indicator
for lon, lat in tqdm(zip(munis['lon_munic'].values, munis['lat_munic'].values), total=len(munis)):
    utm_x, utm_y = convert_to_utm(lon, lat)
    utm_lon.append(utm_x)
    utm_lat.append(utm_y)

# Assign the UTM coordinates back to the DataFrame
munis['utm_lon_munic'] = utm_lon
munis['utm_lat_munic'] = utm_lat

raw = raw.merge(munis[['geometry', 'lon_munic', 'lat_munic', 'codemun', 'utm_lon_munic', 'utm_lat_munic']], on='codemun',how='left', indicator='_merge_munics')
raw['_merge_munics'].value_counts()
#raw.drop(columns=['_merge_munics'], inplace=True)


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################



###########################
# GEOLOCATION FOR RAIS ESTABLISHMENTS

# filepath: \\storage6\bases\DADOS\RESTRITO\RAIS\geocode
# file of type: rais_geolocalizada_2009
years = raw.year.unique().tolist()

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
    geo = pd.read_parquet(f'~/rais/RAIS/geocode/rais_geolocalizada_{year}.parquet')
    geo['year'] = year
    geo.rename(columns={'lon': 'lon_estab', 'lat': 'lat_estab'}, inplace=True)
    # Append the DataFrame to the list
    geo_list.append(geo)

# Concatenate all the DataFrames in the list into a single DataFrame
geo_estab = pd.concat(geo_list, ignore_index=True)
del geo, geo_list 

############################

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


# FROM CHATGPT
# h3_res9, h3_res8, h3_res7: These are H3 indexes at different resolutions (9, 8, and 7 respectively). H3 is a hierarchical hexagonal geospatial indexing system created by Uber. The resolution levels indicate the granularity of the indexing:
# Resolution 7: Coarser granularity.
# Resolution 8: Intermediate granularity.
# Resolution 9: Finer granularity.

###############
raw = raw.merge(geo_estab[['id_estab', 'year', 'Addr_type', 'lon_estab', 'lat_estab', 'utm_lat_estab', 'utm_lon_estab', 'h3_res7']], on=['year', 'id_estab'], how='left', validate='m:1', indicator='_merge_geo_estab')
raw.columns
raw._merge_geo_estab.value_counts()


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################

wtf = pd.DataFrame(rcounts)
print(wtf)


del geo_estab, utm_lon, utm_lat

for l in ['lat_', 'lon_', 'utm_lat_', 'utm_lon_']:
    print('----------------------------------------')
    print('MISSING VALUES FOR ' + l + 'munic')
    print(raw[l + 'munic'].isna().sum(),'/',raw.shape[0])


# Replace NaN values in *_estab columns with values from their *_munic counterparts
for l in ['lat_', 'lon_', 'utm_lat_', 'utm_lon_']:
    print('----------------------------------------')
    print('MISSING VALUES FOR ' + l + 'estab, BEFORE INPUTATION')
    print(raw[l + 'estab'].isna().sum(),'/',raw.shape[0])
    raw[l + 'estab'] = raw[l + 'estab'].fillna(raw[l + 'munic'])
    print('Missings after replacement with ' + l + 'munic, after inputation')
    print(raw[l + 'estab'].isna().sum(),'/',raw.shape[0])

#raw.to_pickle(root + '/Data/derived/mkt_geography_raw.p')
#raw = pd.read_pickle(root + '/Data/derived/mkt_geography_raw.p')


###########################################
# COMPUTING THE 'ACTUAL/OBSERVED' IOTA SPATIAL VARIANCE IN TWO WAYS, BOTH IN METERS

geocode_unit = 'estab'

lat_var = 'utm_lat_' + geocode_unit
lon_var = 'utm_lon_' + geocode_unit
folder = 'geo_' + geocode_unit


# METHOD 1 FOR SPATIAL VARIANCE: Function to calculate standard distance for a group
# DEFINITION (this is a 2-D variance): 
    # 1) calculates the variance of the points (related to the avg center/centroid)
def standard_distance(group, lat_var,lon_var):
    mean_utm_lon = group[lon_var].mean()
    mean_utm_lat = group[lat_var].mean()
    squared_distances = (group[lon_var] - mean_utm_lon) ** 2 + (group[lat_var] - mean_utm_lat) ** 2
    std_distance = np.sqrt(squared_distances.mean())
    return std_distance/1000


# Group by 'iota' and calculate standard distance for each group
spatial_var = raw.groupby('iota').apply(lambda x: pd.Series({
    'std_distance': standard_distance(x,lat_var,lon_var)
})).reset_index()

# Display the resulting DataFrame
spatial_var.describe()


###########################################
# COMPUTING THE 'POTENTIAL' IOTA SPATIAL VARIANCE, NOW USING POTENTIAL MATCHES BY EXPLORING THE MATCH PROBABILITIES

# PROB OF MATCHING WITH GAMMA GIVEN IOTA

# Group by 'iota' and 'gamma' and count occurrences
grouped = raw.groupby(['iota', 'gamma']).size().unstack(fill_value=0)

# Prob of matching with a gamma, given iota
p_gi = grouped.div(grouped.sum(axis=1), axis=0)
p_gi.sum(axis=1)

# Get the total number of observations for each gamma
gamma_totals = raw['gamma'].value_counts()

# Prob of matching with any job inside a gamma, given iota
p_gi_ji = p_gi.div(gamma_totals, axis=1)

# Function to calculate weighted standard distance
def weighted_standard_distance(df, weights_prob, lat_var, lon_var):
    mean_utm_lon = np.average(df[lon_var], weights=weights_prob)
    mean_utm_lat = np.average(df[lat_var], weights=weights_prob)
    squared_distances = weights_prob * ((df[lon_var] - mean_utm_lon) ** 2 + (df[lat_var] - mean_utm_lat) ** 2)
    std_distance = np.sqrt(squared_distances.sum() / weights_prob.sum())
    return std_distance/1000

# List to store results
weighted_spatial_var = []
weights_dict = {iota: p_gi_ji.loc[iota].reset_index().rename(columns={iota: 'weight', 'index': 'gamma'}) for iota in p_gi_ji.index}

# Loop over each iota category
count = 0
tot = len(weights_dict)
for iota, weights in weights_dict.items():
    # Merge the weights directly with raw based on gamma
    raw_temp = raw[[lat_var, lon_var, 'gamma']].merge(weights, on='gamma', how='left')
    raw_temp['weight'].fillna(0, inplace=True)
    print(raw_temp.weight.sum()) 
    # Use the merged variable as weights for the weighted spatial variance calculation
    std_distance = weighted_standard_distance(raw_temp, raw_temp['weight'], lat_var, lon_var)
    # Store results
    weighted_spatial_var.append({'iota': iota, 'weighted_std_distance': std_distance})
    count +=1
    print(str(count) + '/' + str(tot))

del raw_temp

# Merge the final results with spatial_metrics_df on the 'iota' column
spatial_var = spatial_var.merge(pd.DataFrame(weighted_spatial_var), on='iota', how='left', indicator='_merge_weighted')
print(spatial_var._merge_weighted.value_counts())
print(spatial_var.describe())
spatial_var.drop(columns=['_merge_weighted'], inplace=True)


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################



#########################
# BRINGING O*NET TO THE GAME

# Next time we shoud just load the final O*NET dataset in the beginning of this code, in the 'raw' object, using the output of this file:
   # https://github.com/jamiefogel/Networks/blob/61cf62ded22e7722367639237900d511a3a1ea17/Code/process_brazil_onet.py#L23


# Factor names and descriptions from Aguinaldo. See https://mail.google.com/mail/u/0/#inbox/FMfcgzGxTPDrccpzvkjfSwhqQhQxvlKl
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


# These are O*NET scores and factors. I believe that the variable 'O' is a unique identifier corresponding to unique O*NET occupations. 
onet_scores = pd.read_sas(root + 'Data/raw/scores_o_br.sas7bdat')

# This is a mapping of O*NET occs to Brazilian occ-sector pairs. Some Brazilian occs are mapped to different O*NET occs depending on the sector. Otherwise, this would just be a mapping between Brazilian and O*NET occs.
spine =  pd.read_sas(root + 'Data/raw/cbo2002_cnae20_o.sas7bdat').astype(float)

# Merge the spine of Brazilian occ-sector pairs to the O*NET scores
onet_merged = onet_scores.merge(spine, on=['O'], how="left")

# Rename columns using Aguinaldo's labels 
#onet_merged.rename(columns=factor_rename_map, inplace=True)


# Convert columns in raw to string if they are not already
# Convert cbo2002 to numeric, coercing errors to NaN
raw['cbo2002'] = pd.to_numeric(raw['cbo2002'], errors='coerce')
raw['cbo2002'] = raw['cbo2002'].astype('Int64')
raw['clas_cnae20'] = raw['clas_cnae20'].astype('Int64')

# Convert columns in onet_merged to string if they are not already
onet_merged['cbo2002'] = onet_merged['cbo2002'].astype('Int64')
onet_merged['cla_cnae20'] = onet_merged['cla_cnae20'].astype('Int64')

# Function to merge in batches
def merge_in_batches(left_df, right_df, merge_keys, batch_size=1000000):
    # Split the left DataFrame into smaller batches
    batches = np.array_split(left_df, np.ceil(len(left_df) / batch_size))
    
    # Initialize an empty DataFrame to store the merged results
    merged_result = pd.DataFrame()
    
    # Process each batch
    for i, batch in enumerate(batches):
        print(f"Processing batch {i + 1}/{len(batches)}")
        merged_batch = batch.merge(
            right_df,
            left_on=merge_keys['left'],
            right_on=merge_keys['right'],
            how='left',
            suffixes=[None, '_y'],
            validate='m:1',
            indicator='_merge_ONET'
        )
        merged_result = pd.concat([merged_result, merged_batch], ignore_index=True)
    
    return merged_result

# Assuming raw and onet_merged are your DataFrames
merge_keys = {'left': ['cbo2002', 'clas_cnae20'], 'right': ['cbo2002', 'cla_cnae20']}

dups = onet_merged.groupby(['cbo2002', 'cla_cnae20']).size().reset_index(name='dup')
print(dups.dup.value_counts())
onet_merged = onet_merged.drop_duplicates(subset=['cbo2002', 'cla_cnae20'])


# Perform the merge in batches
raw = merge_in_batches(
    raw,
    onet_merged[['cbo2002', 'cla_cnae20', 'Factor1', 'Factor2', 'Factor3', 'Factor4', 'Factor5', 'Factor6', 'Factor7', 'Factor8', 'Factor9', 'Factor10', 'Factor11', 'Factor12', 'Factor13', 'Factor14', 'Factor15', 'Factor16']],
    merge_keys,
    batch_size=1000000  # Adjust batch size as needed
)

gc.collect()
# Display the counts of the merge indicator
print(raw['_merge_ONET'].value_counts())
del onet_merged, spine

# Define the function to save the DataFrame in chunks
def save_dataframe_in_chunks(df, file_path, chunk_size=100000):
    # Split the DataFrame into chunks
    num_chunks = np.ceil(len(df) / chunk_size).astype(int)    
    for i in range(num_chunks):
        chunk = df[i*chunk_size:(i+1)*chunk_size]
        chunk_file_path = f"{file_path}_chunk_{i}.p"
        chunk.to_pickle(chunk_file_path)
        print(f"Saved chunk {i+1}/{num_chunks} to {chunk_file_path}")

# Save the raw DataFrame in chunks
save_dataframe_in_chunks(raw, root + '/Data/derived/mkt_geography_raw_', chunk_size=int(2e7))

# Get a list of all chunk files
def load_dataframe_in_chunks(file_path_pattern):
    chunk_files = sorted(glob.glob(file_path_pattern))
    # Load each chunk and concatenate them into a single DataFrame
    raw_chunks = [pd.read_pickle(chunk_file) for chunk_file in chunk_files]
    return pd.concat(raw_chunks, ignore_index=True)

#raw = load_dataframe_in_chunks(root + '/Data/derived/mkt_geography_raw_chunk_*.p')


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################



################################################
### BRINGING OTHER COVARIATES TO CORRELATE WITH SPATIAL DISTANCE STATISTICS

# Education variables
# Create binary variables
# Mapping from grau_instr to years
years_mapping = {
    1: 1, 2: 3, 3: 5, 4: 7, 5: 9, 6: 10, 7: 12, 8: 14, 9: 16, 10: 18, 11: 21
}

# Create the `years` variable based on the mapping
raw['educ_years'] = raw['grau_instr'].map(years_mapping)

# Create dummy variable for different levels
raw['ed_noeduc'] = np.where(raw['grau_instr'].isin([1, 2]), 1, 0)
raw['ed_elementary'] = np.where(raw['grau_instr'].isin([3, 4]), 1, 0)
raw['ed_midschool'] = np.where(raw['grau_instr'].isin([5, 6]), 1, 0)
raw['ed_highschool'] = np.where(raw['grau_instr'].isin([7, 8]), 1, 0)
raw['ed_college'] = np.where(raw['grau_instr'].isin([9]), 1, 0)
raw['ed_gradschool'] = np.where(raw['grau_instr'].isin([10, 11]), 1, 0)

# Create binary variables
raw['ms_degree'] = np.where(raw['grau_instr'] >= 5, 1, 0)
raw['hs_degree'] = np.where(raw['grau_instr'] >= 7, 1, 0)
raw['college_degree'] = np.where(raw['grau_instr'] >= 9, 1, 0)

# Custom function to get the mode
def mode_function(series):
    mode_value = series.mode()
    if len(mode_value) > 0:
        return mode_value[0]
    else:
        return None

# List of columns for which to compute the mode
mode_columns = ['grau_instr', 'ind2', 'sector_IBGE', 'occ2', 'occ4']

# Dictionary to hold the mode results
mode_results = {}

# Compute the mode for each column
for col in mode_columns:
    print(col)
    mode_results[f'modal_{col}'] = raw.groupby('iota')[col].apply(mode_function).reset_index()

# Merge the mode results into a single DataFrame
modes = mode_results[f'modal_{mode_columns[0]}']
for col in mode_columns[1:]:
    modes = modes.merge(mode_results[f'modal_{col}'], on='iota')

# Assuming modes is your DataFrame
modes = modes.set_index('iota').add_prefix('modal_').reset_index()

iotas_w_attributes = raw.groupby('iota').agg(
    educ_years=('educ_years','mean'),
    educ_median=('grau_instr', 'median'),
    educ_mean=('grau_instr', 'mean'),
    mean_monthly_earnings=('rem_med_r', 'mean'),
    ed_noeduc=('ed_noeduc', 'mean'),    
    ed_elementary=('ed_elementary', 'mean'),    
    ed_midschool=('ed_midschool', 'mean'),    
    ed_highschool=('ed_highschool', 'mean'),    
    ed_college=('ed_college', 'mean'),    
    ed_gradschool=('ed_gradschool', 'mean'),      
    ms_degree=('ms_degree', 'mean'),
    hs_degree=('hs_degree', 'mean'), 
    college_degree=('college_degree', 'mean'),
    f1=('Factor1', 'mean'),
    f2=('Factor2', 'mean'),
    f3=('Factor3', 'mean'),
    f4=('Factor4', 'mean'),
    f5=('Factor5', 'mean'),
    f6=('Factor6', 'mean'),
    f7=('Factor7', 'mean'),
    f8=('Factor8', 'mean'),
    f9=('Factor9', 'mean'),
    f10=('Factor10', 'mean'),
    f11=('Factor11', 'mean'),
    f12=('Factor12', 'mean'),
    f13=('Factor13', 'mean'),
    f14=('Factor14', 'mean'),
    f15=('Factor15', 'mean'),
    f16=('Factor16', 'mean')
).reset_index()




# Add the precomputed modes to the final DataFrame
iotas_w_attributes = iotas_w_attributes.merge(modes, on='iota', how='left')



# Create some rank variables 
iotas_w_attributes['educ_mean_rank'] = iotas_w_attributes['educ_mean'].rank(method='dense', pct=True)
iotas_w_attributes['mean_monthly_earnings_rank'] = iotas_w_attributes['mean_monthly_earnings'].rank(method='dense', pct=True)
iotas_w_attributes['log_mean_monthly_earnings'] = np.log(iotas_w_attributes['mean_monthly_earnings'])

# Compute the number of unique workers and jobs and worker--job pairs 
num_unique_jids = raw.groupby('iota')['jid'].nunique().reset_index().rename(columns={'jid':'num_unique_jids'})
num_unique_wids = raw.groupby('iota')['wid'].nunique().reset_index().rename(columns={'wid':'num_unique_wids'})
#num_unique_wid_jids = raw.groupby('iota').apply(lambda x: x[['jid', 'wid']].drop_duplicates().shape[0]).astype(int).reset_index().rename(columns={0:'num_unique_wid_jids'})
# Group by 'iota' and count unique pairs of 'jid' and 'wid'
num_unique_wid_jids = (raw[['iota', 'jid', 'wid']]
                       .drop_duplicates()
                       .groupby('iota')
                       .size()
                       .reset_index(name='num_unique_wid_jids'))

# Merge on iota characteristics 
for var in [num_unique_jids, num_unique_wids, num_unique_wid_jids]:
    iotas_w_attributes = iotas_w_attributes.merge(var, on='iota', validate='1:1')


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################



##############################################
# COMPUTING THE MATCHING DECAY FUNCTIONS

# Step 1: Compute the centroid of observations for a given iota
def compute_centroid(group):
    mean_utm_lon = group[lon_var].mean()
    mean_utm_lat = group[lat_var].mean()
    return mean_utm_lon, mean_utm_lat

# Step 2: Compute distances from centroid
def compute_distances(group, centroid_lon, centroid_lat):
    distances = np.sqrt((group[lon_var] - centroid_lon) ** 2 + (group[lat_var] - centroid_lat) ** 2)
    return distances/1000

# Step 3: Compute the proportion of observations within specified distances
def compute_proportions(distances, thresholds):
    proportions = [(distances > threshold).mean() for threshold in thresholds]
    return proportions

# Function to compute area under the curve
def compute_auc(proportions, thresholds_km):
    # Compute the area under the curve using the trapezoidal rule
    auc = simps(proportions, thresholds_km)
    return auc

# Distance thresholds in km
distance_thresholds_km = list(range(0, 1001, 25))

# Distance thresholds in meters (for UTM coordinates)
#distance_thresholds = [threshold*1000 for threshold in distance_thresholds_km[1:]]

# store results
avgdist_auc = []

# Loop over each iota category
for iota in raw['iota'].unique():
    # Step 1: Slice raw to keep only observations regarding one iota
    iota_group = raw[raw['iota'] == iota]
    
    # Step 2: Compute the centroid of these observations
    centroid_lon, centroid_lat = compute_centroid(iota_group)
    
    # Step 3: Compute distances from the centroid
    distances = compute_distances(iota_group, centroid_lon, centroid_lat)
    
    # Step 4: Compute the proportion of observations within specified distances
    proportions = compute_proportions(distances, distance_thresholds_km)
    
    # Compute the area under the curve
    auc = compute_auc(proportions, distance_thresholds_km)
    avgdist_auc.append({'iota': iota, 'avg_dist_centroid': distances.mean(), 'auc_decay': auc})
    
    # Step 5: Create a line graph with the decay function
    plt.figure()
    plt.plot(distance_thresholds_km, proportions, marker='o')
    plt.title(f'Decay Function for Iota {iota}')
    plt.xlabel('Distance (km)')
    plt.ylabel('Proportion of Observations')
    plt.grid(True, which='both', color='lightgray', linestyle='--', linewidth=0.5)
    
    # Save the plot
    plt.savefig(os.path.join(root + '/Results/iota_summary_stats/geo_' + geocode_unit + '/decay_plots', f'decay_function_iota_{iota}.png'))
    plt.close()

# Merge the final results with spatial_metrics_df on the 'iota' column
spatial_var = spatial_var.merge(pd.DataFrame(avgdist_auc), on='iota', how='left')


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################



###############################
# MERGE GEOGRAPHY VARIABLES TO THE IOTA DATASET

#iotas_w_attributes.drop(columns=[ 'std_distance', 'avg_dist_centroid', 'auc_decay'], inplace=True)
iotas_w_attributes = iotas_w_attributes.merge(spatial_var, on='iota', how='left')

factor_rename_map2 = {f"f{i}": factor_info[f"Factor{i}"]["name"] for i in range(1, 17)}

iotas_w_attributes.rename(columns=factor_rename_map2, inplace=True)

#########################
# CORRELATION MATRIX

correlation_matrix = iotas_w_attributes[['avg_dist_centroid', 'auc_decay', 'std_distance', 'weighted_std_distance', 'educ_years', 'ed_noeduc', 'ed_elementary', 'ed_midschool', 'ed_highschool', 'ed_college', 'ed_gradschool', 'ms_degree', 'hs_degree', 'college_degree', 'educ_mean', 'educ_mean_rank', 'mean_monthly_earnings', 'mean_monthly_earnings_rank', 'num_unique_jids', 'num_unique_wids', 'num_unique_wid_jids'] + factor_names].corr()

# Display all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)

# Make a mask for the upper triangle
mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))

# Replace the upper triangle with NaN values
lower_triangle_corr_matrix = correlation_matrix.mask(mask)

#print(lower_triangle_corr_matrix)

lower_triangle_corr_matrix.to_csv(root + '/Results/iota_summary_stats/geo_' + geocode_unit + '/correlation_matrix_lower.csv')


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################



##############################################
# BINNED SCATTER PLOTS

# List of y-variables
y_variables = ['mean_monthly_earnings', 'mean_monthly_earnings_rank','avg_dist_centroid', 'auc_decay', 'std_distance', 'weighted_std_distance']

# List of x-variables
x_variables = ['educ_years', 'ed_noeduc', 'ed_elementary', 'ed_midschool', 'ed_highschool', 'ed_college', 'ed_gradschool', 'educ_mean', 'educ_mean_rank', 'mean_monthly_earnings', 'mean_monthly_earnings_rank', 'num_unique_jids', 'num_unique_wids', 'num_unique_wid_jids', 'ms_degree', 'hs_degree', 'college_degree'] + factor_names

#### USING BINSREG
# Loop over y-variables and x-variables to create binned scatter plots
for y_var in y_variables:
    for x_var in x_variables:
        plt.figure(figsize=(10, 6))
        # Create binned scatter plot with binsreg
        est = binsreg.binsreg(
            y=y_var,
            x=x_var,
            data=iotas_w_attributes,
            nbins=20,      # Number of bins
            line=(2, 2),   # Degree of polynomial for regression and CI/CB
            ci=(2, 2),     # Confidence interval polynomial degree
            dots=(0, 0)    # Include binned observation dots
        )
        # Display the plot
        #plot = est.bins_plot + ggtitle(f'Binned Scatter Plot of {y_var} vs. {x_var}') + theme_bw() + theme(legend_position='none')
        # Save the plot
        plot_filename = f'{y_var}_vs_{x_var}.png'
        plt.savefig(root + 'Results/iota_summary_stats/geo_' + geocode_unit + '/binned_plots/' + plot_filename)
        plt.close()


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################



##############################################
# REGRESSIONS

# output directory
output_dir = root + 'Results/iota_summary_stats/geo_' + geocode_unit + '/regs/'

# List of y-variables
y_variables = ['avg_dist_centroid', 'auc_decay', 'std_distance', 'weighted_std_distance']

# Function to run regressions and collect results
def run_regressions(y_vars, x_vars, data):
    results = []
    for y_var in y_vars:
        # Define the dependent variable
        y = data[y_var]
        # Define the independent variables
        X = data[x_vars]
        X = sm.add_constant(X)  # Adds a constant term to the predictors
        # Fit the regression model
        model = sm.OLS(y, X).fit()
        # Collect the regression summary
        summary = model.summary2().tables[1]
        summary['y_variable'] = y_var
        summary['x_variables'] = summary.index
        summary['R_squared'] = model.rsquared
        results.append(summary)
    # Combine all results into a single DataFrame
    all_results = pd.concat(results)
    return all_results

# Run regressions for the first set of x-variables
x_variables1 = ['mean_monthly_earnings', 'num_unique_jids', 'num_unique_wids', 'ed_noeduc', 'ed_elementary', 'ed_midschool', 'ed_highschool', 'ed_college', 'ed_gradschool']
# Add quadratic terms to the dataset
for var in x_variables1:
    iotas_w_attributes[f'{var}_squared'] = iotas_w_attributes[var] ** 2

# Extended list of x-variables including quadratic terms
x_variables1_extended = x_variables1 + [f'{var}_squared' for var in x_variables1]
results1 = run_regressions(y_variables, x_variables1_extended, iotas_w_attributes)

# Run regressions for the second set of x-variables
x_variables2 = factor_names
results2 = run_regressions(y_variables, x_variables2, iotas_w_attributes)

# Save results to Excel
results1.to_excel(os.path.join(output_dir, 'regression_results_set1.xlsx'), index=False)

results2.to_excel(os.path.join(output_dir, 'regression_results_set2.xlsx'), index=False)


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################



##############################################
# OCCUPATION TABLES

# rescaling iotas
iotas_w_attributes['iota_rescaled'] = iotas_w_attributes['iota'] - iotas_w_attributes['iota'].min() + 1

# load occupation tables per iota
occ_tables = pd.read_csv(root + 'Data/derived/occ_counts/panel_3states_2009to2011_occ_counts_by_i_level_0.csv')

def occ_tables_print(iota_rescaled_values, qty = 5, print_table=True):
    for iota in iota_rescaled_values:
        filtered_occ_tables = occ_tables[occ_tables['iota'] == iota]
        # Step 3: Sort the filtered `occ_tables` DataFrame by `share` column in descending order
        sorted_occ_tables = filtered_occ_tables.sort_values(by='share', ascending=False)
        if print_table:
            # Step 4: Print the top 5 observations
            print(f"Top 5 occupations for iota {iota} sorted by share:")
            print(sorted_occ_tables.head(qty))
            print("\n")
        return sorted_occ_tables.head(qty)

def get_extreme_iotas(var_sort, other_var, ascending=True, iota_var = 'iota_rescaled', qty =5, print_occ_count=False):
    tab = iotas_w_attributes.sort_values(by=var_sort, ascending=ascending)[[iota_var] + other_var].head(qty)
    print(tab)
    if print_occ_count:
        occ_tables_print(tab[iota_var].values, qty = 5)
    return tab[iota_var].values

get_extreme_iotas(var_sort= 'mean_monthly_earnings', other_var = ['modal_occ4','mean_monthly_earnings','std_distance'], ascending=True, iota_var = 'iota_rescaled', qty =5, print_occ_count=True)

get_extreme_iotas(var_sort= 'std_distance', other_var = ['modal_occ4','mean_monthly_earnings','std_distance'], ascending=True, iota_var = 'iota_rescaled', qty =5, print_occ_count=True)

get_extreme_iotas(var_sort= 'educ_years', other_var = ['modal_occ4','mean_monthly_earnings','std_distance'], ascending=True, iota_var = 'iota_rescaled', qty =5, print_occ_count=True)


##########################################
rc+=1
print('AHHHHHHHH WTF = ',rc)
rcounts.append([rc,raw.shape[0]])
##########################################

wtf = pd.DataFrame(rcounts)
print(wtf)

##########################################

import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from sklearn.metrics import pairwise_distances
from geobr import read_state, read_municipality
import matplotlib.gridspec as gridspec
import matplotlib.image as mpimg
from scipy.stats import gaussian_kde

# Load state shapes
year_maps = 2010
rj = read_state(code_state='RJ', year=year_maps)
mg = read_state(code_state='MG', year=year_maps)
sp = read_state(code_state='SP', year=year_maps)
states_gdf = gpd.GeoDataFrame(pd.concat([rj, mg, sp], ignore_index=True))

# Load municipal boundaries for state capitals
sp_city = read_municipality(code_muni=3550308, year=year_maps)  # São Paulo city
rj_city = read_municipality(code_muni=3304557, year=year_maps)  # Rio de Janeiro city
bh_city = read_municipality(code_muni=3106200, year=year_maps)  # Belo Horizonte

capitals_gdf = gpd.GeoDataFrame(pd.concat([sp_city, rj_city, bh_city], ignore_index=True))

# Step 1: Count observations per iota in raw
iota_counts = raw['iota'].value_counts().reset_index()
iota_counts.columns = ['iota', 'num_wid_jids_total']
iotas_w_attributes = pd.merge(iotas_w_attributes, iota_counts, on='iota', how='left')

iotas_w_attributes.to_pickle(root + '/Data/derived/iotas_w_attributes_geo_' + geocode_unit + '.p')
#iotas_w_attributes = pd.read_pickle(root + '/Data/derived/iotas_w_attributes.p')



def spatial_gini(df, value_column, lat_column, lon_column):
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[lon_column], df[lat_column]),
        crs='EPSG:4326'
    )
    gdf = gdf.to_crs(epsg=3857)
    coords = np.array(list(zip(gdf.geometry.x, gdf.geometry.y)))
    values = gdf[value_column].values
    dist_matrix = pairwise_distances(coords)
    weights = values / values.sum()
    n = len(values)
    weights_sum = weights.sum()
    weighted_dists = np.dot(dist_matrix, weights)
    gini = 1 - (2 / (weights_sum * n)) * (weights * weighted_dists).sum()
    return gini

def compute_spatial_gini_for_groups(df, group_column, value_column, lat_column, lon_column):
    gini_results = {}
    groups = df[group_column].unique()
    for group in groups:
        subgroup_df = df[df[group_column] == group]
        gini = spatial_gini(subgroup_df, value_column, lat_column, lon_column)
        gini_results[group] = gini
    return gini_results

def create_kde_heatmap(gdf, lat_column, lon_column, states_gdf, capitals_gdf, ax):
    gdf = gdf.to_crs(epsg=3857)
    states_gdf = states_gdf.to_crs(epsg=3857)
    capitals_gdf = capitals_gdf.to_crs(epsg=3857)
    x = gdf.geometry.x
    y = gdf.geometry.y
    xy = np.vstack([x, y])
    kde = gaussian_kde(xy, bw_method=0.1)
    xmin, ymin, xmax, ymax = states_gdf.total_bounds  # Use states' bounds for extent
    xx, yy = np.mgrid[xmin:xmax:100j, ymin:ymax:100j]
    zz = np.reshape(kde(np.vstack([xx.ravel(), yy.ravel()])), xx.shape)
    #colormaps = ['plasma', 'viridis', 'inferno', 'magma', 'cividis', 'coolwarm', 'Spectral']
    im = ax.imshow(np.rot90(zz), cmap='turbo', extent=[xmin, xmax, ymin, ymax], aspect='auto')
    states_gdf.boundary.plot(ax=ax, linewidth=1, edgecolor='black')
    capitals_gdf.boundary.plot(ax=ax, linewidth=1, edgecolor='red')  # Add capitals boundaries
    cbar = plt.colorbar(im, ax=ax, orientation='vertical', fraction=0.036, pad=0.04)
    cbar.set_label('Density')
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_title('KDE Heatmap of Worker Concentration')

def plot_iota_summary(iota, iotas_w_attributes, ax):
    iota_data = iotas_w_attributes[iotas_w_attributes['iota'] == iota].round(4)
    textstr = '\n'.join([f'{col}: {iota_data[col].values[0]}' for col in iota_data.columns if col != 'iota'])
    ax.text(0.5, 0.5, textstr, fontsize=12, verticalalignment='center', horizontalalignment='center')
    ax.axis('off')
    ax.set_title('Iota Summary', fontweight='bold')

def plot_occ_table(iota, ax, iota_min):
    occ_table = occ_tables_print([iota - iota_min + 1], qty=5, print_table=False)
    occ_table = occ_table[['description', 'share']]
    occ_table['share'] = occ_table['share'].map('{:.4f}'.format)
    table = ax.table(cellText=occ_table.values, colLabels=occ_table.columns, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(12)  # Set font size for the whole table
    table.scale(1.5, 1.5)
    for key, cell in table.get_celld().items():
        cell.set_fontsize(12)  # Set font size for each cell
        if key[0] == 0:
            cell.set_text_props(weight='bold')  # Make header bold
    ax.axis('off')
    ax.set_title('Top Occupations', fontweight='bold')

def plot_png_image(ax, img_path):
    img = mpimg.imread(img_path)
    ax.imshow(img)
    ax.axis('off')

def generate_iota_plots(raw, iotas_w_attributes, group_column, value_column, lat_column, lon_column, output_dir, img_path, capitals_gdf):
    iota_min = iotas_w_attributes['iota'].min()
    gdf = gpd.GeoDataFrame(raw, geometry=gpd.points_from_xy(raw[lon_column], raw[lat_column]), crs='EPSG:4326')
    gini_results = compute_spatial_gini_for_groups(raw, group_column, value_column, lat_column, lon_column)
    for iota in raw[group_column].unique():
        print(iota)
        subgroup_gdf = gdf[gdf[group_column] == iota]
        fig = plt.figure(figsize=(18, 15))  # Adjusted figure height
        gs = gridspec.GridSpec(3, 2, height_ratios=[1.5, 0.6, 1.2], width_ratios=[3, 0.5])  # Adjusted width ratio for summary stats
        ax_map = fig.add_subplot(gs[0, 0])
        create_kde_heatmap(subgroup_gdf, lat_column, lon_column, states_gdf, capitals_gdf, ax_map)
        gini = gini_results[iota]
        ax_map.text(0.95, 0.05, f'Gini Coefficient: {gini:.2f}', fontsize=12, verticalalignment='bottom', horizontalalignment='right', transform=ax_map.transAxes, bbox=dict(facecolor='white', alpha=0.6))
        ax_occ_table = fig.add_subplot(gs[1, 0])
        plot_occ_table(iota, ax_occ_table, iota_min)
        ax_img = fig.add_subplot(gs[2, 0])
        plot_png_image(ax_img, img_path + 'decay_function_iota_' + str(iota) + '.png')
        ax_iota_summary = fig.add_subplot(gs[:, 1])
        plot_iota_summary(iota, iotas_w_attributes, ax_iota_summary)
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(f'{output_dir}/iota_{iota}.png')
        plt.close()

# output directory
output_dir = root + 'Results/iota_summary_stats/geo_' + geocode_unit + '/maps/'
img_path = root + 'Results/iota_summary_stats/geo_' + geocode_unit + '/decay_plots/'

raw['workers'] = 1
# Command to generate the plots
generate_iota_plots(raw, iotas_w_attributes, 'iota', 'workers', 'lat_' + geocode_unit, 'lon_' + geocode_unit, output_dir, img_path, capitals_gdf)
>>>>>>> f00f144e66edbc5fbc2ab73e37667348d1885cac
