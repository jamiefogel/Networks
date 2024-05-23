from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import sys
import gc
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats
import geobr
import cividis
import matplotlib.colors as colors
from haversine import haversine, Unit
from shapely.geometry import MultiPoint
import pyproj # for converting geographic degree measures of lat and lon to the UTM system (i.e. in meters)
from scipy.integrate import simps # to calculate the AUC for the decay function
from sklearn.cluster import KMeans
import binsreg
import statsmodels.api as sm


# DIRECTORY WHERE THE DATA IS
# Automatically chooses the folder between windows vs linux servers
if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')


root = homedir + '/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
os.chdir(root)

import bisbm
from pull_one_year import pull_one_year
from explore_gammas_functions import *
from binscatter import *

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

raw = raw.merge(gammas, how='left', validate='m:1', on='jid',indicator=True)
print('Merge stats for gammas')
print(raw._merge.value_counts())
raw.drop(columns=['_merge'], inplace=True)

raw = raw.merge(iotas, how='left', validate='m:1', on='wid',indicator=True)
print('Merge stats for iotas')
print(raw._merge.value_counts())
raw.drop(columns=['_merge'], inplace=True)

raw = raw.merge(muni_meso_cw, on='codemun')

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

raw.to_pickle(root + '/Data/derived/mkt_geography_raw.p')
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

###########################################
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

###########################################
# GETTING LATITUDE AND LONGITUDE FOR ALL MUNICS IN THE DATA & MERGING THEM BACK TO THE MAIN DATASET called 'raw'

# Pull meso codes for our states of interest. Choosing 2010 b/c this isn't available for all years and 2010 is in the middle of our sample
muni_sp = geobr.read_municipality(code_muni="SP", year=2010)
muni_rj = geobr.read_municipality(code_muni='RJ', year=2010)
muni_mg = geobr.read_municipality(code_muni='MG', year=2010)
munis = pd.concat([muni_sp, muni_rj, muni_mg], ignore_index=True)
munis['lon'] = munis.geometry.centroid.x
munis['lat'] = munis.geometry.centroid.y
munis['codemun'] = munis.code_muni//10

# Converting coordinates to UTM, so the units are in meters
# Function to convert geographic coordinates to UTM using a fixed zone 23S for Sao Paulo
def convert_to_utm_fixed_zone(lon, lat, zone=23):
    proj = pyproj.Proj(proj='utm', zone=zone, south=True if lat < 0 else False, ellps='WGS84')
    utm_lon, utm_lat = proj(lon, lat)
    return utm_lon, utm_lat

# Now we can calculate spatial variance using UTM coordinates
munis['utm_lon'], munis['utm_lat'] = zip(*munis.apply(lambda row: convert_to_utm_fixed_zone(row['lon'], row['lat'], zone=23), axis=1))

raw = raw.merge(munis[['codemun', 'lon', 'lat', 'utm_lon', 'utm_lat']], left_on='codemun',right_on='codemun',how='left', indicator=True)
raw['_merge'].value_counts()
raw.drop(columns=['_merge'], inplace=True)

###########################################
# COMPUTING THE 'ACTUAL/OBSERVED' IOTA SPATIAL VARIANCE IN TWO WAYS, BOTH IN METERS

# METHOD 1 FOR SPATIAL VARIANCE: Function to calculate standard distance for a group
# DEFINITION (this is a 2-D variance):
    # 1) calculates the variance of the points (related to the avg center/centroid)
def standard_distance(group):
    mean_utm_lon = group['utm_lon'].mean()
    mean_utm_lat = group['utm_lat'].mean()
   
    squared_distances = (group['utm_lon'] - mean_utm_lon) ** 2 + (group['utm_lat'] - mean_utm_lat) ** 2
    std_distance = np.sqrt(squared_distances.mean())
   
    return std_distance



# Group by 'iota' and calculate standard distance for each group
spatial_var = raw.groupby('iota').apply(lambda x: pd.Series({
    'std_distance': standard_distance(x)
})).reset_index()

# Display the resulting DataFrame
spatial_var

###########################################
# COMPUTING THE 'POTENTIAL' IOTA SPATIAL VARIANCE, NOW USING POTENTIAL MATCHES BY EXPLORING THE MATCH PROBABILITIES

# Function to calculate weighted standard distance
def weighted_standard_distance(df, weights):
    mean_utm_lon = np.average(df['utm_lon'], weights=weights)
    mean_utm_lat = np.average(df['utm_lat'], weights=weights)
   
    squared_distances = weights * ((df['utm_lon'] - mean_utm_lon) ** 2 + (df['utm_lat'] - mean_utm_lat) ** 2)
    std_distance = np.sqrt(squared_distances.sum() / weights.sum())
   
    return std_distance

# List to store results
weighted_spatial_var = []
weights_dict = {iota: p_gi_ji.loc[iota].reset_index().rename(columns={iota: 'weight', 'index': 'gamma'}) for iota in p_gi_ji.index}

# Loop over each iota category
count = 0
tot = len(weights_dict)
for iota, weights in weights_dict.items():
    # Merge the weights directly with raw based on gamma
    merged_df = raw[['utm_lat', 'utm_lon', 'gamma']].merge(weights, on='gamma', how='left')
    merged_df['weight'].fillna(0, inplace=True)
    #print(merged_df.weight.sum())
       
    # Use the merged variable as weights for the weighted spatial variance calculation
    std_distance = weighted_standard_distance(merged_df, merged_df['weight'])
   
    # Store results
    weighted_spatial_var.append({'iota': iota, 'weighted_std_distance': std_distance})
    count +=1
    print(str(count) + '/' + str(tot))

del merged_df

# Merge the final results with spatial_metrics_df on the 'iota' column
spatial_var = spatial_var.merge(pd.DataFrame(weighted_spatial_var), on='iota', how='left')


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


#cols = [c for c in onet_merged.columns if c[0] == '_']
#X = onet_merged[cols]
#kmeans = KMeans(n_clusters=290, random_state=0).fit(X)
#labels  = onet_merged[['cbo2002','cla_cnae20']].rename(columns={'cla_cnae20':'clas_cnae20'})
#labels['kmeans'] = kmeans.labels_ + 1

#pickle.dump(kmeans, open('kmeans.p', 'wb'))
#kmeans = pickle.load(open('kmeans.p', 'rb'))

# Convert columns in raw to string if they are not already
raw['cbo2002'] = raw['cbo2002'].astype('Int64')
raw['clas_cnae20'] = raw['clas_cnae20'].astype('Int64')

# Convert columns in onet_merged to string if they are not already
onet_merged['cbo2002'] = onet_merged['cbo2002'].astype('Int64')
onet_merged['cla_cnae20'] = onet_merged['cla_cnae20'].astype('Int64')


raw = raw.merge(onet_merged[['cbo2002','cla_cnae20',  'Factor1', 'Factor2', 'Factor3', 'Factor4', 'Factor5', 'Factor6', 'Factor7', 'Factor8', 'Factor9', 'Factor10', 'Factor11', 'Factor12', 'Factor13', 'Factor14', 'Factor15', 'Factor16']], left_on=['cbo2002','clas_cnae20'], right_on=['cbo2002','cla_cnae20'], how='left', suffixes=[None, '_y'], indicator=True)
raw._merge.value_counts()


# missing 1 and 71, 115 only appears once. Look at temp.csv for more details
# data_full.kmeans.value_counts(dropna=False).to_csv('temp.csv')
# labels.loc[labels['kmeans']==1]['cbo2002'].unique()
# labels.loc[labels['kmeans']==71]['cbo2002'].unique()
# labels.loc[labels['kmeans']==115]['cbo2002'].unique()
# data_full.kmeans.value_counts(dropna=False)

#kmeans_count = data_full.groupby('kmeans')['kmeans'].transform('count')
#data_full['kmeans'].loc[kmeans_count<5000] = np.nan
#data_full['kmeans'] = data_full.kmeans.rank(method='dense', na_option='keep')
#data_full['kmeans'].loc[np.isnan(data_full.kmeans)] = -1
#data_full['kmeans']  = data_full['kmeans'].astype(int)

#data_full.to_csv( root + 'Data/RAIS_exports/earnings_panel/panel_rio_2009_2012_w_kmeans.csv')
# The issue here is that there's one kmeans group that isn't present in all years so that throws things off.




################################################
### BRINGING OTHER COVARIATES TO CORRELATE WITH SPATIAL DISTANCE STATISTICS

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
    educ_median=('grau_instr', 'median'),
    educ_mean=('grau_instr', 'mean'),
    mean_monthly_earnings=('rem_med_r', 'mean'),
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
num_unique_wid_jids = raw.groupby('iota').apply(lambda x: x[['jid', 'wid']].drop_duplicates().shape[0]).astype(int).reset_index().rename(columns={0:'num_unique_wid_jids'})

# Merge on iota characteristics
for var in [num_unique_jids, num_unique_wids, num_unique_wid_jids]:
    iotas_w_attributes = iotas_w_attributes.merge(var, on='iota', validate='1:1')


##############################################
# COMPUTING THE MATCHING DECAY FUNCTIONS

# Step 1: Compute the centroid of observations for a given iota
def compute_centroid(group):
    mean_utm_lon = group['utm_lon'].mean()
    mean_utm_lat = group['utm_lat'].mean()
    return mean_utm_lon, mean_utm_lat

# Step 2: Compute distances from centroid
def compute_distances(group, centroid_lon, centroid_lat):
    distances = np.sqrt((group['utm_lon'] - centroid_lon) ** 2 + (group['utm_lat'] - centroid_lat) ** 2)
    return distances

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
distance_thresholds = [threshold*1000 for threshold in distance_thresholds_km[1:]]

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
    proportions = [1] + compute_proportions(distances, distance_thresholds)
   
    # Compute the area under the curve
    auc = compute_auc(proportions, distance_thresholds_km)
    avgdist_auc.append({'iota': iota, 'avg_dist_centroid': distances.mean(), 'auc_decay': auc})
   
    # Step 5: Create a line graph with the decay function
    plt.figure()
    plt.plot(distance_thresholds_km, proportions, marker='o')
    plt.title(f'Decay Function for Iota {iota}')
    plt.xlabel('Distance (km)')
    plt.ylabel('Proportion of Observations')
    #plt.xscale('log')
    plt.grid(True, which='both', color='lightgray', linestyle='--', linewidth=0.5)
    #plt.xticks(distance_thresholds_km, [str(threshold) for threshold in distance_thresholds] + ['Above 500'])
   
    # Save the plot
    plt.savefig(os.path.join(root + '/Results/iota_summary_stats/decay_plots', f'decay_function_iota_{iota}.png'))
    plt.close()

# Merge the final results with spatial_metrics_df on the 'iota' column
spatial_var = spatial_var.merge(pd.DataFrame(avgdist_auc), on='iota', how='left')


###############################
# MERGE GEOGRAPHY VARIABLES TO THE IOTA DATASET

#iotas_w_attributes.drop(columns=[ 'std_distance', 'avg_dist_centroid', 'auc_decay'], inplace=True)
iotas_w_attributes = iotas_w_attributes.merge(spatial_var, on='iota', how='left')

factor_rename_map2 = {f"f{i}": factor_info[f"Factor{i}"]["name"] for i in range(1, 17)}

iotas_w_attributes.rename(columns=factor_rename_map2, inplace=True)

#########################
# CORRELATION MATRIX

correlation_matrix = iotas_w_attributes[['avg_dist_centroid', 'auc_decay', 'std_distance', 'weighted_std_distance', 'ms_degree', 'hs_degree', 'college_degree', 'educ_mean', 'educ_mean_rank', 'mean_monthly_earnings', 'mean_monthly_earnings_rank', 'num_unique_jids', 'num_unique_wids', 'num_unique_wid_jids'] + factor_names].corr()

# Display all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)

# Make a mask for the upper triangle
mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))

# Replace the upper triangle with NaN values
lower_triangle_corr_matrix = correlation_matrix.mask(mask)

print(lower_triangle_corr_matrix)

lower_triangle_corr_matrix.to_csv(root + '/Results/iota_summary_stats/correlation_matrix_lower.csv')

##############################################
# BINNED SCATTER PLOTS

# List of y-variables
y_variables = ['mean_monthly_earnings', 'mean_monthly_earnings_rank','avg_dist_centroid', 'auc_decay', 'std_distance', 'weighted_std_distance']

# List of x-variables
x_variables = ['educ_mean', 'educ_mean_rank', 'mean_monthly_earnings', 'mean_monthly_earnings_rank', 'num_unique_jids', 'num_unique_wids', 'num_unique_wid_jids', 'ms_degree', 'hs_degree', 'college_degree'] + factor_names

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
            line=(3, 3),   # Degree of polynomial for regression and CI/CB
            ci=(3, 3),     # Confidence interval polynomial degree
            dots=(0, 0)    # Include binned observation dots
        )
        # Display the plot
        #plot = est.bins_plot + ggtitle(f'Binned Scatter Plot of {y_var} vs. {x_var}') + theme_bw() + theme(legend_position='none')
        # Save the plot
        plot_filename = f'{y_var}_vs_{x_var}.png'
        plt.savefig(root + 'Results/iota_summary_stats/binned_plots/' + plot_filename)
        plt.close()



##############################################
# REGRESSIONS

# output directory
output_dir = root + 'Results/iota_summary_stats/regs/'

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
x_variables1 = ['mean_monthly_earnings', 'num_unique_jids', 'num_unique_wids', 'ms_degree', 'hs_degree', 'college_degree']
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


##############################################

raw.to_pickle(root + '/Data/derived/mkt_geography_raw_final.p')
#raw = pd.read_pickle(root + '/Data/derived/mkt_geography_raw_final.p')
iotas_w_attributes.to_pickle(root + '/Data/derived/iotas_w_attributes.p')
#iotas_w_attributes = pd.read_pickle(root + '/Data/derived/iotas_w_attributes.p')

## NEXT STEPS - May 21st 2024
# revise the education variable in order to capture college degree and HS graduate separately, maybe consider years as well
# explore regressions of spatial variance on education controlling for other variables
# produce binscatter with spatial variance and education (binsreg)
# O*NET: e.g. are markets with higher manual more or less spread out? (check it on GitHub, search for factor 1 through 16)
  # https://github.com/jamiefogel/Networks/blob/61cf62ded22e7722367639237900d511a3a1ea17/Code/process_brazil_onet.py#L23



#### NOTES FROM EMAIL
 # Compute the spatial variance across jobs for each worker type. Correlate it with other things like earnings and worker types.
   #   .. Let's make a heatmap of the spatial distribution for each iota (start with a few that are particularly interesting/emblematic of the points we want to make)
 # If there is a strong relationship between spatial variance and earnings, do a scatter plot, highlight a few dots, and then show the heatmaps for those worker types
 # In addition to computing the spatial variance of the labor market for each worker, we can also compute the function for how match probabilities decay with distance. The challenge with this is that we don't actually know the worker's location but we hopefully will have this in a few weeks once Rafa and team get it geocoded.

## NEXT STEPS - May 17th 2024
# graph with decay and avg decay per iota merged back into the
# run with all years together
# drop if iota-gamma missing (it seems a lot)