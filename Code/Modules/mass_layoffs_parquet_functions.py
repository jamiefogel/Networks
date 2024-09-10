
import pandas as pd
import pyproj # for converting geographic degree measures of lat and lon to the UTM system (i.e. in meters)
from tqdm import tqdm # to calculate progress of some operations for geolocation
import numpy as np

from linearmodels.iv import AbsorbingLS
from linearmodels.panel import PanelOLS
import statsmodels.api as sm
import matplotlib.pyplot as plt

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


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
    #iotas, gammas = load_iotas_gammas(root)
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
  
  
def pull_estab_geos(years, rais):    
    transformer = pyproj.Transformer.from_crs("epsg:4326", "epsg:32723", always_xy=True)
    # Function to convert geographic coordinates to UTM
    def convert_to_utm(lon, lat):
        return transformer.transform(lon, lat)
    
    
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
    return geo_estab


def calculate_distance(row, lat1, lon1, lat2, lon2):
    if np.isnan(row[lat2]) or np.isnan(row[lon2]):
        return np.nan
    else:
        return np.sqrt(
            (row[lat2] - row[lat1])**2 +
            (row[lon2] - row[lon1])**2
        )


def event_studies_by_mkt_size(worker_panel_balanced, y_var, continuous_controls, fixed_effects_cols, 
                              market_size_var, omitted_category, baseline_category = 'low', print_regression=False, savefig=None, title=None, root='//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/'):
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


def merge_aguinaldo_onet(df, aguinaldo_dir):
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
    onet_scores = pd.read_sas(aguinaldo_dir + 'scores_o_br.sas7bdat')
    # Keep only the unique identifier and the factors
    onet_scores = onet_scores[[f'Factor{i}' for i in range(1, 17)] + ['O']]

    # This is a mapping of O*NET occs to Brazilian occ-sector pairs. Some Brazilian occs are mapped to different O*NET occs depending on the sector. Otherwise, this would just be a mapping between Brazilian and O*NET occs.
    spine =  pd.read_sas(aguinaldo_dir + 'cbo2002_cnae20_o.sas7bdat').astype(float)
    
    # Merge the spine of Brazilian occ-sector pairs to the O*NET scores
    onet_merged = onet_scores.merge(spine, on=['O'], how="left")
    
    # Convert columns in onet_merged to string if they are not already
    onet_merged['cbo2002'] = onet_merged['cbo2002'].astype('Int64')
    onet_merged['cla_cnae20'] = onet_merged['cla_cnae20'].astype('Int64')
    
    # Rename columns using Aguinaldo's labels 
    onet_merged.rename(columns=factor_rename_map, inplace=True)

    # Assuming raw and onet_merged are your DataFrames
    merge_keys = {'left': ['cbo2002', 'clas_cnae20'], 'right': ['cbo2002', 'cla_cnae20']}
    
    dups = onet_merged.groupby(['cbo2002', 'cla_cnae20']).size().reset_index(name='dup')
    onet_merged = onet_merged.drop_duplicates(subset=['cbo2002', 'cla_cnae20'])

    df = df.merge(
        onet_merged,
        left_on=merge_keys['left'],
        right_on=merge_keys['right'],
        how='left',
        suffixes=[None, '_y'],
        validate='m:1',
        indicator='_merge_ONET'
    )
    return df
  
  
