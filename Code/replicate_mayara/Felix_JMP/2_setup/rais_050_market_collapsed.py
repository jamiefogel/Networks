import pandas as pd
import numpy as np
import os
from config import root

# Paths
base_path = root + "/Code/replicate_mayara"
monopsas_path = f"{base_path}/monopsonies/sas"

# Define paths
DICT_PATH = f'{base_path}/raisdictionaries/harmonized'
DEIDRAIS_PATH = f'{base_path}/raisdeidentified/dta/20191213'
MONOPASAS_PATH = f'{base_path}/monopsonies/sas'
PUBLIC_PATH = f'{base_path}/publicdata'
EXPORT_PATH = f'{base_path}/monopsonies/sas'

# Define base year
BASEYEAR = 1991
BASEYEAR_O1 = BASEYEAR + 3

# Function to load data
def load_data(filepath, format='dta'):
    if format == 'dta':
        return pd.read_stata(filepath, convert_categoricals=False)
    else:
        raise ValueError("Unsupported file format")

def flag_firms(importers_exporters_df, firm_type):
    """
    Flags firms as exporters or importers.

    Parameters:
    - importers_exporters_df: DataFrame containing exporter/importer information.
    - firm_type: 'exporter' or 'importer'

    Returns:
    - lib_df: Firms flagged as exporters/importers within the base year.
    - any_df: Firms flagged as exporters/importers overall.
    """
    # Filter for the specific type
    type_df = importers_exporters_df[importers_exporters_df[firm_type] == 1]

    # lib&type: within baseyear to baseyear_o1
    lib_df = type_df[
        (type_df['year'] >= BASEYEAR) & 
        (type_df['year'] <= BASEYEAR_O1)
    ][['fakeid_firm', firm_type]].drop_duplicates()

    # any&type: overall
    any_df = type_df[['fakeid_firm', firm_type]].drop_duplicates()

    return lib_df, any_df

def process_level(level2, libexporter, anyexporter, libimporter, anyimporter):
    """
    Processes data for a given level2 ('none' or 'cbo942d').

    Parameters:
    - level2: 'none' or 'cbo942d'
    - libexporter, anyexporter, libimporter, anyimporter: DataFrames with exporter/importer flags.

    Returns:
    - regsfile_mmc: Final regression dataset for the specified level2.
    """

    # 1. Import rais_collapsed_firm_mmc_&level2..dta
    rais_filepath = os.path.join(MONOPASAS_PATH, f'rais_collapsed_firm_mmc_{level2}.dta')
    rais_df = load_data(rais_filepath)

    # 2. Compute theta&level2
    crosswalk_ibgesubsector_indmatch = load_data(os.path.join(MONOPASAS_PATH, 'crosswalk_ibgesubsector_indmatch.dta'))
    theta_indmatch_df = pd.read_parquet(os.path.join(MONOPASAS_PATH, 'theta_indmatch.parquet'))

    # Merge crosswalk with theta_indmatch
    theta_merged = pd.merge(crosswalk_ibgesubsector_indmatch, theta_indmatch_df, on='indmatch', how='inner')

    # Merge with rais_df on ibgesubsector
    merged_theta = pd.merge(theta_merged, rais_df, on='ibgesubsector', how='inner')

    # Filter for baseyear and emp > 0
    merged_theta = merged_theta[
        (merged_theta['year'] == BASEYEAR) &
        (merged_theta['emp'] > 0)
    ]

    # Compute theta as weighted sum
    theta = merged_theta.groupby('ibgesubsector').apply(
        lambda x: np.sum(x['theta'] * x['emp']) / np.sum(x['emp'])
    ).reset_index(name='theta')

    # 3. Compute shares
    # Merge theta with rais_df
    rais_theta = pd.merge(rais_df, theta, on='ibgesubsector', how='inner')

    # Filter for baseyear and emp > 0
    rais_theta = rais_theta[
        (rais_theta['year'] == BASEYEAR) &
        (rais_theta['emp'] > 0)
    ]

    # Calculate earndshare, earndshare2, empshare
    rais_theta['earndshare'] = rais_theta.groupby(['mmc', level2])['totdecearnmw'].transform(lambda x: x / x.sum())
    rais_theta['earndshare2'] = rais_theta['earndshare'] ** 2
    rais_theta['empshare'] = rais_theta.groupby(['mmc', level2])['emp'].transform(lambda x: x / x.sum())

    # Select necessary columns
    all_shares = rais_theta[['fakeid_firm', 'ibgesubsector', 'mmc', level2, 'earndshare', 'earndshare2', 'empshare']].drop_duplicates()

    # 4. Identify Tradable Firms
    cnae95_tariff_df = load_data(os.path.join(MONOPASAS_PATH, 'cnae95_tariff_changes_1990_1994.dta'))

    # Merge rais_df with tariff changes on cnae95
    tradables = pd.merge(rais_df, cnae95_tariff_df, on='cnae95', how='inner')

    # Filter for baseyear and non-null chng19941990TRAINS
    tradables = tradables[
        (tradables['year'] == BASEYEAR) &
        (tradables['emp'] > 0) &
        (tradables['chng19941990TRAINS'].notnull())
    ]

    # Compute Tearndshare
    tradables['Tearndshare'] = tradables.groupby(['mmc', level2])['totdecearnmw'].transform(lambda x: x / x.sum())

    # Select necessary columns
    tradables_shares = tradables[['fakeid_firm', 'mmc', level2, 'Tearndshare', 'chng19941990TRAINS', 'chng19941990ErpTRAINS', 'chng19941990Kume']].drop_duplicates()

    # 5. Calculate Beta Values
    # Merge all_shares with theta
    beta_df = pd.merge(all_shares, theta, on='ibgesubsector', how='inner')

    # Calculate betadw_rf and beta_rf
    beta_df['betadw_rf'] = (beta_df['earndshare'] / beta_df['theta'])
    beta_df['beta_rf'] = (beta_df['empshare'] / beta_df['theta'])

    # Normalize within group
    beta_df['betadw_rf'] = beta_df.groupby(['mmc', level2])['betadw_rf'].transform(lambda x: x / x.sum())
    beta_df['beta_rf'] = beta_df.groupby(['mmc', level2])['beta_rf'].transform(lambda x: x / x.sum())

    # Select necessary columns
    beta_rf = beta_df[['fakeid_firm', 'mmc', level2, 'betadw_rf', 'beta_rf']].drop_duplicates()

    # 6. Summing Shares
    # Merge all_shares with tradables_shares on fakeid_firm, mmc, level2
    sums_merged = pd.merge(all_shares, tradables_shares, on=['fakeid_firm', 'mmc', level2], how='inner')

    # Group by mmc and level2 to sum the shares
    sums_grouped = sums_merged.groupby(['mmc', level2]).agg(
        sum_empshare=('empshare', 'sum'),
        sum_earndshare=('earndshare', 'sum'),
        sum_earndshare2=('earndshare2', 'sum'),
        sum_Tearndshare=('Tearndshare', 'sum')
    ).reset_index()

    # 7. Computing ICE Shocks
    # Merge all_shares, tradables_shares, beta_rf, and sums_grouped
    ice_merged = pd.merge(all_shares, tradables_shares, on=['fakeid_firm', 'mmc', level2], how='inner')
    ice_merged = pd.merge(ice_merged, beta_rf, on=['fakeid_firm', 'mmc', level2], how='inner')
    ice_merged = pd.merge(ice_merged, sums_grouped, on=['mmc', level2], how='inner')

    # Calculate ICE shock variables
    ice_merged['ice_bdwTRAINS'] = ice_merged['betadw_rf'] * ice_merged['chng19941990TRAINS']
    ice_merged['ice_bTRAINS'] = ice_merged['beta_rf'] * ice_merged['chng19941990TRAINS']
    ice_merged['ice_dwTRAINS'] = (ice_merged['earndshare'] / ice_merged['sum_earndshare']) * ice_merged['chng19941990TRAINS']
    ice_merged['ice_TRAINS'] = (ice_merged['empshare'] / ice_merged['sum_empshare']) * ice_merged['chng19941990TRAINS']
    ice_merged['ice_dwErpTRAINS'] = (ice_merged['earndshare'] / ice_merged['sum_earndshare']) * ice_merged['chng19941990ErpTRAINS']
    ice_merged['ice_dwKume'] = (ice_merged['earndshare'] / ice_merged['sum_earndshare']) * ice_merged['chng19941990Kume']
    ice_merged['ice_dwTRAINS_Hf'] = (ice_merged['earndshare2'] / ice_merged['sum_earndshare2']) * ice_merged['chng19941990TRAINS']
    ice_merged['iceT_dwTRAINS'] = (ice_merged['Tearndshare'] / ice_merged['sum_Tearndshare']) * ice_merged['chng19941990TRAINS']

    # Aggregate ICE shocks by mmc and level2
    ice_shocks = ice_merged.groupby(['mmc', level2]).agg(
        ice_bdwTRAINS=('ice_bdwTRAINS', 'sum'),
        ice_bTRAINS=('ice_bTRAINS', 'sum'),
        ice_dwTRAINS=('ice_dwTRAINS', 'sum'),
        ice_TRAINS=('ice_TRAINS', 'sum'),
        ice_dwErpTRAINS=('ice_dwErpTRAINS', 'sum'),
        ice_dwKume=('ice_dwKume', 'sum'),
        ice_dwTRAINS_Hf=('ice_dwTRAINS_Hf', 'sum'),
        iceT_dwTRAINS=('iceT_dwTRAINS', 'sum')
    ).reset_index()

    # 8. Cleaning Up Intermediate Variables
    del theta, rais_theta, all_shares, tradables_shares, beta_df, sums_merged, sums_grouped, ice_merged

    # 9. Merging Exporters and Importers Information
    # Load crosswalk_cnae95_ibgesubsector
    crosswalk_cnae95_ibgesubsector = load_data(os.path.join(MONOPASAS_PATH, 'crosswalk_cnae95_ibgesubsector.dta'))

    # Merge rais_df with crosswalk
    shares_df = pd.merge(rais_df, crosswalk_cnae95_ibgesubsector, on='cnae95', how='inner')

    # Filter for emp > 0 and not null
    shares_df = shares_df[shares_df['emp'] > 0].copy()

    # Merge with exporter/importer flags
    shares_df = pd.merge(shares_df, libexporter, on='fakeid_firm', how='left')
    shares_df = pd.merge(shares_df, anyexporter, on='fakeid_firm', how='left')
    shares_df = pd.merge(shares_df, libimporter, on='fakeid_firm', how='left')
    shares_df = pd.merge(shares_df, anyimporter, on='fakeid_firm', how='left')

    # Replace NaN exporter/importer flags with 0
    shares_df['exporter'] = shares_df['exporter'].fillna(0)
    shares_df['importer'] = shares_df['importer'].fillna(0)

    # Calculate additional share variables
    shares_df['temp'] = shares_df['emp'] * ((shares_df['ibgesubsector'] <= 14) | (shares_df['ibgesubsector'] == 25))
    shares_df['empshare'] = shares_df.groupby(['mmc', level2])['emp'].transform(lambda x: x / x.sum())
    shares_df['Tempshare'] = shares_df['temp'] / shares_df.groupby(['mmc', level2])['temp'].transform('sum')
    shares_df['NTempshare'] = (shares_df['emp'] * ((shares_df['ibgesubsector'] >= 13) & (shares_df['ibgesubsector'] <= 23))) / shares_df.groupby(['mmc', level2])['emp'].transform('sum')
    shares_df['TNEXPempshare'] = (shares_df['emp'] * (~shares_df['exporter'].astype(bool)) * ((shares_df['ibgesubsector'] <= 14) | (shares_df['ibgesubsector'] == 25))) / shares_df.groupby(['mmc', level2])['emp'].transform('sum')
    shares_df['EXPempshare'] = (shares_df['emp'] * (shares_df['exporter'] == 1)) / shares_df.groupby(['mmc', level2])['emp'].transform('sum')
    shares_df['earndshare'] = shares_df['totdecearnmw'] / shares_df.groupby(['mmc', level2])['totdecearnmw'].transform('sum')
    shares_df['Tearndshare'] = (shares_df['totdecearnmw'] * ((shares_df['ibgesubsector'] <= 14) | (shares_df['ibgesubsector'] == 25))) / shares_df.groupby(['mmc', level2])['totdecearnmw'].transform('sum')
    shares_df['NTearndshare'] = (shares_df['totdecearnmw'] * ((shares_df['ibgesubsector'] >= 13) & (shares_df['ibgesubsector'] <= 23))) / shares_df.groupby(['mmc', level2])['totdecearnmw'].transform('sum')
    shares_df['EXPearndshare'] = (shares_df['totdecearnmw'] * (shares_df['exporter'] == 1)) / shares_df.groupby(['mmc', level2])['totdecearnmw'].transform('sum')
    shares_df['TNEXPearndshare'] = (shares_df['totdecearnmw'] * (~shares_df['exporter'].astype(bool)) * ((shares_df['ibgesubsector'] <= 14) | (shares_df['ibgesubsector'] == 25))) / shares_df.groupby(['mmc', level2])['totdecearnmw'].transform('sum')

    # Rename exporter flags for clarity
    shares_df.rename(columns={'exporter_x': 'explib', 'exporter_y': 'expany'}, inplace=True)

    # 10. Calculating Employment Percentiles
    # Filter for baseyear
    base_df = shares_df[shares_df['year'] == BASEYEAR].copy()

    # Calculate percentiles within mmc and level2
    percentiles = base_df.groupby(['mmc', level2])['emp'].quantile([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]).unstack(level=-1).reset_index()
    percentiles.columns = ['mmc', level2, 'p5', 'p10', 'p25', 'p50', 'p75', 'p90', 'p95']

    # Export percentiles
    percentiles.to_stata(os.path.join(EXPORT_PATH, f'regsfile_mmc_{level2}_{BASEYEAR}_emp_pctiles.dta'), write_index=False)

    # 11. Calculating Percentiles for Tradable Firms Only
    tradable_df = shares_df[
        (shares_df['year'] == BASEYEAR) &
        ((shares_df['ibgesubsector'] <= 14) | (shares_df['ibgesubsector'] == 25))
    ]

    tradable_percentiles = tradable_df.groupby(['mmc', level2])['emp'].quantile([0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]).unstack(level=-1).reset_index()
    tradable_percentiles.columns = ['mmc', level2, 'p5', 'p10', 'p25', 'p50', 'p75', 'p90', 'p95']

    # Export tradable percentiles
    tradable_percentiles.to_stata(os.path.join(EXPORT_PATH, f'regsfile_mmc_{level2}_{BASEYEAR}_empT_pctiles.dta'), write_index=False)

    # 12. Tagging Firms Based on Percentiles
    # Merge base_df with percentiles
    tags_df = pd.merge(base_df, percentiles, on=['mmc', level2], how='left')
    tags_df = pd.merge(tags_df, tradable_percentiles, on=['mmc', level2], how='left', suffixes=('', '_T'))

    # Create tags
    tags_df['top10_1991'] = (tags_df['emp'] >= tags_df['p90']).astype(int)
    tags_df['bot90_1991'] = (tags_df['emp'] < tags_df['p90']).astype(int)
    tags_df['top5_1991'] = (tags_df['emp'] >= tags_df['p95']).astype(int)
    tags_df['bot95_1991'] = (tags_df['emp'] < tags_df['p95']).astype(int)
    tags_df['bot25_1991'] = (tags_df['emp'] <= tags_df['p25']).astype(int)
    tags_df['mid2550_1991'] = ((tags_df['emp'] >= tags_df['p25']) & (tags_df['emp'] <= tags_df['p50'])).astype(int)
    tags_df['mid5075_1991'] = ((tags_df['emp'] >= tags_df['p50']) & (tags_df['emp'] <= tags_df['p75'])).astype(int)
    tags_df['top25_1991'] = (tags_df['emp'] >= tags_df['p75']).astype(int)
    tags_df['gt50_1991'] = (tags_df['emp'] > 50).astype(int)
    tags_df['gt100_1991'] = (tags_df['emp'] > 100).astype(int)
    tags_df['gt1000_1991'] = (tags_df['emp'] > 1000).astype(int)
    tags_df['lt50_1991'] = (tags_df['emp'] <= 50).astype(int)
    tags_df['lt100_1991'] = (tags_df['emp'] <= 100).astype(int)
    tags_df['lt1000_1991'] = (tags_df['emp'] <= 1000).astype(int)

    # Tradable firm tags
    tags_df['top10_T_1991'] = ((tags_df['emp'] >= tags_df['p90_T']) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['bot90_T_1991'] = ((tags_df['emp'] < tags_df['p90_T']) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['top5_T_1991'] = ((tags_df['emp'] >= tags_df['p95_T']) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['bot95_T_1991'] = ((tags_df['emp'] < tags_df['p95_T']) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['bot25_T_1991'] = ((tags_df['emp'] <= tags_df['p25_T']) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['mid2550_T_1991'] = (((tags_df['emp'] >= tags_df['p25_T']) & (tags_df['emp'] <= tags_df['p50_T'])) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['mid5075_T_1991'] = (((tags_df['emp'] >= tags_df['p50_T']) & (tags_df['emp'] <= tags_df['p75_T'])) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['top25_T_1991'] = ((tags_df['emp'] >= tags_df['p75_T']) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['gt50_T_1991'] = ((tags_df['emp'] > 50) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['gt100_T_1991'] = ((tags_df['emp'] > 100) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['gt1000_T_1991'] = ((tags_df['emp'] > 1000) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['lt50_T_1991'] = ((tags_df['emp'] <= 50) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['lt100_T_1991'] = ((tags_df['emp'] <= 100) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)
    tags_df['lt1000_T_1991'] = ((tags_df['emp'] <= 1000) & ((tags_df['ibgesubsector'] <= 14) | (tags_df['ibgesubsector'] == 25))).astype(int)

    # 13. Creating Outcomes Dataset
    # Aggregate market-level outcomes
    def weighted_avg(x, values, weights):
        return np.average(x[values], weights=x[weights])

    mktout = shares_df.groupby(['mmc', level2, 'year']).agg(
        mkt_temp=('temp', 'sum'),
        mkt_emp=('emp', 'sum'),
        mkt_avgdearn=('avgdecearn', lambda x: np.average(x, weights=shares_df.loc[x.index, 'emp'])),
        mkt_wdbill=('totdecearnmw', 'sum'),
        avg_firmemp=('emp', 'mean'),
        hf_emp=('empshare', lambda x: np.sum(x**2)),
        hf_wdbill=('earndshare', lambda x: np.sum(x**2)),
        hf_Temp=('Tempshare', lambda x: np.sum(x**2)),
        hf_Twdbill=('Tearndshare', lambda x: np.sum(x**2)),
        hf_NTemp=('NTempshare', lambda x: np.sum(x**2)),
        hf_NTwdbill=('NTearndshare', lambda x: np.sum(x**2)),
        hf_EXPemp=('EXPempshare', lambda x: np.sum(x**2)),
        hf_EXPwdbill=('EXPearndshare', lambda x: np.sum(x**2)),
        hf_TNEXPemp=('TNEXPempshare', lambda x: np.sum(x**2)),
        hf_TNEXPwdbill=('TNEXPearndshare', lambda x: np.sum(x**2)),
        mkt_firms=('fakeid_firm', 'nunique'),
        expany_firms=('expany', 'sum'),
        explib_firms=('explib', 'sum'),
        expany_emp=('emp', lambda x: x[tags_df.loc[x.index, 'expany'] == 1].sum()),
        explib_emp=('emp', lambda x: x[tags_df.loc[x.index, 'explib'] == 1].sum()),
        top5_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'top5_1991'] == 1].sum()),
        bot95_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'bot95_1991'] == 1].sum()),
        top10_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'top10_1991'] == 1].sum()),
        bot90_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'bot90_1991'] == 1].sum()),
        top25_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'top25_1991'] == 1].sum()),
        mid5075_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'mid5075_1991'] == 1].sum()),
        mid2550_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'mid2550_1991'] == 1].sum()),
        bot25_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'bot25_1991'] == 1].sum()),
        gt50_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'gt50_1991'] == 1].sum()),
        gt100_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'gt100_1991'] == 1].sum()),
        gt1000_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'gt1000_1991'] == 1].sum()),
        lt50_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'lt50_1991'] == 1].sum()),
        lt100_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'lt100_1991'] == 1].sum()),
        lt1000_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'lt1000_1991'] == 1].sum()),
        # Similar aggregation for tradable firm tags
        top5_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'top5_T_1991'] == 1].sum()),
        bot95_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'bot95_T_1991'] == 1].sum()),
        top10_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'top10_T_1991'] == 1].sum()),
        bot90_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'bot90_T_1991'] == 1].sum()),
        top25_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'top25_T_1991'] == 1].sum()),
        mid5075_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'mid5075_T_1991'] == 1].sum()),
        mid2550_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'mid2550_T_1991'] == 1].sum()),
        bot25_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'bot25_T_1991'] == 1].sum()),
        gt50_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'gt50_T_1991'] == 1].sum()),
        gt100_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'gt100_T_1991'] == 1].sum()),
        gt1000_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'gt1000_T_1991'] == 1].sum()),
        lt50_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'lt50_T_1991'] == 1].sum()),
        lt100_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'lt100_T_1991'] == 1].sum()),
        lt1000_T_1991_emp=('emp', lambda x: x[tags_df.loc[x.index, 'lt1000_T_1991'] == 1].sum())
    ).reset_index()

    # 14. Preparing Regression Dataset
    # Merge mktout with ice_shocks
    regsfile_mmc = pd.merge(mktout, ice_shocks, on=['mmc', level2], how='left')

    # Export the regression dataset
    regsfile_mmc.to_stata(os.path.join(EXPORT_PATH, f'regsfile_mmc_{level2}.dta'), write_index=False)

    return regsfile_mmc

def main():
    # Load importers and exporters data
    importers_exporters_filepath = os.path.join(MONOPASAS_PATH, 'importers_exporters.dta')
    importers_exporters_df = load_data(importers_exporters_filepath)

    # Flag exporters and importers
    libexporter, anyexporter = flag_firms(importers_exporters_df, 'exporter')
    libimporter, anyimporter = flag_firms(importers_exporters_df, 'importer')

    # Process for both levels
    for level in ['none', 'cbo942d']:
        print(f"Processing level2={level}...")
        regsfile_mmc = process_level(level, libexporter, anyexporter, libimporter, anyimporter)
        print(f"Completed processing for level2={level}.")

    # Final cleanup can be handled by Python's garbage collector
    print("Data processing completed successfully.")

if __name__ == "__main__":
    main()
