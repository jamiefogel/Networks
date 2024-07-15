#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  1 13:53:05 2023

@author: jfogel
"""


import os
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import sys
import matplotlib.pyplot as plt
import getpass
from scipy.sparse import lil_matrix
from scipy.sparse import coo_matrix
from scipy.sparse import csr_matrix
import statsmodels.formula.api as smf
import statsmodels.formula.api as smf
import statsmodels.api as sm
from linearmodels.panel import PanelOLS
from linearmodels.panel import compare
from scipy import stats


run_firm_cnae_pull = True
run_worker_pull = True

fyear = 1991
lyear = 1997

homedir = os.path.expanduser('~')
if getpass.getuser()=='p13861161':
    if os.name == 'nt':
        homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
        root = homedir + '/labormkt_rafaelpereira/NetworksGit/'
        rais = "//storage6/bases/DADOS/RESTRITO/RAIS/"
        print("Running on IPEA Windows")
    else:
        root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
elif getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'

sys.path.append(root + 'Code/Modules')
figuredir = root + 'Results/'




# Pull region codes
region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_micro_cw = pd.DataFrame({'code_micro':region_codes.code_micro,'codemun':region_codes.code_munic//10})

manaus_codemun_list = [130260, 130250, 130255, 130185, 130030, 130110, 130115, 230395, 230945, 230195]


# Load Mayara's aggregation of cbo1994 into Occ2s
cbo1994_occ2_cw = pd.read_excel(root + 'Code/MarketPower/valid_cbo94.xlsx', sheet_name='fix_plus_ggregation', engine='openpyxl')
cbo1994_occ2_cw = cbo1994_occ2_cw.drop(columns='cbo942d').rename(columns={'aggregate':'occ2'})

######################################################
# Pull worker-level data
    
if run_worker_pull==True:    
    usecols = ['pis', 'emp_31dez', 'grau_instr', 'genero', 'cnpj_raiz', 'codemun', 'subs_ibge', 'subativ_ibge']
    #'rem_dez_r', 'idade',, 'rem_dez_sm'
    nrows=None
    dfs = []
    # range(fyear, 2000)
    for year in [1991, 1997]: #range(1990,1997+1)
        cols = usecols.copy()
        print(year)
        if year>2002:
            cols.append('cbo2002')
        else:
            cols.append('cbo1994')
        if year>1993:
            cols.append('idade')
            cols.append('rem_dez_r')
            cols.append('rem_dez_sm')
            cols.append('tipo_vinculo')
        else:
            cols.append('fx_etaria')
            cols.append('rem_dez_sm')
            cols.append('nat_vinculo')
        now = datetime.now()
        currenttime = now.strftime('%H:%M:%S')
        print('Starting ', year, ' at ', currenttime)
        if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
            sep = ';'
        else:
            sep = ','
        filename = rais + 'csv/brasil' + str(year) + '.csv'
        raw_data = pd.read_csv(filename, usecols=cols, nrows=nrows, sep=sep)
        # Drop Manaus
        raw_data = raw_data[~raw_data['codemun'].isin(manaus_codemun_list)]
        raw_data['year'] = year
        raw_data = raw_data.merge(muni_micro_cw, on='codemun', how='left', validate='m:1', indicator=True)
        raw_data['female'] = np.where(raw_data.genero==2, 1, np.where(raw_data.genero==1, 0, np.nan))
        print(raw_data._merge.value_counts())
        # Recode 'idade' into a categorical variable
        bins = [17, 24, 29, 39, 49, 64]
        labels = ['18-24', '25-29', '30-39', '40-49', '50-64']
        if year>1993:
            raw_data = raw_data.loc[(raw_data.idade>=18) & (raw_data.idade<=64)]
            raw_data['age_cat'] = pd.cut(raw_data['idade'], bins=bins, labels=labels, right=True)
            # Drop public sector
            raw_data = raw_data.loc[~raw_data['tipo_vinculo'].isin([30, 31, 35])]
        else:
            raw_data = raw_data.loc[(raw_data.fx_etaria>=3) & (raw_data.fx_etaria<=7)]
            raw_data['age_cat'] = raw_data.fx_etaria.astype('category').cat.rename_categories(labels)
            # Drop public sector
            raw_data = raw_data.loc[raw_data.nat_vinculo!=2]
        # Keep only people employed on December 31 and who have positive December earnings
        raw_data = raw_data.loc[raw_data.emp_31dez==1]
        raw_data = raw_data.loc[raw_data.rem_dez_sm>0]
        # Drop public sectors
        raw_data = raw_data.loc[~raw_data['subs_ibge'].isin([14,24])]
        # Mapping from GRAU_INSTR to new English category labels
        education_map = {
            1: 'No formal education',
            2: 'Incomplete primary school',
            3: 'Primary school',
            4: 'Incomplete middle school',
            5: 'Middle school',
            6: 'Incomplete high school',
            7: 'High school',
            8: 'Incomplete college',
            9: 'College', # Combining college, master's, and doctorate
            10: 'College', # Combining college, master's, and doctorate
            11: 'College' # Combining college, master's, and doctorate
        }
        # Convert GRAU_INSTR to new categorical labels
        raw_data['educ_cat'] = raw_data['grau_instr'].map(education_map)
        # Convert the 'Education_Level' column to a categorical type
        raw_data['educ_cat'] = raw_data['educ_cat'].astype('category')
        raw_data.drop(columns=['codemun','_merge','grau_instr','idade','fx_etaria', 'genero'], inplace=True, errors='ignore')
        # XX Need to decide how to filter on tipo_vinculo/nat_vinculo
        # Merge on Mayara's occ2s (stored as the column 'aggregate'
        raw_data = raw_data.merge(cbo1994_occ2_cw, left_on='cbo1994', right_on='cbo94', how='left', validate='m:1', indicator=True)
        # Code firm-market dummies
        raw_data['has_nan_firm_market'] = raw_data[['cnpj_raiz', 'code_micro', 'occ2']].isnull().any(axis=1)
        raw_data['firm_market_fe'] = raw_data['cnpj_raiz'].astype(str) + '_' + raw_data['code_micro'].astype(str) + '_' + raw_data['occ2'].astype(str)
        raw_data.loc[raw_data['has_nan_firm_market'], 'firm_market_fe'] = np.nan
        
        raw_data['has_nan_market'] = raw_data[['code_micro', 'occ2']].isnull().any(axis=1)        
        raw_data['market_fe'] = raw_data['code_micro'].astype(str) + '_' + raw_data['occ2'].astype(str)
        raw_data.loc[raw_data['has_nan_market'], 'market_fe'] = np.nan
        raw_data.drop(columns=['has_nan_market','has_nan_firm_market'], inplace=True)
        dfs.append(raw_data)
    
    
    df = pd.concat(dfs)    
    df.to_pickle(root + f"Data/derived/mayara_regression_panel_{fyear}_{lyear}.p")
    
    
df = pd.read_pickle(root + f"Data/derived/mayara_regression_panel_{fyear}_{lyear}.p")


# Restrict to obs with positive wage
df = df.loc[df.firm_market_fe.isna()==False]



######################################################
# Identify the CNAE for each firm

if run_firm_cnae_pull==True:    
    dfs = []
    # Loop over the years, load each dataset, and keep the necessary columns
    for year in range(1990, 1997 + 1):
        print(year)
        if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
            sep = ';'
        else:
            sep = ','
        # Keep the necessary columns, handle the conditional existence of 'class_cnae'
        if year >= 1995:
            cols = ['cnpj_raiz', 'subativ_ibge', 'clas_cnae']
        else:
            cols = ['cnpj_raiz', 'subativ_ibge']
        
        # Load the dataset for the current year
        raw = pd.read_csv(rais + f'csv/estab{year}.csv', usecols=cols, sep=sep)  # Assuming the data is in CSV format
        raw['year'] = year
        # Append the dataframe to the list
        dfs.append(raw)
    
    # Concatenate all dataframes
    firmdf = pd.concat(dfs, ignore_index=True)
    
    firmdf.to_pickle(root + "Data/derived/mayara_regression_panel_firmdf.p")
    
firmdf = pd.read_pickle(root + "Data/derived/mayara_regression_panel_firmdf.p")



# Create a separate dataset identifying the first observed value of 'class_cnae' for each 'cnpj_raiz'
first_class_cnae = firmdf.dropna(subset=['clas_cnae']).drop_duplicates(subset=['cnpj_raiz'], keep='first')



df = df.merge(first_class_cnae[['cnpj_raiz','clas_cnae']], on='cnpj_raiz', how='left', validate='m:1', indicator='cnae_merge')
# XX Need to figure out how to impute cnae using subativ_ibge for firms that die before 1995. From Mayara's appendix:
''' RAIS’ finest sector codes for 1986-2000 are 4-digit “IBGESUBATIVIDADE” (prior to 1995) and 5-digit “CNAE95” (1995 onwards). I focus on the 5-digit CNAE95 codes to map tariff shocks to firms in RAIS. For firms that exit the data prior to reporting any CNAE95 codes, I assign a CNAE95 code using a correspondence table I constructed using the pre-1995 and post-1995 codes of firms in business in both periods. To each IBGESUBATIVIDADE code I assign the most commonly reported CNAE95 code. Finally, throughout all years I use the first CNAE95 code ever reported by a firm as its official CNAE95 code.
'''

ibge_cnae_map = pd.read_stata(root + 'Code/MarketPower/mayara_data/mapping_procedures/map_ibgesubactivity_cnae/output/ibgesubactivityXcnae95_finalmap.dta')
ibge_cnae_map = ibge_cnae_map[['ibgesubactivity','cnae','cnae_pct_consist']]
# Identify the maximum cnae_pct_consist within each ibge
idx = ibge_cnae_map.groupby('ibgesubactivity')['cnae_pct_consist'].idxmax()
ibge_cnae_map = ibge_cnae_map.loc[idx]

df = df.merge(ibge_cnae_map, left_on='subativ_ibge', right_on='ibgesubactivity', how='left', validate='m:1', indicator='cnae_merge2')
# Replace missing values in 'clas_cnae' with values from 'cnae'
df.loc[df.clas_cnae.isna(),'clas_cnae'] = df.loc[df.clas_cnae.isna(),'cnae']
df.drop(columns=['cnae','cnae_merge','cnae_merge2'], inplace=True)

# There are a very small number of cases where clas_cnae is not unique within cnpj_raiz. In these cases take the mode. I'm not sure exactly what the root of this non-uniqueness is. Probably related to trying to impute cnae, but I can't figure that out exactly.

def mode(series):
    mode_value = series.mode()
    if not mode_value.empty:
        return mode_value.iloc[0]
    else:
        return series.iloc[0]

# Apply mode to clas_cnae within each cnpj_raiz
df['clas_cnae'] = df.groupby('cnpj_raiz')['clas_cnae'].transform(mode)



# Read in Mayara's tariffs data
cnae_avg_tariffs = pd.read_stata(root + 'Code/MarketPower/cnae_avg_tariffs.dta')
cnae_avg_tariffs['clas_cnae'] = cnae_avg_tariffs.cnae.astype(float)
cnae_cols = ['cnae', 'tradable', 'mean_rate1989',  'mean_rate1990', 'mean_rate1991', 'mean_rate1992', 'mean_rate1993', 'mean_rate1994', 'mean_rate1995']
df = df.merge(cnae_avg_tariffs[cnae_cols], left_on='clas_cnae', right_on='cnae', how='left', validate='m:1', indicator='tariff_merge')



df.tariff_merge.value_counts()
# Why are so many CNAEs not matched to tariff data. Is it because the tariff data don't include non-tradables? Can we just set tariffs for all non-matched obs to 0 
'''
cnae_avg_tariffs.tradable.value_counts()
Out[91]: 
tradable
1.0    293
0.0      3
Name: count, dtype: int64
'''
df['temp'] = df.tariff_merge=='both'

#############################################
#############################################
# KEEPING ONLY OBSERVATIONS THAT WERE MATCHED WITH THE TARIFF DATA
#df_full = df.copy()
#df = df[df.tariff_merge=='both']
#############################################
#############################################


df.groupby(['subs_ibge']).temp.mean()



columns_to_fill = ['tradable', 'mean_rate1989', 'mean_rate1990', 'mean_rate1991',
                   'mean_rate1992', 'mean_rate1993', 'mean_rate1994', 'mean_rate1995']
# Fill NaN values with 0 in the specified columns
df[columns_to_fill] = df[columns_to_fill].fillna(0)

df.drop(columns = ['_merge','tariff_merge'], inplace=True)



############################################
# Run the regression to compute wage premia

# Ensure 'age_cat', 'educ_cat', 'firm_market_fe' are treated as categorical variables
df['female'] = df['female'].astype('category')
df['age_cat'] = df['age_cat'].astype('category')
df['educ_cat'] = df['educ_cat'].astype('category')

# Create an interaction term between 'firm_market_fe' and 'year'
df['firm_market_fe_year'] = df['firm_market_fe'].astype(str) + ':' + df['year'].astype(str)
df['firm_market_fe_year'] = df['firm_market_fe_year'].astype('category')

df['ln_rem_dez_sm'] = np.log(df['rem_dez_sm'])




# Save the processed dataframe
df.to_pickle(root + "Data/derived/mayara_regression_panel_processed_df.p")

df = pd.read_pickle(root + "Data/derived/mayara_regression_panel_processed_df.p")


df['unique_firms_per_year'] = df.groupby(['market_fe', 'year'])['cnpj_raiz'].transform('nunique')
df = df.loc[df['unique_firms_per_year']>2]




df['d_ln_tau'] = - np.log((1+df['mean_rate1994']/100)/(1+df['mean_rate1990']/100))

d_ln_tau = df[['cnpj_raiz','d_ln_tau']]
d_ln_tau.drop_duplicates(inplace=True)



# Set the index for PanelOLS
df = df.set_index(['firm_market_fe_year','year'])


# XX Should we be estimating firm-market-year FEs using all years in 1986-2000? See page 35 of Mayara's appendix.
dict_firm_market_year_fes = {}
for year in [fyear,lyear]:    
    df_year = df.loc[pd.IndexSlice[:,year],:]
    # Convert categorical variables to dummy variables
    X = pd.get_dummies(df_year[['age_cat','educ_cat','female']], drop_first=True)
    
    # Specify the dependent variable and independent variables
    y = df_year[['ln_rem_dez_sm']]
    
    # Fit the model using PanelOLS
    model = PanelOLS(y, X, entity_effects=True).fit()
    firm_market_year_fes = model.estimated_effects.copy().reset_index()
    firm_market_year_fes.drop_duplicates(subset=['firm_market_fe_year','year'],keep='last', inplace=True) # Previously I tried dropping duplicates based on all variables but because of precision issues some values of estimated_effects vary slightly within a firm_market_fe_year. The amount of variation is trivial, but technically non-zero, so it was preventing certain rows that are essentially duplicates from being dropped. 
    # Create the new 'firm_market_fe' column by removing the year suffix
    firm_market_year_fes['firm_market_fe'] = firm_market_year_fes['firm_market_fe_year'].str.replace(r':\d{4}$', '', regex=True)
    firm_market_year_fes.drop(columns='firm_market_fe_year', inplace=True)
    # Reshape to wide format on 'year'
    firm_market_year_fes.reset_index(drop=True, inplace=True)
    dict_firm_market_year_fes[year] = firm_market_year_fes.rename(columns={'estimated_effects':f'w_zm{year}'}).drop(columns='year')

# Print the summary of the regression
print(model.summary)

firm_market_year_fes = pd.merge(dict_firm_market_year_fes[fyear], dict_firm_market_year_fes[lyear], on='firm_market_fe', validate='1:1', how='inner')

omega_zmt = pd.melt(firm_market_year_fes.rename(columns={f'w_zm{fyear}':fyear,f'w_zm{lyear}':lyear}), id_vars=['firm_market_fe'], var_name='year', value_name='omega_zmt')

# XX 78% of rows are NAN, presumably because many small firm-markets don't exist in both years
firm_market_year_fes['d_ln_w_zm'] = firm_market_year_fes[f'w_zm{lyear}'] - firm_market_year_fes[f'w_zm{fyear}']

d_ln_w_zm = firm_market_year_fes[['firm_market_fe','d_ln_w_zm']]
d_ln_w_zm['cnpj_raiz'] = d_ln_w_zm['firm_market_fe'].str.split('_').str[0].astype(float)
d_ln_w_zm['market_fe'] = d_ln_w_zm['firm_market_fe'].str.split('_').str[1] + '_' + d_ln_w_zm['firm_market_fe'].str.split('_').str[2]


############################################
# Run the regression to compute wage premia

# Compute firm-market employment differences
l_zmt = df.groupby(['firm_market_fe','year'])['rem_dez_sm'].count().reset_index(name='l_zmt').pivot(index='firm_market_fe', columns='year', values='l_zmt')
l_zmt = l_zmt.dropna(subset=[fyear, lyear])

d_ln_l_zm = np.log(l_zmt[lyear]) - np.log(l_zmt[fyear])
d_ln_l_zm = d_ln_l_zm.reset_index()
d_ln_l_zm.rename(columns={0:'d_ln_l_zm'}, inplace=True)
# Split firm-market FEs into firm and market
d_ln_l_zm['cnpj_raiz'] = d_ln_l_zm['firm_market_fe'].str.split('_').str[0].astype(float)
d_ln_l_zm['market_fe'] = d_ln_l_zm['firm_market_fe'].str.split('_').str[1] + '_' + d_ln_l_zm['firm_market_fe'].str.split('_').str[2]


# Compute for later
l_zmt = pd.melt(l_zmt.reset_index(), id_vars=['firm_market_fe'], var_name='year', value_name='l_zmt')

# Baseline firm counts
l_zm_1991 = l_zmt.loc[l_zmt.year==1991].drop(columns='year').rename(columns={'l_zmt':'l_zm_1991'})

# Checking that we have the same number of obs for both wages and employment
d_ln_w_zm.d_ln_w_zm.notna().sum() == d_ln_l_zm.d_ln_l_zm.notna().sum()


############################################
# Step 2: Run the regression in Mayara's equation (16)

# Merge on d_ln_tau to d_ln_l_zm
step2_df = d_ln_l_zm.merge(d_ln_tau, on='cnpj_raiz', how='left', validate='m:1', indicator=True)
step2_df = step2_df.merge(l_zm_1991, on='firm_market_fe', validate='1:1')
step2_df = step2_df.set_index(['market_fe','cnpj_raiz'])
step2_df = step2_df.loc[step2_df.d_ln_l_zm.notna()]

y = step2_df['d_ln_l_zm']
X = step2_df['d_ln_tau']
wgt = step2_df['l_zm_1991']

model_M16 = PanelOLS(y, X, weights=wgt, entity_effects=True).fit()
print(model_M16.summary)
print(model_M16.params.values[0], '; Mayara: -0.554')
print('Num obs (us): ', model_M16.nobs, '; Num obs Mayara: 855k')

unique_market_fe = step2_df.index.get_level_values('market_fe').nunique()
unique_cnpj_raiz = step2_df.index.get_level_values('cnpj_raiz').nunique()
print("\nNumber of unique 'market_fe':", unique_market_fe, "; Mayara: 16k")
print("Number of unique 'cnpj_raiz':", unique_cnpj_raiz, "; Mayara: 345k")

'''
Mayara's figures
N = 855k, 
firms = 345k
mkts = 16k
1st stage estimate = -.55    (lambda in equation m.16)

'''

# Compute predicted values d_ln_l_zm_hat
temp = pd.merge(model_M16.predict(X).copy(),model_M16.estimated_effects.copy(), left_index=True, right_index=True)
step2_df['d_ln_l_zm_hat'] = temp.predictions + temp.estimated_effects

step2_df = step2_df.merge(d_ln_w_zm.drop(columns='firm_market_fe').set_index(['market_fe','cnpj_raiz']), left_index=True, right_index=True, how='left', validate='1:1', indicator='_merge2')


# Check the reduced form
y_rf = step2_df['d_ln_w_zm']
X_rf = step2_df['d_ln_tau']
model_rf = PanelOLS(y_rf, X_rf, weights=wgt, entity_effects=True).fit()
print(model_rf.summary)
print(model_rf.params.values[0], '; Mayara: -0.545')

y = step2_df['d_ln_w_zm']
X = step2_df['d_ln_l_zm_hat']
wgt = step2_df['l_zm_1991']

model_M15 = PanelOLS(y, X, weights=wgt, entity_effects=True).fit()
print(model_M15.summary)
print(model_M15.params.values[0], "Mayara's estimate for 1/eta in eq. 15: 0.98")
print('Num obs (us): ', model_M15.nobs, '; Num obs Mayara: 855k')

unique_market_fe = step2_df.index.get_level_values('market_fe').nunique()
unique_cnpj_raiz = step2_df.index.get_level_values('cnpj_raiz').nunique()

# Save market FEs for later
d_delta_m = model_M15.estimated_effects.copy().reset_index().drop(columns='cnpj_raiz')
d_delta_m.drop_duplicates(subset=['market_fe'],keep='last', inplace=True)


eta_hat = 1 / model_M15.params.values[0]


print(model_M15.params.values[0])
# I think this estimate of 0.292 should be compared to Mayara's estimate of 0.985 in Panel C of Table 2 on page 48



##############################################################################
# Third step: estimating taste-adjusted marekt-level labor supplies

# Note omega_zmt stored in firm_market_year_fes

step3_df = omega_zmt.merge(l_zmt, on=['firm_market_fe','year'], how='inner', validate='1:1')
step3_df['lhs'] = step3_df['omega_zmt'] - (1/eta_hat) * np.log(step3_df['l_zmt'])
step3_df['market_year_fe'] = step3_df['firm_market_fe'].str.split('_').str[1] + '_' + step3_df['firm_market_fe'].str.split('_').str[2] + '_' + step3_df['year'].astype(str)
step3_df['cnpj_raiz'] = step3_df['firm_market_fe'].str.split('_').str[0].astype(float)
step3_df.set_index(['market_year_fe','cnpj_raiz'], inplace=True)

# Export to Stata to check if xtreg replicates PanelOLS [it does]
temp = step3_df.reset_index()
temp['year'] = temp['year'].astype(int)
temp.to_stata(root + "Data/derived/step3_df.dta")

# Specify the dependent variable and independent variables
y = step3_df[['lhs']]
step3_df['X'] = 1
X = step3_df['X']

# Fit the model using PanelOLS
model = PanelOLS(y, X, entity_effects=True).fit()
step3_df['nu_zmt_hat'] = model.resids
step3_df['xi_zmt'] = (1 + eta_hat) * np.exp(step3_df['nu_zmt_hat'])

ces_inner = pd.DataFrame((step3_df['xi_zmt']*step3_df['l_zmt'])**((1+eta_hat)/eta_hat))
ces_inner.reset_index(inplace=True)

L_mt = (ces_inner.groupby(['market_year_fe'])[0].sum())**(eta_hat/(1+eta_hat))
L_mt = L_mt.to_frame().reset_index()
L_mt['market'] = L_mt['market_year_fe'].str.split('_').str[0] + '_'+  L_mt['market_year_fe'].str.split('_').str[1]
L_mt['year']   = L_mt['market_year_fe'].str.split('_').str[2]


L_mt = L_mt.pivot(index='market', columns = 'year', values=0)
d_ln_L_m = np.log(L_mt[f'{lyear}']) - np.log(L_mt[f'{fyear}'])
d_ln_L_m = d_ln_L_m.reset_index().rename(columns={0:'d_ln_L_m'})

##############################################################################
# Fourth step: estimating cross-market substition 1/theta

df.reset_index(inplace=True)

# Step 2: Calculate total payroll for each market
market_total_payroll      = df.loc[df.year==1991].groupby('market_fe')['rem_dez_sm'].sum().reset_index(name='market_total_payroll')
firm_market_total_payroll = df.loc[df.year==1991].groupby(['firm_market_fe','market_fe'])['rem_dez_sm'].sum().reset_index(name='firm_market_total_payroll')

# Step 3: Merge the total market payroll back to the original DataFrame
merged = pd.merge(firm_market_total_payroll, market_total_payroll, on='market_fe', how='left', validate='m:1', indicator=True)

# Step 4: Calculate the share of each firm's payroll in its market's total payroll
merged['s_zm'] = merged['firm_market_total_payroll'] / merged['market_total_payroll']

s_zm = merged[['s_zm','firm_market_fe']]

s_zm['cnpj_raiz'] = s_zm['firm_market_fe'].str.split('_').str[0].astype(float)
s_zm['market'] = s_zm['firm_market_fe'].str.split('_').str[1] + '_' + s_zm['firm_market_fe'].str.split('_').str[2]
s_zm['s_zm_sq'] = s_zm['s_zm']**2

# Merge on indicator for firm z being tradable and keep tradables only
# Merge on d_ln_tau
# d_ICE_m should only be defined for firms in the tradable sector
firms_tradables = df[['cnpj_raiz','tradable']].drop_duplicates()
s_zm = s_zm.merge(firms_tradables, on='cnpj_raiz', how='left', validate='m:1')

# Keep only tradables
s_zm = s_zm.loc[s_zm.tradable==1]
s_zm = s_zm.merge(d_ln_tau, on='cnpj_raiz', how='left', validate='m:1')
s_zm['s_zm_sq_X_d_ln_tau'] = s_zm['s_zm_sq']*s_zm['d_ln_tau']
s_zm['denom'] = s_zm.groupby('market')['s_zm_sq'].transform('sum')
s_zm['inside_sum'] = s_zm['s_zm_sq_X_d_ln_tau'] / s_zm['denom']

d_ICE_m = s_zm.groupby('market')['inside_sum'].sum().reset_index().rename(columns={'inside_sum':'d_ICE_m'})

### 
# XX Note that we have more rows in d_ln_L_m than in d_ICE_m. Presumably because some markets have no tradable firms in them in 1991
# XX We have unmatched firms in both master and using. Probably because some markets have no tradables in 1991 and some have no employment in 1997.
# XX Should we be restricting to only the matched markets in the regressions above?
step4_df = pd.merge(d_ln_L_m, d_ICE_m, on='market', how='inner', validate='1:1')
step4_df = step4_df.merge(d_delta_m, left_on='market',right_on='market_fe', how='left', validate='1:1')

# Define the dependent and independent variables
X = step4_df['d_ICE_m']
y = step4_df['d_ln_L_m']
# Add a constant to the independent variables
X = sm.add_constant(X)
# Fit the regression model
model = sm.OLS(y, X).fit()
print(model.summary())
lambda_hat = model.params.values[1]
print('\hat lambda: ', lambda_hat, "; Mayara's estimate: -0.396")
# Get the fitted values
step4_df['fitted_values'] = model.fittedvalues

X = step4_df['fitted_values']
y = step4_df['estimated_effects']
# Add a constant to the independent variables
X = sm.add_constant(X)
# Fit the regression model
model = sm.OLS(y, X).fit()
inv_theta_minus_inv_eta_hat = model.params[1]
theta_hat = 1/(inv_theta_minus_inv_eta_hat + (1/eta_hat))


print(theta_hat) # -3.005
print(eta_hat)   #  3.423

# Check that shares always equal 1
merged.groupby('market_fe')['s_zm'].sum().min()
merged.groupby('market_fe')['s_zm'].sum().max()


# First stage: regress d_ln_L_m on d_ICE_m



###############################
# Diagnostics

# df['has_nan_market'] = df[['code_micro', 'occ2']].isnull().any(axis=1)        
df.loc[df['has_nan_market'], 'market_fe'] = np.nan


df = pd.read_pickle(root + "Data/derived/mayara_regression_panel_processed_df.p")


df.loc[df.year==1991].market_fe.nunique()


counts_table = df.loc[df.year==1991].groupby('market_fe').agg(
    unique_cnpj_raiz=('cnpj_raiz', 'nunique'),
    employment=('market_fe', 'size')
).reset_index()


(counts_table.unique_cnpj_raiz>1).sum()
#Out[11]: 24900

(counts_table.unique_cnpj_raiz>2).sum()
#Out[12]: 22174

(counts_table.unique_cnpj_raiz>3).sum()
#Out[13]: 20192

# This really matters
counts_table.describe(percentiles=[ .10, .25, .50, .75, .90])
counts_table.loc[counts_table.unique_cnpj_raiz>1].describe(percentiles=[ .10, .25, .50, .75, .90])
counts_table.loc[counts_table.unique_cnpj_raiz>2].describe(percentiles=[ .10, .25, .50, .75, .90])

# Mayara has 475 micro regions according to Table A.4
df.code_micro.nunique()
