'''
Outstanding issues:
    - We are losing the first iota/mmc/etc interaction term because it is the "main" effect of dlnonetariff_1990_1995 and therefore will not be caught in the regular expressions below
    - We are impliclty dropping people with no employment in year T. Do we want to code these as 0? If so, how to do so with logs? Or do we want to do a separate regression with employment? Or cumulative (log cumulative?) earnings over all years?
    - How are we handling multiple jobs in one year?
'''
import pandas as pd
import statsmodels.formula.api as smf
import statsmodels.api as sm
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pickle

import getpass
import platform

import copy

def save_essential_regression_results(results, filepath=None):
    """
    Extracts essential attributes from a statsmodels RegressionResultsWrapper,
    stores them in a dictionary, and optionally saves the dictionary to a file using pickle.
    
    Parameters:
        results (RegressionResultsWrapper): The regression results object.
        filepath (str, optional): Path to the file where the dictionary should be saved.
                                  If None, the dictionary is not saved to file.
    
    Returns:
        A dictionary containing essential regression results.
    """
    essential_data = {
        'params': results.params,
        'bse': results.bse,
        'pvalues': results.pvalues,
        'conf_int': results.conf_int(),
        'rsquared': results.rsquared,
        'rsquared_adj': results.rsquared_adj,
        'aic': results.aic,
        'bic': results.bic,
        'summary': results.summary().as_text()
        # Include any other summaries or statistics you need
    }

    # If a filepath is provided, save the dictionary to that file
    if filepath:
        with open(filepath, 'wb') as file:
            pickle.dump(essential_data, file)
        print(f"Saved essential regression results to {filepath}")
    
    return essential_data

def define_root_directory():
    user = getpass.getuser()
    os_name = platform.system()

    if user == 'p13861161':
        if os_name == 'Windows':
            root = "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/"
        elif os_name == 'Linux':
            root = "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/"
        else:
            root = None  # or a default path, if any
    else:
        root = None  # or a default path for other users, if any

    return root

# Use the function to get the root path
root = define_root_directory()
print("Root path is set to:", root)

# Load your data
df = pd.read_stata(root + "Data/derived/trade_shock/trade_shock_regression_data.dta")
#df = pd.read_stata("/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Data/derived/trade_shock/trade_shock_regression_data.dta")


##
# XX This is where we could merge on iotas and gammas from the new mcmcs. Would have to replace existing iotas; existing gammas are merged on later in this file so we'd want to comment out that merge. 

# Loop for regressions excluding year 1999
results = {}
coef_dict = {}
for year in range(1991, 2006):
    if year != 1999:
        formula = f'd_log_rem_1990_{year} ~ dlnonetariff_1990_1995'
        result = smf.ols(formula, data=df).fit()
        results[year] = result
        coef_dict[year] = result.params['dlnonetariff_1990_1995']


# Display results (similar to esttab)
for key, result in results.items():
    print(f'Regression Results for year: {key}')
    print(result.summary())
    print("\n")



# Plotting the coefficients
years = list(coef_dict.keys())
coefficients = list(coef_dict.values())

# Create the plot
plt.figure(figsize=(10, 5))
plt.plot(years, coefficients, marker='o', linestyle='-', color='b')  # Blue line with circle markers
plt.title('Regression Coefficients of dlnonetariff_1990_1995 Over Years')
plt.xlabel('Year')
plt.ylabel('Coefficient of dlnonetariff_1990_1995')
plt.grid(True)
plt.show()


# Define occ2Xmeso and occ3
region_codes = pd.read_csv(root + 'Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'code_uf':region_codes.code_uf,'codemun':region_codes.code_munic//10})
df['codemun'] = df['codemun'].astype(int)
df = df.merge(muni_meso_cw, how='left', on='codemun', copy=False) # validate='m:1', indicator=True)
df['occ2Xmeso'] = df.cbo1994.astype('str').str[0:2] + '_' + df['code_meso'].astype('str')
    
df['occ3'] = df.cbo1994.astype('Int64').astype(str).replace('<NA>', 'na').str[0:3]
#df['occ3_temp'] = df.cbo1994.astype('Int64').astype(str).replace('<NA>', None).str[0:3]
#df['occ3_temp'].isna().sum()

#####################
# Load CBO1994 translated names (from Marc Muendler: https://econweb.ucsd.edu/muendler/download/brazil/cbo/cbotrans.csv)
cbo1994_names = pd.read_csv(root + 'Data/raw/cbotrans.csv', encoding='ISO-8859-1', header=None, names=['cboid', 'cbotrans'], dtype={'cboid': str})
cbo1994_names['cbo1994'] = cbo1994_names['cboid'].str.replace(r'\D', '', regex=True).astype(int)


##############
# Identify obs used in iota regression and restrict to these obs
# Interaction regressions with 'iota'
iota_reg_temp = smf.ols('d_log_rem_1990_2000 ~ dlnonetariff_1990_1995*C(iota)', data=df).fit()
df['iota_sample'] = iota_reg_temp.fittedvalues  # Creating a column for sample
# Get the index of the rows used in the regression
used_indices = iota_reg_temp.model.data.row_labels
# Initialize a new column with 0 (not used)
df['used_in_regression'] = 0
# Set to 1 for rows that were used in the regression
df.loc[used_indices, 'used_in_regression'] = 1
del iota_reg_temp



df_filtered = df.loc[df.used_in_regression == 1]


# Getting coefficients


#XX We are losing the first iota/mmc/etc interaction term because it is the "main" effect of dlnonetariff_1990_1995 and therefore will not be caught in the regular expressions below

# More interaction regressions and analyses
# Repeat similar steps as above for 'mmc' and 'cbo1994' interactions
iota_reg = smf.ols('d_log_rem_1990_2000 ~ dlnonetariff_1990_1995*C(iota)', data=df_filtered).fit()
df_filtered['iota_fitted_values'] = iota_reg.fittedvalues
beta_iota = iota_reg.params.filter(regex='dlnonetariff_1990_1995:C\(iota\)\[T').reset_index().rename(columns={0: 'coef_iota'})
se_iota = iota_reg.bse.filter(regex='dlnonetariff_1990_1995:C\(iota\)\[T').reset_index().rename(columns={0: 'se_iota'})
beta_iota = beta_iota.merge(se_iota, on='index')
beta_iota['t_iota'] = beta_iota['coef_iota']/beta_iota['se_iota']
beta_iota['iota'] = beta_iota['index'].str.extract(r'\[T\.(\d+)\.0?\]')
beta_iota['iota'] = beta_iota['iota'].astype(int)
beta_iota.drop(columns='index', inplace=True)
iota_reg = save_essential_regression_results(iota_reg, filepath = root + 'Data/derived/trade_shock/iota_reg.pickle')

mmc_reg  = smf.ols('d_log_rem_1990_2000 ~ dlnonetariff_1990_1995*C(mmc)', data=df_filtered).fit()
df_filtered['mmc_fitted_values'] = mmc_reg.fittedvalues
beta_mmc = mmc_reg.params.filter(regex='dlnonetariff_1990_1995:C\(mmc\)\[T').reset_index().rename(columns={0: 'coef_mmc'})
se_mmc = mmc_reg.bse.filter(regex='dlnonetariff_1990_1995:C\(mmc\)\[T').reset_index().rename(columns={0: 'se_mmc'})
beta_mmc = beta_mmc.merge(se_mmc, on='index')
beta_mmc['t_mmc'] = beta_mmc['coef_mmc']/beta_mmc['se_mmc']
beta_mmc['mmc'] = beta_mmc['index'].str.extract(r'\[T\.(\d+)[\.0]?\]')
beta_mmc['mmc'] = beta_mmc['mmc'].astype(int)
beta_mmc.drop(columns='index', inplace=True)
mmc_reg = save_essential_regression_results(mmc_reg, filepath = root + 'Data/derived/trade_shock/mmc_reg.pickle')


# XX Will need to add fitted values for other 3 specs
occ4_reg  = smf.ols('d_log_rem_1990_2000 ~ dlnonetariff_1990_1995*C(occ4)', data=df_filtered).fit()
df_filtered['occ4_fitted_values'] = occ4_reg.fittedvalues
beta_occ4 = occ4_reg.params.filter(regex='dlnonetariff_1990_1995:C\(occ4\)\[T').reset_index().rename(columns={0: 'coef_occ4'})
se_occ4 = occ4_reg.bse.filter(regex='dlnonetariff_1990_1995:C\(occ4\)\[T').reset_index().rename(columns={0: 'se_occ4'})
beta_occ4 = beta_occ4.merge(se_occ4, on='index')
beta_occ4['t_occ4'] = beta_occ4['coef_occ4']/beta_occ4['se_occ4']
beta_occ4['occ4'] = beta_occ4['index'].str.extract(r'\[T\.(\d+)\.0?\]')
beta_occ4['occ4'] = beta_occ4['occ4'].astype(int)
print('NaNs in occ4 = ',beta_occ4['occ4'].isna().sum())
beta_occ4.drop(columns='index', inplace=True)
occ4_reg = save_essential_regression_results(occ4_reg, filepath = root + 'Data/derived/trade_shock/occ4_reg.pickle')

occ3_reg  = smf.ols('d_log_rem_1990_2000 ~ dlnonetariff_1990_1995*C(occ3)', data=df_filtered).fit()
df_filtered['occ3_fitted_values'] = occ3_reg.fittedvalues
beta_occ3 = occ3_reg.params.filter(regex='dlnonetariff_1990_1995:C\(occ3\)\[T').reset_index().rename(columns={0: 'coef_occ3'})
se_occ3 = occ3_reg.bse.filter(regex='dlnonetariff_1990_1995:C\(occ3\)\[T').reset_index().rename(columns={0: 'se_occ3'})
beta_occ3 = beta_occ3.merge(se_occ3, on='index')
beta_occ3['t_occ3'] = beta_occ3['coef_occ3']/beta_occ3['se_occ3']
beta_occ3['occ3'] = beta_occ3['index'].str.extract(r'\[T\.(\d+)\.?\0?\]')
# dealing with 1 NaNs (deleting it)
print('NaNs in occ3 = ',beta_occ3['occ3'].isna().sum())
print('NaNs in occ3 = ',(beta_occ3['occ3']=='na').sum())
beta_occ3 = beta_occ3.dropna(subset=['occ3'])
beta_occ3['occ3'] = beta_occ3['occ3'].astype(int)
beta_occ3.drop(columns='index', inplace=True)
occ3_reg = save_essential_regression_results(occ3_reg, filepath = root + 'Data/derived/trade_shock/occ3_reg.pickle')

occ2Xmeso_reg  = smf.ols('d_log_rem_1990_2000 ~ dlnonetariff_1990_1995*C(occ2Xmeso)', data=df_filtered).fit()
df_filtered['occ2Xmeso_fitted_values'] = occ2Xmeso_reg.fittedvalues
beta_occ2Xmeso = occ2Xmeso_reg.params.filter(regex='dlnonetariff_1990_1995:C\(occ2Xmeso\)\[T').reset_index().rename(columns={0: 'coef_occ2Xmeso'})
se_occ2Xmeso = occ2Xmeso_reg.bse.filter(regex='dlnonetariff_1990_1995:C\(occ2Xmeso\)\[T').reset_index().rename(columns={0: 'se_occ2Xmeso'})
beta_occ2Xmeso = beta_occ2Xmeso.merge(se_occ2Xmeso, on='index')
beta_occ2Xmeso['t_occ2Xmeso'] = beta_occ2Xmeso['coef_occ2Xmeso']/beta_occ2Xmeso['se_occ2Xmeso']
#beta_occ2Xmeso[['occ2', 'meso']] = beta_occ2Xmeso['index'].str.extract(r'T\.(\d+)_(\d+)')
beta_occ2Xmeso[['occ2', 'meso']] = beta_occ2Xmeso['index'].str.extract(r'T\.(.+?)_(.+?)\]')
beta_occ2Xmeso['occ2Xmeso'] = beta_occ2Xmeso['index'].str.extract(r'T\.(.+?_.+?)\]')
beta_occ2Xmeso['occ2Xmeso_str'] = beta_occ2Xmeso['index'].str.extract(r'T\.(.+?)\]')
beta_occ2Xmeso.drop(columns='index', inplace=True)
print('NaNs in occ2 = ',(beta_occ2Xmeso['occ2']=='na').sum())
print('NaNs in occ2 = ',beta_occ2Xmeso['occ2'].isna().sum())
occ2Xmeso_reg = save_essential_regression_results(occ2Xmeso_reg, filepath = root + 'Data/derived/trade_shock/occ2Xmeso_reg.pickle')


## deleting occ3 NAs from df filtered (otherwise the merge below won't work)
(df_filtered['occ3']=='na').sum()
print('% of df_filtered dropped due to occ3 NaN = ', np.round(100*(df_filtered['occ3']=='na').sum()/df_filtered.shape[0],2), '%')
df_filtered = df_filtered.loc[df_filtered['occ3']!='na']
df_filtered['occ3'] = df_filtered['occ3'].astype(int)

df_filtered = df_filtered.merge(beta_iota, on='iota', how='left', validate='m:1')
df_filtered = df_filtered.merge(beta_mmc, on='mmc', how='left', validate='m:1')
df_filtered = df_filtered.merge(beta_occ4, on='occ4', how='left', validate='m:1')
df_filtered = df_filtered.merge(beta_occ3, on='occ3', how='left', validate='m:1')
df_filtered = df_filtered.merge(beta_occ2Xmeso, on='occ2Xmeso', how='left', validate='m:1')

# Merge on cbo2002 using crosswalk I created in cbo_1994to2992cw.py
modal_cbo2002 = pd.read_pickle('//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Data/temp/cbo_1994to2002_cw.p')
df_filtered = df_filtered.merge(cbo1994_names[['cbo1994','cbotrans']], on='cbo1994', how='left', validate='m:1', indicator=True)


for k in ['mmc','iota','occ3','occ4', 'occ2Xmeso']:
    df_filtered[f'count_{k}'] = df_filtered.groupby(k)[k].transform('count')





###############################################################################################
# Merge on gammmas

# Temp fix because we were missing gammas. Tried re-running trade_shock_regressions.do to fix that but failed due to some weird memory issue in a temp directory

gammas = pd.read_csv(root + 'Data/derived/sbm_output/model_trade_shock_jblocks.csv')[['jid', 'job_blocks_level_0']].rename(columns={'job_blocks_level_0':'gamma'})
df_filtered = df_filtered.merge(gammas, on='jid', how='left', validate='m:1', indicator='_merge_gamma')


###############################################################################################
# Compute Bartik-style exposure measures

# Compute shares
pivot_df = pd.pivot_table(df_filtered, index='subsibge', columns='gamma', aggfunc='size', fill_value=0)
industry_share_of_gamma = pivot_df.apply(lambda x: x/x.sum(), axis=0).reset_index()

pivot_df = pd.pivot_table(df_filtered, index='gamma', columns='iota', aggfunc='size', fill_value=0)
gamma_share_of_iota = pivot_df.apply(lambda x: x/x.sum(), axis=0).reset_index()

pivot_df = pd.pivot_table(df_filtered, index='subsibge', columns='iota', aggfunc='size', fill_value=0)
industry_share_of_iota = pivot_df.apply(lambda x: x/x.sum(), axis=0).reset_index()

# Extract tariff changes by industry
dlnonetariff_1990_1995 = df_filtered[['subsibge','dlnonetariff_1990_1995']].drop_duplicates()


#####
# E_tariff_by_iota_given_gamma_industry

## Step 1: Compute expected tariff for each gamma (integrate tariffs over P{industry|gamma})
# Set the index of industry_share_of_gamma to 'gamma'
industry_share_of_gamma.set_index('subsibge', inplace=True)
# Transpose industry_share_of_gamma
industry_share_of_gamma_transposed = industry_share_of_gamma.T
# Multiply the transposed industry_share_of_gamma by dlnonetariff_1990_1995
E_tariff_by_gamma = industry_share_of_gamma_transposed.dot(dlnonetariff_1990_1995.set_index('subsibge'))

## Step 2: Compute expected tariff for each iota (integrate expected tariffs for each gamma over P{gamma|iota})
gamma_share_of_iota.set_index('gamma', inplace=True)
# Transpose industry_share_of_gamma
gamma_share_of_iota_transposed = gamma_share_of_iota.T
# Multiply the transposed industry_share_of_gamma by dlnonetariff_1990_1995
E_tariff_by_iota_given_gamma_industry = gamma_share_of_iota_transposed.dot(E_tariff_by_gamma).reset_index().rename(columns={'dlnonetariff_1990_1995':'E_tariff_by_iota_given_gamma_industry'})

E_tariff_by_gamma = E_tariff_by_gamma.reset_index().rename(columns={'dlnonetariff_1990_1995':'E_tariff_by_gamma'})


#####
# E_tariff_by_iota_given_industry

industry_share_of_iota.set_index('subsibge', inplace=True)
# Transpose industry_share_of_gamma
industry_share_of_iota_transposed = industry_share_of_iota.T
# Multiply the transposed industry_share_of_gamma by dlnonetariff_1990_1995
E_tariff_by_iota_given_industry = industry_share_of_iota_transposed.dot(dlnonetariff_1990_1995.set_index('subsibge')).reset_index().rename(columns={'dlnonetariff_1990_1995':'E_tariff_by_iota_given_industry'})



#####
# E_tariff_by_mmc_given_industry

pivot_df = pd.pivot_table(df_filtered, index='subsibge', columns='mmc', aggfunc='size', fill_value=0)
industry_share_of_mmc = pivot_df.apply(lambda x: x/x.sum(), axis=0).reset_index()


industry_share_of_mmc.set_index('subsibge', inplace=True)
# Transpose industry_share_of_gamma
industry_share_of_mmc_transposed = industry_share_of_mmc.T
# Multiply the transposed industry_share_of_gamma by dlnonetariff_1990_1995
E_tariff_by_mmc_given_industry = industry_share_of_mmc_transposed.dot(dlnonetariff_1990_1995.set_index('subsibge')).reset_index().rename(columns={'dlnonetariff_1990_1995':'E_tariff_by_mmc_given_industry'})


df_filtered = df_filtered.merge(E_tariff_by_iota_given_gamma_industry, on='iota', how='left')
df_filtered = df_filtered.merge(E_tariff_by_iota_given_industry, on='iota', how='left')
df_filtered = df_filtered.merge(E_tariff_by_gamma, on='gamma', how='left')
df_filtered = df_filtered.merge(E_tariff_by_mmc_given_industry, on='mmc', how='left')





######################################################################################################
# Save resulting dataframe

df_filtered.to_pickle( root + "/Data/derived/trade_shock/df_filtered.p")






######################################################################################################
# Densities of fitted values

# No treatment effect heterogeneity
homo_reg = smf.ols('d_log_rem_1990_2000 ~ dlnonetariff_1990_1995', data=df_filtered).fit()
df_filtered['homo_fitted_values'] = homo_reg.fittedvalues

E_tariff_by_gamma = smf.ols( 'd_log_rem_1990_2000 ~ E_tariff_by_gamma*C(iota)', data=df_filtered).fit()
df_filtered['E_tariff_by_gamma_fitted_values'] = E_tariff_by_gamma.fittedvalues
print(E_tariff_by_gamma.rsquared)



result = smf.ols( 'd_log_rem_1990_2000 ~ E_tariff_by_iota_given_industry', data=df_filtered).fit()
print(result.summary2())
result = smf.ols( 'd_log_rem_1990_2000 ~ E_tariff_by_mmc_given_industry', data=df_filtered).fit()


plt.figure(figsize=(10, 6))
sns.kdeplot(df_filtered['iota_fitted_values'], bw_adjust=0.5, label='iota_fitted_values', color='blue', fill=True)
sns.kdeplot(df_filtered['mmc_fitted_values'], bw_adjust=0.5, label='mmc_fitted_values', color='red', fill=True)
sns.kdeplot(df_filtered['E_tariff_by_gamma_fitted_values'], bw_adjust=0.5, label='E_tariff_by_gamma_fitted_values', color='green', fill=True)
#sns.kdeplot(df_filtered['homo_fitted_values'], bw_adjust=0.5, label='homo_fitted_values', color='green', fill=True)
#sns.kdeplot(df_filtered['d_log_rem_1990_2000'], bw_adjust=0.5, label='d_log_rem_1990_2000', color='yellow', fill=True)
plt.xlim(-1, 1)  # Set the x-axis limits
plt.title('Density Plot of Fitted Values')
plt.xlabel('Fitted Values')
plt.ylabel('Density')
plt.legend()
plt.savefig(root + 'Results/trade_shock/fitted_values_density_plot.pdf')
plt.show()
# One thing we could do is identify the workers with the largest gaps between iota and homo and see if we can explain this somehow



plt.figure(figsize=(10, 6))
sns.kdeplot(df_filtered['iota_fitted_values'], bw_adjust=0.5, label='iota_fitted_values', color='blue', fill=True)
sns.kdeplot(df_filtered['mmc_fitted_values'], bw_adjust=0.5, label='mmc_fitted_values', color='red', fill=True)
sns.kdeplot(df_filtered['occ3_fitted_values'], bw_adjust=0.5, label='occ3_fitted_values', color='green', fill=True)
sns.kdeplot(df_filtered['occ4_fitted_values'], bw_adjust=0.5, label='occ4_fitted_values', color='yellow', fill=True)
sns.kdeplot(df_filtered['occ2Xmeso_fitted_values'], bw_adjust=0.5, label='occ2Xmeso_fitted_values', color='purple', fill=True)
#sns.kdeplot(df_filtered['homo_fitted_values'], bw_adjust=0.5, label='homo_fitted_values', color='green', fill=True)
#sns.kdeplot(df_filtered['d_log_rem_1990_2000'], bw_adjust=0.5, label='d_log_rem_1990_2000', color='yellow', fill=True)
plt.xlim(-1, 1)  # Set the x-axis limits
plt.title('Density Plot of Fitted Values')
plt.xlabel('Fitted Values')
plt.ylabel('Density')
plt.legend()
plt.savefig(root + 'Results/trade_shock/fitted_values_density_plot_2.pdf')
plt.show()


###################################
# Speculative analyses

df_filtered[['iota_fitted_values','mmc_fitted_values','E_tariff_by_gamma_fitted_values','homo_fitted_values']].corr()

df_filtered[['iota_fitted_values','E_tariff_by_gamma','E_tariff_by_iota_given_gamma_industry','E_tariff_by_iota_given_industry','dlnonetariff_1990_1995']].corr()

test_reg1 = smf.ols('iota_fitted_values ~ dlnonetariff_1990_1995 + E_tariff_by_gamma + E_tariff_by_iota_given_gamma_industry + E_tariff_by_iota_given_industry', data=df_filtered).fit()
test_reg1.summary2()


test_reg2 = smf.ols('iota_fitted_values ~  E_tariff_by_gamma + E_tariff_by_iota_given_gamma_industry', data=df_filtered).fit()
test_reg2.summary2()

df_filtered[['coef_iota','E_tariff_by_gamma','E_tariff_by_iota_given_gamma_industry','E_tariff_by_iota_given_industry','dlnonetariff_1990_1995']].corr()




###################################
# COMPUTING R2, AIC AND BIC
model_names = ['mmc', 'iota', 'occ3', 'occ4', 'occ2Xmeso']
stats_dict = {}

for name in model_names:
    model = globals()[f'{name}_reg']  # Dynamically access the model object
    stats_dict[name] = {
        'R-squared': model['rsquared'],
        'Adj R-squared': model['rsquared_adj'],
        'AIC': model['aic'],
        'BIC': model['bic']
    }

pd.set_option('display.precision', 3)
stats = pd.DataFrame(stats_dict)
stats

###################################

for k in ['mmc','iota','occ3','occ4', 'occ2Xmeso']:
    print('# of coefficients missing for ', k, ': ', df_filtered['coef_'+k].isna().sum())

for k in ['mmc','iota','occ3','occ4', 'occ2Xmeso']:
    print('# missing categories for ', k, ': ', df_filtered[k].isna().sum())

df_filtered[['coef_mmc', 'coef_iota','coef_occ3', 'coef_occ4', 'coef_occ2Xmeso']].describe()
# removing the non-missing iota coefficients
df_filtered.loc[df_filtered['coef_iota'].notna(),['coef_mmc', 'coef_iota','coef_occ3', 'coef_occ4', 'coef_occ2Xmeso']].describe()


for k in ['mmc','iota','occ3','occ4', 'occ2Xmeso']:
    print(k,' unique values: ',df.loc[df.used_in_regression==1][[k]].value_counts().shape)


plt.figure(figsize=(10, 6))
sns.kdeplot(df_filtered['coef_iota'], bw_adjust=0.5, label='Iota', color='blue', fill=True)
sns.kdeplot(df_filtered['coef_mmc'], bw_adjust=0.5, label='MMC', color='red', fill=True)
sns.kdeplot(df_filtered['coef_occ4'], bw_adjust=0.5, label='Occ4', color='green', fill=True)
sns.kdeplot(df_filtered['coef_occ3'], bw_adjust=0.5, label='Occ3', color='yellow', fill=True)
sns.kdeplot(df_filtered['coef_occ2Xmeso'], bw_adjust=0.5, label='Occ2XMeso', color='purple', fill=True)
plt.xlim(-5, 5)  # Set the x-axis limits
plt.title('Density Plot of Coefficients by Classification (Weighted)')
plt.xlabel('Coefficient Values')
plt.ylabel('Density')
plt.legend()
plt.savefig(root + 'Results/trade_shock/coef_density_plot.pdf')
plt.show()


coef_summary_stats = pd.DataFrame()
coef_summary_stats['iota']      = df_filtered['coef_iota'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
coef_summary_stats['mmc']       = df_filtered['coef_mmc'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
coef_summary_stats['occ3']      = df_filtered['coef_occ3'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
coef_summary_stats['occ4']      = df_filtered['coef_occ4'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
coef_summary_stats['occ2Xmeso'] = df_filtered['coef_occ2Xmeso'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
print(coef_summary_stats)
coef_summary_stats = coef_summary_stats.apply(lambda x: round(x, 3) if x.dtype.kind in 'f' else x)
coef_summary_stats.to_excel(root + 'Results/trade_shock/coef_summary_stats.xlsx', sheet_name='Summary Stats', index_label='Statistic', engine='openpyxl')
coef_summary_stats.to_latex(root + 'Results/trade_shock/coef_summary_stats.tex')


plt.figure(figsize=(10, 6))
sns.kdeplot(df_filtered['se_iota'], bw_adjust=0.5, label='Iota', color='blue', fill=True)
sns.kdeplot(df_filtered['se_mmc'], bw_adjust=0.5, label='MMC', color='red', fill=True)
sns.kdeplot(df_filtered['se_occ4'], bw_adjust=0.5, label='Occ4', color='green', fill=True)
sns.kdeplot(df_filtered['se_occ3'], bw_adjust=0.5, label='Occ3', color='yellow', fill=True)
sns.kdeplot(df_filtered['se_occ2Xmeso'], bw_adjust=0.5, label='Occ2XMeso', color='purple', fill=True)
plt.xlim(-5, 5)  # Set the x-axis limits
plt.title('Density Plot of Standard Errors by Classification (Weighted)')
plt.xlabel('Standard Errors')
plt.ylabel('Density')
plt.legend()
plt.savefig(root + 'Results/trade_shock/se_density_plot.pdf')
plt.show()

se_summary_stats = pd.DataFrame()
se_summary_stats['iota']      = df_filtered['se_iota'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
se_summary_stats['mmc']       = df_filtered['se_mmc'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
se_summary_stats['occ3']      = df_filtered['se_occ3'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
se_summary_stats['occ4']      = df_filtered['se_occ4'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
se_summary_stats['occ2Xmeso'] = df_filtered['se_occ2Xmeso'].describe(percentiles=[0.01, 0.05, 0.1, 0.9, 0.95, 0.99])
print(se_summary_stats)
se_summary_stats = se_summary_stats.apply(lambda x: round(x, 3) if x.dtype.kind in 'f' else x)
se_summary_stats.to_excel(root + 'Results/trade_shock/se_summary_stats.xlsx', sheet_name='Summary Stats', index_label='Statistic', engine='openpyxl')
se_summary_stats.to_latex(root + 'Results/trade_shock/se_summary_stats.tex')




plt.figure(figsize=(10, 6))
sns.kdeplot(df_filtered['t_iota'], bw_adjust=0.5, label='Iota', color='blue', fill=True)
sns.kdeplot(df_filtered['t_mmc'], bw_adjust=0.5, label='MMC', color='red', fill=True)
sns.kdeplot(df_filtered['t_occ4'], bw_adjust=0.5, label='Occ4', color='green', fill=True)
sns.kdeplot(df_filtered['t_occ3'], bw_adjust=0.5, label='Occ3', color='yellow', fill=True)
sns.kdeplot(df_filtered['t_occ2Xmeso'], bw_adjust=0.5, label='Occ2XMeso', color='purple', fill=True)
plt.xlim(-5, 5)  # Set the x-axis limits
plt.title('Density Plot of T-Statistics by Classification (Weighted)')
plt.xlabel('T-Statistics')
plt.ylabel('Density')
plt.legend()
plt.savefig(root + 'Results/trade_shock/t_density_plot.pdf')
plt.show()

t_summary_stats = pd.DataFrame()
t_summary_stats['iota']      = df_filtered['t_iota'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
t_summary_stats['mmc']       = df_filtered['t_mmc'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
t_summary_stats['occ3']      = df_filtered['t_occ3'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
t_summary_stats['occ4']      = df_filtered['t_occ4'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
t_summary_stats['occ2Xmeso'] = df_filtered['t_occ2Xmeso'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
print(t_summary_stats)
t_summary_stats = t_summary_stats.apply(lambda x: round(x, 3) if x.dtype.kind in 'f' else x)
t_summary_stats.to_excel(root + 'Results/trade_shock/t_summary_stats.xlsx', sheet_name='Summary Stats', index_label='Statistic', engine='openpyxl')
t_summary_stats.to_latex(root + 'Results/trade_shock/t_summary_stats.tex')





plt.figure(figsize=(10, 6))
sns.kdeplot(df_filtered['count_iota'], bw_adjust=0.5, label='Iota', color='blue', fill=True)
sns.kdeplot(df_filtered['count_mmc'], bw_adjust=0.5, label='MMC', color='red', fill=True)
sns.kdeplot(df_filtered['count_occ4'], bw_adjust=0.5, label='Occ4', color='green', fill=True)
sns.kdeplot(df_filtered['count_occ3'], bw_adjust=0.5, label='Occ3', color='yellow', fill=True)
sns.kdeplot(df_filtered['count_occ2Xmeso'], bw_adjust=0.5, label='Occ2XMeso', color='purple', fill=True)
#plt.xlim(-5, 5)  # Set the x-axis limits
plt.title('Density Plot of Counts by Classification (Weighted)')
plt.xlabel('Counts')
plt.ylabel('Density')
plt.legend()
plt.savefig(root + 'Results/trade_shock/count_density_plot.pdf')
plt.show()

count_summary_stats = pd.DataFrame()
count_summary_stats['iota']      = df_filtered['count_iota'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
count_summary_stats['mmc']       = df_filtered['count_mmc'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
count_summary_stats['occ3']      = df_filtered['count_occ3'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
count_summary_stats['occ4']      = df_filtered['count_occ4'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
count_summary_stats['occ2Xmeso'] = df_filtered['count_occ2Xmeso'].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
print(count_summary_stats)
count_summary_stats = count_summary_stats.apply(lambda x: round(x, 3) if x.dtype.kind in 'f' else x)
count_summary_stats.to_excel(root + 'Results/trade_shock/count_summary_stats.xlsx', sheet_name='Summary Stats', index_label='Statistic', engine='openpyxl')
count_summary_stats.to_latex(root + 'Results/trade_shock/count_summary_stats.tex')



###########################################################################
# FINDINGS
# - We have much smaller standard errors and much larger t-stats
# Using iotas, most workers are in gorups that have statistically signficant effects. Using other classifications this isn't true. 
# Also we are powered to reject homogeneous treatment effects using iotas; not really powered to do so with other classifications


###########################################################################
# NEXT STEPS
# - What are characteristics of iotas with larger and smaller betas? Caan we do this qualitatively by tabulating occupation and geography for highest and lowest beta iotas
# Review what ADHS do and if it applies


def get_nth_frequency(df_filtered, n, sizecutoff):
    """
    Returns the frequency of 'cbo1994' and 'cbotrans' for the nth largest or smallest unique 'coef_iota',
    filtering the DataFrame for entries where 'count_iota' is at least 'sizecutoff'.

    Parameters:
    df_filtered (DataFrame): The DataFrame containing the data.
    n (int): Rank of the 'coef_iota'. Positive for nth largest, negative for nth smallest.
    sizecutoff (int): Minimum count of 'count_iota' to include a row in the analysis.

    Returns:
    countries: A pandas Series with the frequency count of 'cbo1994' and 'cbotrans'.
    """
    # Step 1: Filter rows where count_iota is greater than or equal to sizecutoff
    temp = df_filtered[df_filtered['count_iota'] >= sizecutoff]

    # Drop duplicates to ensure unique coef_iota for each iota
    unique_temp = temp.drop_duplicates(subset=['iota', 'coef_iota'])

    # Ensure n is not zero
    if n == 0:
        raise ValueError("n cannot be zero.")

    # Step 2: Find the iota value with the nth largest or smallest unique coef_iota depending on n
    if n > 0:
        if n > len(unique_temp):
            raise ValueError("n is greater than the number of available rows after filtering for largest.")
        nth_value_row = unique_temp.nlargest(n, 'coef_iota').iloc[-1]
    else:
        if -n > len(unique_temp):
            raise ValueError("n is greater than the number of available rows after filtering for smallest.")
        nth_value_row = unique_temp.nsmallest(-n, 'coef_iota').iloc[-1]

    # Print iota and its corresponding coef_iota value
    print(f"iota: {nth_value_row['iota']}, coef_iota: {nth_value_row['coef_iota']}")
    iota = nth_value_row['iota']
    coef_iota = nth_value_row['coef_iota']

    # Step 3: Restrict to the rows corresponding to this iota value
    final_df = temp[temp['iota'] == nth_value_row['iota']]

     # Step 4: Tabulate the frequencies of cbo1994 within these rows
    frequency_cbo1994 = final_df[['cbo1994', 'cbotrans']].value_counts().reset_index()
    #frequency_cbo1994.columns = ['cbo1994', 'cbotrans', 'count']

    # Calculate the share for each count
    frequency_cbo1994['share'] = frequency_cbo1994['count'] / frequency_cbo1994['count'].sum()

    return frequency_cbo1994, iota, coef_iota


for i in [-1,-2,1,2]:
    table, iota, coef = get_nth_frequency(df_filtered, i, 10000)
    print(table.head(10))
    print(iota)
    print(coef)










df_filtered[['coef_iota', 't_iota', 'E_tariff_by_iota_given_gamma_industry', 'E_tariff_by_iota_given_industry', 'E_tariff_by_mmc_given_industry', 'dlnonetariff_1990_1995']].corr()



# Density plot of Std Errors
plt.figure(figsize=(10, 6))
sns.kdeplot(coef_iota[0], bw_adjust=0.5, label='Iota', color='blue', fill=True)
sns.kdeplot(coef_mmc[0], bw_adjust=0.5, label='MMC', color='green', fill=True)
sns.kdeplot(coef_occ2Xmeso[0], bw_adjust=0.5, label='Occ2Xmeso', color='red', fill=True)
sns.kdeplot(coef_occ3[0], bw_adjust=0.5, label='Occ3', color='black', fill=True)
sns.kdeplot(coef_occ4[0], bw_adjust=0.5, label='SE Occ4', color='orange', fill=True)
plt.title('Density Plot of Coefficientss (Unweighted)')
plt.xlim(0,5)
plt.legend()



# Density plot of t-stats
plt.figure(figsize=(10, 6))
sns.kdeplot(t_iota, bw_adjust=0.5, label='T Iota', color='blue', fill=True)
sns.kdeplot(t_mmc, bw_adjust=0.5, label='T MMC', color='green', fill=True)
sns.kdeplot(t_occ2Xmeso, bw_adjust=0.5, label='T Occ2Xmeso', color='red', fill=True)
sns.kdeplot(t_occ3, bw_adjust=0.5, label='T Occ3', color='black', fill=True)
sns.kdeplot(t_occ4, bw_adjust=0.5, label='T Occ4', color='orange', fill=True)
plt.title('Density Plot of T-Stats (Unweighted)')
plt.legend()










# Density plot of Std Errors
plt.figure(figsize=(10, 6))
sns.kdeplot(se_iota[0], bw_adjust=0.5, label='SE Iota', color='blue', fill=True)
sns.kdeplot(se_mmc[0], bw_adjust=0.5, label='SE MMC', color='green', fill=True)
sns.kdeplot(se_occ2Xmeso[0], bw_adjust=0.5, label='SE Occ2Xmeso', color='red', fill=True)
sns.kdeplot(se_occ3[0], bw_adjust=0.5, label='SE Occ3', color='black', fill=True)
sns.kdeplot(se_occ4[0], bw_adjust=0.5, label='SE Occ4', color='orange', fill=True)
plt.title('Density Plot of Standard Errors (Unweighted)')
plt.xlim(0,5)
plt.legend()


# Cluster size distributions
summary_stats = pd.DataFrame()
summary_stats['iota'] = df_filtered['iota'].value_counts().describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
summary_stats['mmc'] = df_filtered['mmc'].value_counts().describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
summary_stats['occ3'] = df_filtered['occ3'].value_counts().describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
summary_stats['occ4'] = df_filtered['occ4'].value_counts().describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])
summary_stats['occ2Xmeso'] = df_filtered['occ2Xmeso'].value_counts().describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.75, 0.9, 0.95, 0.99])

pd.set_option('display.float_format', '{:.0f}'.format)
print(summary_stats)



result = smf.ols( 'd_log_rem_1990_2000 ~ dlnonetariff_1990_1995', data=df_filtered).fit()
print(result.summary2())
result = smf.ols( 'd_log_rem_1990_2000 ~ E_tariff_by_iota_given_gamma_industry', data=df_filtered).fit()
print(result.summary2())
result = smf.ols( 'd_log_rem_1990_2000 ~ E_tariff_by_iota_given_industry', data=df_filtered).fit()
print(result.summary2())
result = smf.ols( 'd_log_rem_1990_2000 ~ E_tariff_by_mmc_given_industry', data=df_filtered).fit()
print(result.summary2())

# Some useless summary stats


merged = beta_iota.merge(E_tariff_by_iota_given_gamma_industry, on='iota', indicator='_merge1')
merged = merged.merge(E_tariff_by_iota_given_industry, on='iota', indicator='_merge2')
merged[['E_tariff_by_iota_given_gamma_industry','E_tariff_by_iota_given_industry']].corr()
plt.scatter(merged['E_tariff_by_iota_given_gamma_industry'],merged['E_tariff_by_iota_given_industry'])

# Betas are uncorrelated with iota mean log wages
merged = merged.merge(df_filtered.groupby('iota')['log_rem_1990'].mean().reset_index().rename(columns={'log_rem_1990':'iota_mean_log_rem_1990'}), on='iota')
merged[['coef_iota', 'iota_mean_log_rem_1990']].corr()


merged = beta_iota.merge(E_tariff_by_iota.reset_index(), on='iota', indicator=True)
merged[['coef_iota','dlnonetariff_1990_1995']].corr()
plt.scatter(merged.coef_iota, merged.dlnonetariff_1990_1995)






##################################################################################################################
##################################################################################################################
# Try group-level regressions
##################################################################################################################
##################################################################################################################


iota_collapsed = df_filtered.groupby('iota')[['log_rem_1990','log_rem_2000']].mean().reset_index()
iota_collapsed['d_log_iota_earn_1990_2000'] = iota_collapsed['log_rem_2000'] - iota_collapsed['log_rem_1990']
iota_collapsed = iota_collapsed.merge(E_tariff_by_iota_given_gamma_industry, on='iota', indicator='_merge1')
result = smf.ols( 'd_log_iota_earn_1990_2000 ~ E_tariff_by_iota_given_gamma_industry', data=iota_collapsed).fit()
result.summary()


mmc_collapsed = df_filtered.groupby('mmc')[['log_rem_1990','log_rem_2000']].mean().reset_index()
mmc_collapsed['d_log_mmc_earn_1990_2000'] = mmc_collapsed['log_rem_2000'] - mmc_collapsed['log_rem_1990']
rtc_kume = pd.read_stata(root + 'Code/DixCarneiro_Kovak_2017/Data/rtc_kume.dta', columns=['mmc','rtc_kume_main'])
rtc_kume['mmc'] = rtc_kume['mmc'].astype(int)
mmc_collapsed = mmc_collapsed.merge(rtc_kume, on='mmc', how='left', indicator='_merge1')
result = smf.ols( 'd_log_mmc_earn_1990_2000 ~ rtc_kume_main', data=mmc_collapsed).fit()
result.summary()



# These regressions are wrong-signed. Likely because of selection on having earnings in the post-period
mmcEarnPremia_main_1986_2010 = pd.read_stata(root + 'Code/DixCarneiro_Kovak_2017/ProcessedData_RAIS/RegionalEarnPremia/mmcEarnPremia_main_1986_2010.dta')
mmcEarnPremia_main_1986_2010 = mmcEarnPremia_main_1986_2010[mmcEarnPremia_main_1986_2010['year'].isin([1990, 2000])]
# Pivot the DataFrame to wide format with years as columns
mmcEarnPremia_main_1986_2010 = mmcEarnPremia_main_1986_2010.pivot(index='mmc', columns='year', values='coeff_rem_dez')
# Calculate the difference between the years 2000 and 1990
mmcEarnPremia_main_1986_2010['diff_2000_1990'] = mmcEarnPremia_main_1986_2010[2000] - mmcEarnPremia_main_1986_2010[1990]
mmcEarnPremia_main_1986_2010.reset_index(inplace=True)
mmcEarnPremia_main_1986_2010['mmc'] = mmcEarnPremia_main_1986_2010['mmc'].astype(int)
mmc_collapsed = mmc_collapsed.merge(mmcEarnPremia_main_1986_2010, on='mmc', how='left', indicator='_merge2')

result = smf.ols( 'diff_2000_1990 ~ rtc_kume_main', data=mmc_collapsed).fit()
result.summary()

# Display the resulting DataFrame
print(pivot_df[['diff_2000_1990']])