#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on August 13 2024
- Adapted from NetworksGit/Code/MarketPower/do_all_marketpower.py
- Goalis to delete unnecessary code to focus on simply computing distributions of HHIs and markdowns across different market definitions

@author: jfogel
"""


import os
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import getpass
from scipy.sparse import lil_matrix
from scipy.sparse import coo_matrix
from scipy.sparse import csr_matrix


homedir = os.path.expanduser('~')
if getpass.getuser()=='p13861161':
    if os.name == 'nt':
        homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
        root = homedir + '/labormkt_rafaelpereira/NetworksGit/'
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

# The 2013 to 2016 panel doesn't keep cnpj_raiz, which we need to compute HHIs following Mayara. I could probably re-run the create_earnings_panel to get it, but don't want to deal with that now 
#mle_data_filename      = root + "Data/derived/earnings_panel/panel_3states_2013to2016_new_level_0.csv"
mle_data_filename      = root + "Data/derived/earnings_panel/panel_3states_2009to2011_level_0.csv"


usecols = ['wid_masked', 'jid_masked', 'year', 'iota', 'gamma', 'cnpj_raiz','real_hrly_wage_dec', 'ln_real_hrly_wage_dec', 'codemun', 'occ2', 'occ2_first', 'code_meso', 'occ2Xmeso', 'occ2Xmeso_first']

data_full = pd.read_csv(mle_data_filename, usecols=usecols)
data_full = data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1) & (data_full.jid_masked!=-1)]
data_full = data_full.merge(muni_micro_cw, on='codemun', how='left', validate='m:1', indicator='_merge')

# Mayara defines markets as occ2-micros. 

# According to Mayara's equation (9) on page 11, the average markdown in labor market 

#Felix, p. 27: Appendix C.2.4 shows that the country-level average markdown—that is, the country- level ratio of (employment-weighted) average MRPL to (employment-weighted) average wage—is a weighted average of the market-level markdowns in Proposition 1, where the weights are each market’s payroll share of the country’s total payroll.




''' Ingredients for computing labor supply elasiticities according to our model:
 - $\Psi_{ig}$
 - 1/theta (our theta corresponds to Mayara's eta, her 1/eta=0.985)
 - 1/nu (our nu corresponds to Mayara's theta, her 1/theta=1.257)
 - s_ig = iota's share of market gamma employment
 - s_jg = job j's share of market gamma employment [Note: in overleaf, we call this s_ijg but I think the i subscript is confusing]
 - s_ij = s_ig*s_ijg = job j's share of type iota employment
'''


inv_eta_mayara   = 0.985
inv_theta_mayara = 1.257

eta_mayara = 1/inv_eta_mayara
theta_mayara = 1/inv_theta_mayara

eta_bhm = 7.14
theta_bhm = .45


# Create variable for Mayara's market definitions
data_full['mkt_mayara'] = data_full.groupby(['occ2', 'code_micro']).ngroup()


def compute_payroll_weighted_share(df, firm_col, market_col, pay_col):
    # Compute total pay for each firm within each market
    firm_total_pay = df.groupby([market_col, firm_col])[pay_col].sum().reset_index()
    # Compute total pay for each market
    market_total_pay = df.groupby(market_col)[pay_col].sum().reset_index()
    # Merge the two DataFrames on 'market'
    merged = pd.merge(firm_total_pay, market_total_pay, on=market_col, suffixes=('_firm', '_market'))
    # Compute the payroll-weighted share for each firm
    merged['payroll_weighted_share'] = merged[pay_col + '_firm'] / merged[pay_col + '_market']
    return merged[[firm_col, market_col, 'payroll_weighted_share']]


def compute_payroll_weighted_share(df, firm_col, market_col, pay_col):
    # Get all unique firms and markets
    all_firms = df[firm_col].unique()
    all_markets = df[market_col].unique()
    # Create a DataFrame with all possible firm-market combinations
    all_combinations = pd.DataFrame([(firm, market) for firm in all_firms for market in all_markets], columns=[firm_col, market_col])
    # Compute total pay for each firm within each market
    firm_total_pay = df.groupby([market_col, firm_col])[pay_col].sum().reset_index()
    # Compute total pay for each market
    market_total_pay = df.groupby(market_col)[pay_col].sum().reset_index()

    # Merge the all_combinations with firm_total_pay
    merged = pd.merge(all_combinations, firm_total_pay, on=[market_col, firm_col], how='left')
    # Fill NaN values with 0 for firms that don't exist in certain markets
    merged[pay_col] = merged[pay_col].fillna(0)
    # Merge with market_total_pay
    merged = pd.merge(merged, market_total_pay, on=market_col, suffixes=('_firm', '_market'))
    # Compute the payroll-weighted share for each firm
    merged['payroll_weighted_share'] = merged[pay_col + '_firm'] / merged[pay_col + '_market']
    # Replace NaN values with 0 (this handles cases where market total pay is 0)
    #merged['payroll_weighted_share'] = merged['payroll_weighted_share'].fillna(0)
    return merged[[firm_col, market_col, 'payroll_weighted_share']]



s_ij = compute_payroll_weighted_share(data_full, 'iota', 'jid_masked', 'real_hrly_wage_dec')
s_jg = compute_payroll_weighted_share(data_full, 'jid_masked', 'gamma', 'real_hrly_wage_dec')
s_fm = compute_payroll_weighted_share(data_full, 'cnpj_raiz', 'mkt_mayara', 'real_hrly_wage_dec')
s_gi = compute_payroll_weighted_share(data_full, 'gamma', 'iota', 'real_hrly_wage_dec')

# This is the quantity Ben and Bernardo derived on the white board on 8/14
# - Numerator: for each iota compute the total (hourly) earnings for that iota in job j. Raise this to (1+eta). Then sum these quantities over all jobs j in market gamma and raise this quantity to the (1+theta)/(1+eta).
# Denominator: Compute the numerator for each market gamma and sum over all markets gamma
# - The result will be one value for each iota, all of which sum to 1.

def compute_s_gammaiota(data_full, eta, theta):
    # Get all unique firms and markets
    all_iotas  = data_full['iota'].unique()
    all_gammas = data_full['gamma'].unique()
    # Create a DataFrame with all possible firm-market combinations
    all_combinations = pd.DataFrame([(iota, gamma) for iota in all_iotas for gamma in all_gammas], columns=['iota', 'gamma'])
    
    # Group by iota and job, and sum hourly earnings
    job_earnings = data_full.groupby(['iota', 'jid_masked','gamma'])['real_hrly_wage_dec'].sum().reset_index()
    # Compute the (1+eta) power of earnings
    job_earnings['earnings_powered'] = job_earnings['real_hrly_wage_dec'] ** (1 + eta)
    # Group by iota and market (gamma), and sum the powered earnings
    market_earnings = job_earnings.groupby(['iota', 'gamma'])['earnings_powered'].sum().reset_index()
    # Compute the (1+theta)/(1+eta) power of the sum
    market_earnings['market_sum_powered'] = market_earnings['earnings_powered'] ** ((1 + theta) / (1 + eta))
    numerator = market_earnings[['iota', 'gamma', 'market_sum_powered']]
    # Compute the denominator: sum of numerators over all gammas within each iota
    denominator = numerator.groupby('iota')['market_sum_powered'].sum().reset_index()

    # Merge the all_combinations with numerator on iota and gamma 
    merged = pd.merge(all_combinations, numerator, on=['iota', 'gamma'], how='left')
    merged['market_sum_powered'] = merged['market_sum_powered'].fillna(0)
    # Merge with denominator on iota
    merged = pd.merge(merged, denominator, on='iota', suffixes=('_gi', '_i'))
    # Compute the payroll-weighted share for each gamma-iota
    merged['s_gammaiota'] = merged['market_sum_powered_gi'] / merged['market_sum_powered_i']

    return merged[['iota', 'gamma', 's_gammaiota']]

s_gi_hat = compute_s_gammaiota(data_full, eta_bhm, theta_bhm)
print(s_gi_hat['s_gammaiota'].sum())
print(s_gi_hat['s_gammaiota'].sort_values().tail(446).describe())


s_gi_hat = compute_s_gammaiota(data_full, 0, 0)
print(s_gi_hat['s_gammaiota'].sum())
print(s_gi_hat['s_gammaiota'].sort_values().tail(446).describe())



jid_gamma_cw = data_full[['jid_masked', 'gamma']].drop_duplicates()
# Note that s_ij corresponds to pi_{j \iota} on OVerleaf. Need to clean up notation.
product = s_ij.merge(jid_gamma_cw, on='jid_masked', how='left', validate='m:1', indicator='_merge1')
product = product.merge(s_gi_hat, on=['iota','gamma'], how='left', validate='m:1', indicator='_merge2')
product['product'] = product['s_gammaiota'] * product['payroll_weighted_share']
sum_product = product.groupby('jid_masked')['product'].sum()

# This is critical to make sure the columns being summed have the same index
s_jg = s_jg.set_index('jid_masked', verify_integrity=True)

epsilon_j_bhm = eta_bhm * (1 - s_jg['payroll_weighted_share']) + theta_bhm * s_jg['payroll_weighted_share'] * (1 - sum_product)

epsilon_j_bhm = eta_bhm * s_jg['payroll_weighted_share'] + theta_bhm * s_jg['payroll_weighted_share'] * (1 - sum_product)





# Calculate the squared payroll-weighted share for each firm
s_jg['squared_share'] = s_jg['payroll_weighted_share'] ** 2
s_fm['squared_share'] = s_fm['payroll_weighted_share'] ** 2


# Sum up the squared shares within each market (gamma) to get the HHI
HHI = s_jg.groupby('gamma')['squared_share'].sum().reset_index()
HHI.columns = ['gamma', 'HHI']
HHI['count'] = data_full.groupby(['gamma']).gamma.count()
# This is the markdown according to Mayara's model, but using gammas as the definition of a market. 
markdown = 1 + 1/theta * HHI.HHI + 1/eta *(1-HHI.HHI)
print(markdown.describe())


HHI_mayara = s_fm.groupby('mkt_mayara')['squared_share'].sum().reset_index()
HHI_mayara.columns = ['mkt_mayara', 'HHI']
HHI_mayara['count'] = data_full.groupby(['mkt_mayara']).mkt_mayara.count()
# This is the markdown according to Mayara's model, but using gammas as the definition of a market. 
markdown_mayara = 1 + 1/theta * HHI_mayara.HHI + 1/eta *(1-HHI_mayara.HHI)
print(markdown_mayara.describe())



def plot_markdowns_multiple(input_tuples):
    plt.figure(figsize=(12, 6))
    
    for i, (HHI, eta, theta, weights, label) in enumerate(input_tuples):

        markdown = 1 + 1/theta * HHI + 1/eta * (1-HHI)
        
        print(f"Markdown statistics for input {i+1}:")
        print(markdown.describe())
        print("\n")
        
        color = plt.cm.rainbow(i / len(input_tuples))
        
        if weights is None:
            sns.histplot(markdown, kde=True, color=color, alpha=0.5, label=label)
        else:
            sns.histplot(x=markdown, weights=weights, kde=True, color=color, alpha=0.5, label=label)
    
    plt.title('Markdowns Histogram')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.legend()
    
    plt.show()
    plt.savefig('markdowns_histogram_multiple.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return markdown

# Example usage:
# Assuming HHI, HHI_mayara, eta, and theta are defined

input_tuples1 = [
    (HHI.HHI, 1.015, 0.796, HHI['count'], 'Our HHI; Mayara parameters'),
    (HHI_mayara.HHI, 1.015, 0.796, HHI_mayara['count'], 'Mayara HHI; Mayara parameters'),
    # Add more tuples as needed
]

plot_markdowns_multiple(input_tuples1)


input_tuples2 = [
    (HHI.HHI, 7.14, .45, HHI['count'], 'Our HHI; BHM parameters'),
    (HHI.HHI, 1.015, 0.796, HHI['count'], 'Our HHI; Mayara parameters'),
    # Add more tuples as needed
]

markdowns = plot_markdowns_multiple(input_tuples2)


input_tuples3 = [
    (HHI.HHI,        7.14, .45, HHI['count'], 'Our HHI; BHM parameters'),
    (HHI_mayara.HHI, 7.14, .45, HHI_mayara['count'], 'Mayara HHI; BHM parameters'),
    # Add more tuples as needed
]
markdowns = plot_markdowns_multiple(input_tuples3)



# Unweighted
if 1==1:
    plt.figure(figsize=(12, 6))
    
    # Plot histogram for 'markdown'
    sns.histplot(HHI.HHI, kde=True, color='blue', alpha=0.5, label='HHI (gamma)')
    
    # Overlay histogram for 'markdown_mayara'
    sns.histplot(HHI_mayara.HHI, kde=True, color='red', alpha=0.5, label='HHI (Mayara)')
    
    plt.title('Histogram Comparison')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.legend()
    
    # Save the figure
    plt.show()
    plt.savefig('histogram_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()


# Weighted
if 1==1:
    plt.figure(figsize=(12, 6))
    
    # Plot weighted histogram for HHI
    sns.histplot(x=HHI.HHI, weights=HHI['count'], kde=True, color='blue', alpha=0.5, label='HHI')
    
    # Overlay weighted histogram for HHI_mayara
    sns.histplot(x=HHI_mayara.HHI, weights=HHI_mayara['count'], kde=True, color='red', alpha=0.5, label='HHI Mayara')
    
    plt.title('Weighted Histogram Comparison of HHI')
    plt.xlabel('HHI Values')
    plt.ylabel('Weighted Frequency')
    plt.legend()
    
    # Optionally, set x-axis limits if you want to focus on a specific range
    # plt.xlim(0, 10000)  # Adjust these values as needed
    
    # Save the figure
    plt.show()
    plt.savefig('weighted_hhi_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()


XX Add histograms of markdowns. Then redo using BHM estimates (see below)



#################################
# Simulations to show the effect of misclassification on estimates of eta and theta
# - Use 2009-2011 to estimate Psi
# - Use BHM's estimates of eta and theta:





