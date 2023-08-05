#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  3 08:39:29 2021

@author: jfogel
"""

import torch
import pandas as pd
import pickle
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import sklearn
import os

diverging_colors = sns.color_palette("RdBu", 10)
cmap=sns.palplot(diverging_colors)

def correlogram(psi_hat, corrsavefile, histsavefile, cosine=False, p_ig=None):
    
    if p_ig!=None:
        psi_hat = psi_hat * p_ig[:,1:]
    
    if cosine==True:
        corrmatrix = sklearn.metrics.pairwise.cosine_similarity(psi_hat, dense_output=True)
    else: 
        #psi_hat_T = pd.DataFrame(np.array(torch.transpose(psi_hat,0,1)/psi_hat.mean(dim=1)))
        psi_hat_T = pd.DataFrame(np.array(torch.transpose(psi_hat, 0, 1)))
        corrmatrix = psi_hat_T.corr()
        
        
    mask = np.triu(np.ones_like(corrmatrix, dtype=bool))
    # We are currently omitting the diagonal since it will be all ones. This alternative includes the diagonal.
    #mask = 1-np.tril(np.ones_like(corrmatrix, dtype=bool))
    
    fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
    #sns.heatmap(df1.iloc[:, 1:6:], annot=True, linewidths=.5, ax=ax)
    sns.heatmap(
        corrmatrix, 
        mask = mask,
        vmin=-1, vmax=1, center=0,
        cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
        #cmap=sns.color_palette("coolwarm", n_colors=20),
        #cmap=sns.color_palette("RdBu", 100),
        square=False,
        xticklabels=False,
        yticklabels=False,
        ax=ax
    )
    #ax.set_aspect(1.2)
    ax.tick_params(axis='both', which='major', labelsize=18)
    ax.figure.savefig(corrsavefile, dpi=300, bbox_inches="tight")

    #Extract unique correlations coefficients (drop diagonal and upper triangle) and plot histogram
    a = np.tril(corrmatrix).flatten()
    coefs = a[(a!=1) & (a!=0)]
    coefs = np.round(coefs,8)   # Something weird is going on with rounding error in the specific skills benchmark
    
    fig, ax = plt.subplots(figsize=(5.76,4.8))
    ax.hist(coefs, density=False, bins=20)
    ax.set_xlim(-1,1)
    ax.text(.6, .9, r'Std Dev='+str(np.round(np.std(coefs), decimals=3)), fontsize=16, transform = ax.transAxes)
    #ax.tick_params(axis='both', which='major', labelsize=18)
    plt.show()    
    ax.figure.savefig(histsavefile, dpi=300, bbox_inches="tight")



for idx in [('iota','gamma'), ('occ4_first_recode','sector_IBGE'), ('occ4_first_recode','gamma')]:
    # using wtype_var and jtype_Var instead of worker_type_var and job_type_var so we don't reset the master versions set in do_all.py
    wtype_var = idx[0]
    jtype_var    = idx[1]
    print(wtype_var, jtype_var)
    mle_data_sums_corr_filename = homedir + "/Networks/RAIS_exports/earnings_panel/panel_rio_2009_2012_mle_data_sums_" + wtype_var + "_" + jtype_var + "_level_0.p"
    mle_data_sums_corr = pickle.load(open(mle_data_sums_corr_filename, "rb"), encoding='bytes')
    psi_hat = pickle.load(open(homedir + "/Networks/Code/june2021/MLE_estimates/panel_rio_2009_2012_psi_normalized_" + wtype_var + "_" + jtype_var + "_level_0_eta_2.p", "rb"), encoding='bytes')['psi_hat']    
    
    # Merge on mean wages by worker type and sort
    psi_hat_merged = torch.cat((torch.reshape(mle_data_sums_corr['mean_wage_i'],(mle_data_sums_corr['mean_wage_i'].shape[0],1)), psi_hat),dim=1)
    psi_hat_sorted = psi_hat_merged[psi_hat_merged[:, 0].sort()[1]][:,1:]

    corr_name_stub = 'correlograms_' + wtype_var + '_' + jtype_var
    hist_name_stub = 'correlograms_hist_' + wtype_var + '_' + jtype_var
    correlogram(psi_hat,        figuredir+corr_name_stub + '.png'                , figuredir+hist_name_stub + '.png')
    correlogram(psi_hat_sorted, figuredir+corr_name_stub + '_sorted.png'         , figuredir+hist_name_stub + '_sorted.png')
    correlogram(psi_hat_sorted, figuredir+corr_name_stub + '_sorted_weighted.png', figuredir+hist_name_stub + '_sorted_weighted.png', p_ig = mle_data_sums_corr['p_ig_actual'])
  
    
  
    
####################################################################################
# Extreme benchmarks

# 2 clusters
means = np.ones(50)
diagonal    = np.random.uniform(low=.5, high=2, size=(25,25))
offdiagonal = np.random.uniform(low=-1,  high=-.5,   size=(25,25))
cov = np.vstack((np.hstack((diagonal,offdiagonal)), np.hstack((np.transpose(offdiagonal),diagonal))))
psi_2_clusters = torch.tensor(np.transpose(np.random.multivariate_normal(means,cov, (50))) )
corr_name_stub = 'correlograms_benchmark_2_clusters'
hist_name_stub = 'correlograms_hist_benchmark_2_clusters'
correlogram(psi_2_clusters,        figuredir+corr_name_stub + '.png'                , figuredir+hist_name_stub + '.png')

# Highly correlated
psi_1_cluster =  torch.tensor(np.transpose(np.random.multivariate_normal(np.ones(50), np.random.uniform(low=10, high=10.5, size=(50,50)), (200))) )
corr_name_stub = 'correlograms_benchmark_1_cluster'
hist_name_stub = 'correlograms_hist_benchmark_1_cluster'
correlogram(psi_1_cluster,        figuredir+corr_name_stub + '.png'                , figuredir+hist_name_stub + '.png')

# Uncorrelated
psi_specific_skills = torch.tensor(np.diag(np.random.uniform(low=.5, high=2, size=(50))))
corr_name_stub = 'correlograms_benchmark_specific_skills'
hist_name_stub = 'correlograms_hist_benchmark_specific_skills'
correlogram(psi_specific_skills,        figuredir+corr_name_stub + '.png'                , figuredir+hist_name_stub + '.png')


