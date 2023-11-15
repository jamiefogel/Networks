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

def correlogram(psi_hat, corrsavefile, histsavefile, cosine=False, p_ig=None, return_correlations=False):
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
    plt.close()
    if return_correlations==True:
        return coefs, corrmatrix

