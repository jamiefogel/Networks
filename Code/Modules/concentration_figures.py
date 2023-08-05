#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 14:04:06 2021

@author: jfogel
"""

from matplotlib.collections import LineCollection


################
## Make into function or loop and then incorporate occ2Xmeso
##################



import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def concentration_figures(xvar, xvarlabel, yvarlist, yvarlabels, savefile, weighted=True):
    # Example usage: 
    #xvar = 'iota'
    #xvarlabel = 'Workers'
    #yvarlist = ['sector_IBGE','gamma']
    #yvarlabels = {'sector_IBGE':'Sector','gamma':'Market'}
    fig, ax = plt.subplots()
    savename_str = ''
    for yvar in yvarlist:
        print(yvar)
        crosstab = pd.crosstab(index = data_full[xvar], columns = data_full[yvar])
        yvar_probabilities_by_xvar = crosstab.div(crosstab.sum(axis=1),axis=0).reset_index()
        yvar_probabilities_by_xvar['hhi'] = yvar_probabilities_by_xvar.drop(columns=xvar).pow(2).sum(axis=1)
        xvar_counts = data_full[xvar].value_counts().reset_index().rename(columns={'index':xvar,xvar:'count'})
        # Normalize the counts by the minimum count and round to an integer. This avoids having so many points in the scatter plot that it won't print.
        xvar_counts['count'] = np.round(xvar_counts['count']/xvar_counts['count'].min())
        yvar_probabilities_by_xvar = yvar_probabilities_by_xvar.merge(xvar_counts, on=xvar)
        if weighted==False:
            hhis = yvar_probabilities_by_xvar['hhi'].sort_values()
            wgt = ''
        if weighted==True:
            hhis = yvar_probabilities_by_xvar.loc[yvar_probabilities_by_xvar.index.repeat(yvar_probabilities_by_xvar['count'])]['hhi'].sort_values()
            wgt = '_wgt'
        ax.scatter(np.arange(1,hhis.shape[0]+1), hhis,s=3,label=yvarlabels[yvar])
        del crosstab, yvar_probabilities_by_xvar, xvar_counts, hhis
    ax.set_xlabel(xvarlabel)
    ax.set_ylabel('Concentration (HHI)')
    ax.set_xticklabels([])
    ax.set_xticks([])
    ax.set_ylim(0,1)
    ax.legend()
    ax.figure.savefig(savefile, dpi=300,bbox_inches='tight') # Used by paper and slides


