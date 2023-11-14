#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 17:06:27 2021

@author: jfogel
"""
import pandas as pd
import numpy as np
import torch

def dgp(mle_data_filename, mle_data_sums, phi, sigma, equi, firstyear, lastyear, nrows=None, replaceyear=None, persistent=False):
    
    real_data = pd.read_csv(mle_data_filename, usecols=['wid_masked','year','iota','gamma','c','occ4_first_recode', 'sector_IBGE', 'ln_real_hrly_wage_dec'], nrows=nrows)
    #real_data = pd.read_csv(mle_data_filename, usecols=['wid_masked','year','iota','gamma','c','occ4_first_recode', 'sector_IBGE', 'ln_real_hrly_wage_dec'])
    real_data = real_data[(real_data['gamma']!=-1) & (real_data['iota']!=-1)]

    real_data = real_data.loc[(real_data.year>=firstyear) & (real_data.year<=lastyear)]
    
    S = int(real_data.sector_IBGE.max())
    
    # creating a matrix with proportion of sector IBGE per each gamma
    sector_prop = real_data[['sector_IBGE', 'gamma']].copy()
    sector_prop['count'] = 1
    sector_prop = sector_prop.pivot_table(columns='sector_IBGE', index='gamma', aggfunc='count')
    sector_prop = sector_prop.fillna(0)
    sector_prop = sector_prop.div(sector_prop.sum(axis=1), axis=0)
    
    ##########################
    # Creating log Phi matrix
    log_phi = pd.DataFrame(np.array(torch.log(phi)))
    # Start indices at 1, not 0
    log_phi.index = np.arange(1, len(log_phi)+1)
    log_phi.columns = np.arange(1,len(log_phi.columns)+1)
    log_phi_df = pd.melt(log_phi.reset_index().rename(columns={'index':'iota'}), id_vars='iota', var_name='gamma', value_name='log_phi')
    
    
    
    iotas_and_occs = real_data.drop(columns = ['gamma', 'sector_IBGE']).sort_values(by=['iota','wid_masked','year']).reset_index().drop(columns='index')
    
    iotas_and_occs['gamma'] = np.nan
    iotas_and_occs['ln_real_hrly_wage_dec'] = np.nan
    iotas_and_occs['sector_IBGE'] = np.nan
    
    iota_counts = iotas_and_occs.index.to_series().groupby(iotas_and_occs['iota']).agg(['count']).reset_index()
    
    #######################
    # Draw gammas
    for idx_iota in range(1,mle_data_sums['I']+1):
        iota_count      = iota_counts.loc[iota_counts.iota==idx_iota]['count'].values # Number of obs in this iota
        choice_temp = np.random.choice(mle_data_sums['G']+1, size=iota_count, replace=True, p=equi['p_ig'][idx_iota-1,:])
        iotas_and_occs.loc[iotas_and_occs.iota == idx_iota, 'gamma'] = choice_temp
    
    # Make gammas persistent whenever c==0
    if persistent==True:
        iotas_and_occs.loc[iotas_and_occs.c==0, 'gamma'] = np.nan
        iotas_and_occs['gamma'] = iotas_and_occs['gamma'].ffill()
        
    
    #######################
    # Draw sectors
    iotas_and_occs.sort_values(by=['gamma'], inplace=True)
    gamma_counts =iotas_and_occs.gamma.value_counts().reset_index().rename(columns={'index':'gamma','gamma':'count'}).sort_values(by='gamma')
    for idx_gamma in range(1,mle_data_sums['G']+1):
        gamma_count     = gamma_counts.loc[gamma_counts.gamma==idx_gamma]['count'].values # Number of obs in this gamma
        # The issue here is that some very small gammas are never observed in 2014. This me
        if sector_prop[sector_prop.index == idx_gamma].empty==False:
            sector_temp = np.random.choice(np.arange(1,S+1), size=gamma_count, replace=True, p=sector_prop[sector_prop.index == idx_gamma].values[0])
        else:
            sector_temp = 12 # I just chose the modal sector (sector_prop.sum()).  This is super sloppy but it's for rare cases so whatever. 
        iotas_and_occs.loc[iotas_and_occs.gamma==idx_gamma, 'sector_IBGE'] = sector_temp
    
    
    iotas_and_occs.sort_values(by=['iota','wid_masked','year'], inplace=True)
    
    # Make sectors persistent whenever c==0. Need to be sorted by wid and year first.
    if persistent==True:
        iotas_and_occs.loc[iotas_and_occs.c==0,'sector_IBGE'] = np.nan
        iotas_and_occs.loc[iotas_and_occs.c==0,'sector_IBGE'] = np.nan
        iotas_and_occs.loc[iotas_and_occs.gamma==0,'sector_IBGE'] = 999   # Need a temporary filler for when gamma=0 to prevent sector from being filled forward 
        iotas_and_occs['sector_IBGE'] = iotas_and_occs['sector_IBGE'].ffill()
        iotas_and_occs.loc[iotas_and_occs['sector_IBGE','sector_IBGE']==999] = np.nan

        
    iotas_and_occs = iotas_and_occs.merge(log_phi_df, on=['iota','gamma'], how="left", validate='m:1', indicator=False)
    
    #######################
    # Draw log earnings
    iotas_and_occs['ln_real_hrly_wage_dec'] = np.random.lognormal(mean = iotas_and_occs.log_phi, sigma = sigma, size=len(iotas_and_occs))
    iotas_and_occs['real_hrly_wage_dec'] = np.exp(iotas_and_occs['ln_real_hrly_wage_dec'])
    
    if replaceyear!=None:
        iotas_and_occs['year'] = replaceyear
    
    
    return iotas_and_occs


####################################################################################################################################
# Another (possibly related) issue is that we are not currently doing generating persistent matches. That is,
#   to really match the model we should be drawing c, or at least ensuring that gamma doesn't change unless c=1.
#   The latter shouldn't be too hard. Could be done by setting gamma=nan when c=0 and then doing something 
#   equivalent to carryforward
#
# After discussing with Bernardo on 6/23/2021 we don't think this actually matters since the model assumes that 
#   draws of c are orthogonal to everything. But we still could make this change

