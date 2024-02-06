#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 17:06:27 2021


Edit 12/13/2023: 
- allow user to specify which worker types and job types (in addition to iota and gamma) to include in final data. Worker types are just kept from the raw data and then job types are drawn as we draw sectors from the gamma-sector and gamma-occ2Xmeso distributions
- Deleted the 'persistent' option because we don't actually use it

@author: jfogel
"""
import pandas as pd
import numpy as np
import torch

def dgp(mle_data_filename, mle_data_sums, phi, sigma, equi, firstyear, lastyear, sector_var='sector_IBGE', wtypes_to_impute=['occ2Xmeso_first_recode'], jtypes_to_impute=['occ2Xmeso_recode'], nrows=None, replaceyear=None):
    
    real_data = pd.read_csv(mle_data_filename, usecols=['wid_masked','year','iota','gamma','c', sector_var, 'ln_real_hrly_wage_dec'] + wtypes_to_impute + jtypes_to_impute, nrows=nrows)
    #real_data = pd.read_csv(mle_data_filename, usecols=['wid_masked','year','iota','gamma','c','occ4_first_recode', sector_var, 'ln_real_hrly_wage_dec'])
    real_data = real_data[(real_data['gamma']!=-1) & (real_data['iota']!=-1)]
    real_data = real_data.loc[(real_data.year>=firstyear) & (real_data.year<=lastyear)]
    
    ##########################
    # Creating log Phi matrix
    log_phi = pd.DataFrame(np.array(torch.log(phi)))
    # Start indices at 1, not 0
    log_phi.index = np.arange(1, len(log_phi)+1)
    log_phi.columns = np.arange(1,len(log_phi.columns)+1)
    log_phi_df = pd.melt(log_phi.reset_index().rename(columns={'index':'iota'}), id_vars='iota', var_name='gamma', value_name='log_phi')
    
    
    
    iotas_and_occs = real_data.drop(columns = ['gamma', sector_var] + jtypes_to_impute).sort_values(by=['iota','wid_masked','year']).reset_index().drop(columns='index')
    
    iotas_and_occs['gamma'] = np.nan
    iotas_and_occs['ln_real_hrly_wage_dec'] = np.nan
    iotas_and_occs[sector_var] = np.nan
    
    iota_counts = iotas_and_occs.index.to_series().groupby(iotas_and_occs['iota']).agg(['count']).reset_index()
    
    #######################
    # Draw gammas
    for idx_iota in range(1,mle_data_sums['I']+1):
        iota_count      = iota_counts.loc[iota_counts.iota==idx_iota]['count'].values # Number of obs in this iota
        choice_temp = np.random.choice(mle_data_sums['G']+1, size=iota_count, replace=True, p=equi['p_ig'][idx_iota-1,:])
        iotas_and_occs.loc[iotas_and_occs.iota == idx_iota, 'gamma'] = choice_temp
    

    iotas_and_occs.sort_values(by=['gamma'], inplace=True)
    gamma_counts =iotas_and_occs.gamma.value_counts().reset_index().rename(columns={'index':'gamma','gamma':'count'}).sort_values(by='gamma')

        
    #######################
    # Draw other job classifications

    jtypes_dict = {}
    for jtype in [sector_var] + jtypes_to_impute:
        N = int(real_data[jtype].nunique())
        jtype_mode = int(real_data[jtype].mode())
        
        # creating a matrix with proportion of jtype per each gamma
        jtype_prop = real_data[[jtype, 'gamma']].copy()
        jtype_prop['count'] = 1
        jtype_prop = jtype_prop.pivot_table(columns=jtype, index='gamma', aggfunc='count')
        jtype_prop = jtype_prop.fillna(0)
        jtype_prop = jtype_prop.div(jtype_prop.sum(axis=1), axis=0)
            
        for idx_gamma in range(1,mle_data_sums['G']+1):
            gamma_count     = gamma_counts.loc[gamma_counts.gamma==idx_gamma]['count'].values # Number of obs in this gamma
            # The issue here is that some very small gammas are never observed in 2014. This me
            if jtype_prop[jtype_prop.index == idx_gamma].empty==False:
                jtype_temp = np.random.choice(np.arange(1,N+1), size=gamma_count, replace=True, p=jtype_prop[jtype_prop.index == idx_gamma].values[0])
            else:
                jtype_temp = jtype_mode # I just chose the modal sector (jtype_prop.sum()).  This is super sloppy but it's for rare cases so whatever. 
            iotas_and_occs.loc[iotas_and_occs.gamma==idx_gamma, jtype] = jtype_temp
        
        
    iotas_and_occs.sort_values(by=['iota','wid_masked','year'], inplace=True)
        
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

