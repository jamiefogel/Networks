#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  8 13:46:53 2021

@author: jfogel
"""

import pandas as pd
import numpy as np
import os
import pickle
import sys
import torch
from os.path import expanduser

### Values to be defined elsewhere

def mle_load_fulldata(mle_data_filename, mle_data_sums_filename, worker_type_var, job_type_var, mle_firstyear=np.nan, mle_lastyear=np.nan):
    print('Computing sums for' + worker_type_var + ' and ' + job_type_var)

    # Confirm that worker_type_var and job_type_var exist in the data frame before fully loading it
    columns = pd.read_csv(mle_data_filename, nrows=1).columns
    # Add occ2 and occ4 variables to list of columns since they are created below and therefore shouldn't cause the check to fail
    columns = columns.tolist() + ['occ2_first_recode','occ4_first_recode']
    missing_columns = [var for var in [worker_type_var, job_type_var] if var not in columns]
    if missing_columns:
        raise ValueError(f"The following columns are missing in the CSV file: {', '.join(missing_columns)}")

    
    data_full = pd.read_csv(mle_data_filename)

    if 'cbo2002_first_recode' in data_full.columns: # This variable didn't exist in some older versions of the raw data
        data_full['cbo2002_first_recode'] = data_full.groupby('cbo2002_first').grouper.group_info[0]
        
    if np.isnan(mle_firstyear)==False & np.isnan(mle_lastyear)==False:
        data_full = data_full.loc[(data_full['year']>=mle_firstyear) & (data_full['year']<=mle_lastyear)]
        

    # Create occ2 variables if needed
    if (worker_type_var=='occ2_first_recode') | (job_type_var=='occ2_first_recode'):
        data_full['occ2_first']         = data_full.cbo2002_first.astype(str).str[:2].astype(int)
        data_full['occ2_first_recode']  = data_full.cbo2002_first.astype(str).str[:2].rank(method='dense').astype(int)

    # Create occ4 variables if needed    
    if (worker_type_var=='occ4_first_recode') | (job_type_var=='occ4_first_recode'):
        data_full['occ4_first']         = data_full.cbo2002_first.astype(str).str[:4].astype(int)
        # Set occ4s that rarely occur to missing. The cutoff at 5000 is totally arbitrary
        data_full['occ4_first'].loc[data_full.groupby('occ4_first')['occ4_first'].transform('count') < 5000] = np.nan
        # Recode occ4s to go from 1 to I. 
        data_full['occ4_first_recode']  = data_full.occ4_first.rank(method='dense', na_option='keep')
        # Recode missings to -1
        data_full['occ4_first'].loc[np.isnan(data_full.occ4_first)] = -1
        data_full['occ4_first_recode'].loc[np.isnan(data_full.occ4_first_recode)] = -1
        # Convert from float to int (nans are automatically stored as floats so when we added nans it converted to floats)
        data_full['occ4_first']         = data_full['occ4_first'].astype(int)
        data_full['occ4_first_recode']  = data_full['occ4_first_recode'].astype(int)
        
        
    if worker_type_var=='1':
        data_full['worker_type'] = 1
        data_full['worker_type'].loc[data_full['iota']==-1] = -1
    else: 
        data_full['worker_type'] = data_full[worker_type_var]
        
    if job_type_var=='1':
        data_full['job_type'] = 1
        data_full['job_type'].loc[data_full['gamma']== 0] = 0
        data_full['job_type'].loc[data_full['gamma']==-1] = -1
    else:
        data_full['job_type']    = data_full[job_type_var]
   
    data_full.loc[data_full['gamma']==0, 'job_type'] = 0 #Make sure the job type is set to 0 for non-employed
    
    # JSF: I think we want to keep gamma and iota hard-coded here because we're going to want to keep using this restriction for sample definition, but subsequently we will allow the user to specify worker and job type vars.
    data_full_levels = data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1)]
    data_full_levels = data_full_levels.loc[(data_full['job_type']!=-1) & (data_full['worker_type']!=-1)] #This is sloppy but for now I'm adding this so that we can set occ4_first_recode=-1 for occ4s that appear very rarely
    data_full_levels['temp2']    = np.square(data_full_levels['ln_real_hrly_wage_dec'])
    data_full_levels['count']    = 1
    
    data_levels = data_full_levels.groupby(['worker_type','job_type'])['count'].count().reset_index()
    data_levels['omega']    = list(data_full_levels.groupby(['worker_type','job_type'])['ln_real_hrly_wage_dec'].sum()) # Not sure why converting to a list is necessary but it works
    data_levels['omega_sq'] = list(data_full_levels.groupby(['worker_type','job_type'])['temp2'].sum())
    data_levels['c']        = list(data_full_levels.groupby(['worker_type','job_type'])['c'].sum())
      
    # We're getting some weird warning messages but I don't think they actually matter. See the "False positives" section here: https://www.dataquest.io/blog/settingwithcopywarning/ or see here: https://stackoverflow.com/questions/42105859/pandas-map-to-a-new-column-settingwithcopywarning
    
    # Keep this one hard coded too
    data_full_logs = data_full.loc[(data_full['gamma']>0)   & (data_full['iota']!=-1)]
    data_full_logs = data_full_logs.loc[(data_full['job_type']!=-1) & (data_full['worker_type']!=-1)]
    data_full_logs['temp']     = np.log(data_full_logs['ln_real_hrly_wage_dec'])
    data_full_logs['temp2']    = np.square(data_full_logs['temp'])
    data_full_logs['count']    = 1
    
    data_logs = data_full_logs.groupby(['worker_type','job_type'])['count'].count().reset_index()
    data_logs['logomega']    = list(data_full_logs.groupby(['worker_type','job_type'])['temp'].sum())
    data_logs['logomega_sq'] = list(data_full_logs.groupby(['worker_type','job_type'])['temp2'].sum())
   #XX Fix this
    
    # I confirmed that these match the relevant quantities in R
    sum_c_ig        = data_levels.pivot_table(index=["worker_type"], columns='job_type', values='c').values
    sum_omega_ig    = data_levels.pivot_table(index=["worker_type"], columns='job_type', values='omega').values
    sum_omega_sq_ig = data_levels.pivot_table(index=["worker_type"], columns='job_type', values='omega_sq').values
    sum_count_ig    = data_levels.pivot_table(index=["worker_type"], columns='job_type', values='count').values
    sum_logomega_ig    = data_logs.pivot_table(index=["worker_type"], columns='job_type', values='logomega').values
    sum_logomega_sq_ig = data_logs.pivot_table(index=["worker_type"], columns='job_type', values='logomega_sq').values
    
    # The R code included the option na.rm=T. Do we need something equivalent here? Shouldn't matter for level 3, but might matter for lower levels. 
    sum_omega       = np.nansum(sum_omega_ig)
    sum_omega_sq    = np.nansum(sum_omega_sq_ig)
    sum_logomega    = np.nansum(sum_logomega_ig)
    sum_logomega_sq = np.nansum(sum_logomega_sq_ig)
    
    obs_all         = np.nansum(sum_count_ig)    
    obs_employ      = np.nansum(sum_count_ig[:,1:])
    
    sum_c_i = np.nansum(sum_c_ig, axis=1)
    sum_c_g = np.nansum(sum_c_ig, axis=0)    
    
    sum_count_i= np.nansum(sum_count_ig, axis=1)
    sum_count_g= np.nansum(sum_count_ig, axis=0)


    sum_count_ig       = np.nan_to_num(sum_count_ig)
    sum_c_ig           = np.nan_to_num(sum_c_ig)
    sum_omega_ig       = np.nan_to_num(sum_omega_ig)
    sum_omega_sq_ig    = np.nan_to_num(sum_omega_sq_ig)
    sum_logomega_ig    = np.nan_to_num(sum_logomega_ig)
    sum_logomega_sq_ig = np.nan_to_num(sum_logomega_sq_ig)
    
    I = sum_c_ig.shape[0]
    G = sum_c_ig.shape[1]-1
    
    m_i = sum_count_i/(sum_count_i).sum()
    
    # We don't actually use these for the MLE but these are useful elsewhere
    mean_wage_g = np.array(data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1)].groupby(['job_type'])['real_hrly_wage_dec'].mean())
    mean_wage_i = np.array(data_full.loc[(data_full['job_type']!=-1) & (data_full['worker_type']!=-1)].groupby(['worker_type'])['real_hrly_wage_dec'].mean())
    
    mle_data_sums = {
        'sum_c_ig'            : torch.tensor(sum_c_ig,          requires_grad=False),
        'sum_c_i'             : torch.tensor(sum_c_i,   	requires_grad=False),
        'sum_c_g'             : torch.tensor(sum_c_g,           requires_grad=False),
        'sum_count_ig'        : torch.tensor(sum_count_ig,	requires_grad=False),
        'sum_count_i'         : torch.tensor(sum_count_i,	requires_grad=False),
        'sum_count_g'         : torch.tensor(sum_count_g,	requires_grad=False),
        'obs_all'             : torch.tensor(obs_all,           requires_grad=False),
        'obs_employ'          : torch.tensor(obs_employ,	requires_grad=False),
        'sum_omega_ig'        : torch.tensor(sum_omega_ig,	requires_grad=False),
        'sum_omega'           : torch.tensor(sum_omega,         requires_grad=False),
        'sum_omega_sq_ig'     : torch.tensor(sum_omega_sq_ig,	requires_grad=False),
        'sum_omega_sq'        : torch.tensor(sum_omega_sq,	requires_grad=False),
        'sum_logomega_ig'     : torch.tensor(sum_logomega_ig,	requires_grad=False),
        'sum_logomega'        : torch.tensor(sum_logomega,	requires_grad=False),
        'sum_logomega_sq_ig'  : torch.tensor(sum_logomega_sq_ig,requires_grad=False),
        'sum_logomega_sq'     : torch.tensor(sum_logomega_sq,	requires_grad=False),
        'I'                   : torch.tensor(I,                 requires_grad=False),
        'G'                   : torch.tensor(G,                 requires_grad=False),
        'm_i'                 : torch.reshape(torch.tensor(m_i, requires_grad=False), (I, 1)),
        'mean_wage_g'         : torch.tensor(mean_wage_g,	requires_grad=False),
        'mean_wage_i'         : torch.tensor(mean_wage_i,	requires_grad=False)
        }
    
    p_ig_actual = mle_data_sums['sum_count_ig'] / torch.reshape(torch.sum(mle_data_sums['sum_count_ig'], dim=1), (I,1))
    
    mle_data_sums['p_ig_actual'] = p_ig_actual
    

    pickle.dump(mle_data_sums, open(mle_data_sums_filename, "wb"))

    # Delete objects to preserve memory and prevent confusion
    del data_full, data_full_levels, data_full_logs, data_levels, data_logs, mle_data_sums, sum_c_ig, sum_c_i, sum_c_g, sum_count_ig, sum_count_i, sum_count_g, obs_all, obs_employ, sum_omega_ig, sum_omega, sum_omega_sq_ig, sum_omega_sq, sum_logomega_ig, sum_logomega, sum_logomega_sq_ig, sum_logomega_sq, I, G

    

 
   
    
