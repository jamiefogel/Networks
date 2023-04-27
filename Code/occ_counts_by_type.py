#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  8 10:18:03 2021

@author: jfogel
"""


import pandas as pd

def occ_counts_by_type(data, level, w_output=None, j_output=None):
    #panel refers to the data set called 'balanced' in functions.py
    # Read in translated occupation codes
    cw = pd.read_csv('~/Networks/Data/translated_occ_codes/translated_occ_codes_english_only.csv')
    
    panel = pd.read_csv(data, usecols=['iota','gamma','cbo2002'])
    
    wblock_var = 'iota'
    jblock_var = 'gamma'
      
    # These are implicitly weigthed by the number of edges. So if a job has 100 edges, it will be counted 100 times. The total number of obs of the occupation var equals the total number of edges
    w_occs = panel[['cbo2002',wblock_var]].rename(columns={wblock_var:'iota'})
    j_occs = panel[['cbo2002',jblock_var]].rename(columns={jblock_var:'gamma'})	
    w_occs['cbo2002'] = pd.to_numeric(w_occs['cbo2002'])
    j_occs['cbo2002'] = pd.to_numeric(j_occs['cbo2002'])
    
    w_occs_m = w_occs.merge(cw, left_on='cbo2002', right_on='CBO2002', validate='m:1', indicator=True)
    j_occs_m = j_occs.merge(cw, left_on='cbo2002', right_on='CBO2002', validate='m:1', indicator=True)
    
    w_occs_by_i = w_occs_m.groupby(['iota', 'cbo2002','description']).size().reset_index().rename(columns={0:'counts'})
    w_occs_by_i['iota_count'] = w_occs_by_i.groupby(['iota'])['counts'].transform(sum)
    w_occs_by_i['share'] = w_occs_by_i['counts']/w_occs_by_i['iota_count']
    
    j_occs_by_g = j_occs_m.groupby(['gamma','cbo2002','description']).size().reset_index().rename(columns={0:'counts'})
    j_occs_by_g['gamma_count'] = j_occs_by_g.groupby(['gamma'])['counts'].transform(sum)
    j_occs_by_g['share'] = j_occs_by_g['counts']/j_occs_by_g['gamma_count']
    
    w_occs_by_i.sort_values(['iota' ,'counts'], ascending=[True,False], inplace=True)
    j_occs_by_g.sort_values(['gamma','counts'], ascending=[True,False], inplace=True)
    
    if w_output is not None:
        w_occs_by_i.to_csv(w_output)
        
    if j_output is not None:
        j_occs_by_g.to_csv(j_output)
        
    
    w_dict = {int(i): w_occs_by_i.loc[w_occs_by_i['iota']==i]  for i in w_occs_by_i.iota.unique()}
    j_dict = {int(g): j_occs_by_g.loc[j_occs_by_g['gamma']==g] for g in j_occs_by_g.gamma.unique()}

    return [w_dict, j_dict]

# Usage


[w_dict, j_dict] = occ_counts_by_type(mle_data_filename, level=0, w_output=homedir + '/Networks/Code/aug2021/dump/panel_rio_2009_2012_occ_counts_by_i_level_' + str(level) + '.csv', j_output=homedir + '/Networks/Code/aug2021/dump/panel_rio_2009_2012_occ_counts_by_g_level_' + str(level) + '.csv')
                   

pickle.dump((w_dict, j_dict), open(homedir + '/Networks/Code/aug2021/dump/occ_counts.p', 'wb'))
(w_dict, j_dict) = pickle.load(open(homedir + '/Networks/Code/aug2021/dump/occ_counts.p', "rb"), encoding='bytes')


