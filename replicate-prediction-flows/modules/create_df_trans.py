# -*- coding: utf-8 -*-
"""
Created on Thu Jul 20 17:33:15 2023

- Create a dataframe of job-to-job transitions and keep the associated wid, iota, and previous and current markets (gamma and occ2Xmeso)
- Previously this was in predicting_flows_data_pull

@author: p13861161

import create_df_trans 
reload(create_df_trans)
from create_df_trans import create_df_trans
df_trans_ins = create_df_trans(raw_dfs)
"""

import pandas as pd
import numpy as np
    
def create_df_trans(raw_dfs):  
    # In-sample
    df = pd.concat(raw_dfs, axis=0)
    df = df.sort_values(by=['wid','start_date'])
    df['jid_prev'] = df.groupby('wid')['jid'].shift(1)
    df['gamma_prev'] = df.groupby('wid')['gamma'].shift(1)
    df['occ2Xmeso_prev'] = df.groupby('wid')['occ2Xmeso'].shift(1)
        
    # Restrict to obs with non-missing current and [revious gammas, occ2Xmesos, and iotas.
    # XX should I actually be cutting on non-missing jid_prev? I think I should actually wait to do that until making the unipartite transition matrices below. For the bipartite there is no reason why we need to have observed a previous jid. 
    df_trans = df[      (df['iota'] != -1)                                              \
                      & (df['gamma'].notnull())     & (df['gamma_prev'].notnull())      \
                      & (df['gamma'] != -1)         & (df['gamma_prev'] != -1)          \
                      & (df['occ2Xmeso'].notnull()) & (df['occ2Xmeso_prev'].notnull())  \
                      & (df['jid'].notnull())       & (df['jid_prev'].notnull())        \
                      & (df['jid']!=df['jid_prev'])                                     \
                ][['jid','jid_prev','wid','iota','gamma','gamma_prev','occ2Xmeso','occ2Xmeso_prev']]
 
    return df_trans
