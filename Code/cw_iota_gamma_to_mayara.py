# -*- coding: utf-8 -*-
"""
Created on Wed Jan 29 19:02:46 2025

@author: p13861161
"""

import pandas as pd
import numpy as np
from config import root, rais


bipartite_edgelist = pd.read_pickle(root + '/Data/derived/bipartite_edgelist_trade_shock.p')
wid_list = bipartite_edgelist[['wid']].drop_duplicates()
wid_list['wid'] = wid_list['wid'].astype('int64')

wblocks = pd.read_csv(root + 'Data/derived/sbm_output/model_trade_shock_mcmc_wblocks.csv', usecols=['wid', 'worker_blocks_level_0', 'worker_blocks_level_1'])

wid_list_merged = wid_list.merge(wblocks, on='wid', how='outer', validate='1:1', indicator=True)


rais_workerid_fakeid_link = pd.read_stata(root + "Code/replicate_mayara/output/dta/IDlinksrais/rais_workerid_fakeid_link_20191213.dta")

wid_list_merged2 = wid_list_merged.merge(rais_workerid_fakeid_link, left_on='wid', right_on='workerid_pis', how='outer', validate='1:1', indicator='_merge2')
print(wid_list_merged2._merge2.value_counts())
'''
_merge2
right_only    94906762
both          23685307
left_only        30535
Name: count, dtype: int64
'''

cw_wid__fakeid_worker__iota = wid_list_merged2.loc[wid_list_merged2._merge2=='both', ['wid', 'workerid_pis', 'worker_blocks_level_0', 'worker_blocks_level_1', 'fakeid_worker']]
# A very small number of wids map to the same fakeid_worker so I'm just breaking ties arbitrarily
cw_wid__fakeid_worker__iota = cw_wid__fakeid_worker__iota.drop_duplicates(subset=['fakeid_worker'], keep='first')


# Check for uniqueness
if cw_wid__fakeid_worker__iota.duplicated(subset=['fakeid_worker']).any():
    raise ValueError("Error: 'fakeid_worker' does not uniquely identify the dataset.")
    
cw_wid__fakeid_worker__iota.to_pickle(root + '/Data/derived/cw_wid__fakeid_worker__iota.p')


# An issue I had to deal with below is that there are some jids in the csv that do not have leading zeroes, creating duplicates when I pad leading zeros. This would be innocuous except that in some cases (need to figure out how many) they map to different gammas, so I'll have to arbitrarily choose a gamma in these cases.

jid_list = bipartite_edgelist[['jid']].drop_duplicates()
jid_list[['estab_id', 'occ4']] = jid_list['jid'].str.split('_', expand=True)
jid_list['estab_id'] = jid_list['estab_id'].astype('int64')
jid_list['occ4'] = jid_list['occ4'].astype('int64')
print(jid_list.shape)
jid_list = jid_list.sort_values(by=['estab_id', 'occ4'])
jid_list = jid_list.drop_duplicates(subset=['estab_id', 'occ4'], keep="first").copy()
print(jid_list.shape)


jblocks = pd.read_csv(root + 'Data/derived/sbm_output/model_trade_shock_mcmc_jblocks.csv', usecols=['jid','jid_py', 'job_blocks_level_0', 'job_blocks_level_1'])
jblocks = jblocks.drop_duplicates()
jblocks[['estab_id', 'occ4']] = jblocks['jid'].str.split('_', expand=True)
jblocks['estab_id'] = jblocks['estab_id'].astype('int64')
jblocks['occ4'] = jblocks['occ4'].astype('int64')
jblocks = jblocks.sort_values(by=['estab_id', 'occ4'])
jblocks = jblocks.drop_duplicates(subset=['estab_id', 'occ4'], keep="first").copy()
jblocks.drop(columns='jid',inplace=True)

jid_list_merged = jid_list.merge(jblocks, on=['estab_id','occ4'], how='outer', validate='m:1', indicator=True)
print(jid_list_merged._merge.value_counts())
# About 10% of jids assigned gammas
jid_list_merged = jid_list_merged.loc[jid_list_merged._merge=='both']

rais_estabid_fakeid_link = pd.read_stata(root + "Code/replicate_mayara/output/dta/IDlinksrais/rais_estabid_fakeid_link_20191213.dta")

jid_list_merged2 = jid_list_merged.merge(rais_estabid_fakeid_link, left_on='estab_id', right_on='estabid_cnpjcei', how='outer', validate='m:1', indicator='_merge2')

print(jid_list_merged2._merge2.value_counts())

cw_jid__fakeid_estab__gamma = jid_list_merged2.loc[jid_list_merged2._merge2=='both',['jid', 'estab_id', 'occ4','job_blocks_level_0', 'job_blocks_level_1', 'fakeid_firm', 'fakeid_estab']]

# A very small number of jids map to the same fakeid_estab so I'm just breaking ties arbitrarily
cw_jid__fakeid_estab__gamma = cw_jid__fakeid_estab__gamma.drop_duplicates(subset=['fakeid_estab','occ4'], keep='first')

# Check for uniqueness
if cw_jid__fakeid_estab__gamma.duplicated(subset=['fakeid_estab', 'occ4']).any():
    raise ValueError("Error: 'fakeid_estab' and 'occ4' do not uniquely identify the dataset.")
    
cw_jid__fakeid_estab__gamma.to_pickle(root + '/Data/derived/cw_jid__fakeid_estab__gamma.p')

