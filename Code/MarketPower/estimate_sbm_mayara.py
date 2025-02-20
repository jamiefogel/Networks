# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 18:28:33 2025

@author: p13861161
"""

import pandas as pd
import numpy as np
from config import root
import time
import psutil
import graph_tool.all as gt
import gc
import matplotlib.pyplot as plt
import pickle
import bisbm
from datetime import datetime


# ------------------------------------------------------------------------------
# 1) LOADING DATA (original function, unchanged)
# ------------------------------------------------------------------------------


dfs = []
# Should this start with 86?
for year in range(1986, 1991):
    print(year)
    df = pd.read_parquet(f'/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies/sas/rais{year}.parquet')
    df = df[['fakeid_worker', 'fakeid_firm', 'fakeid_estab', 'agegroup', 'cbo', 'municipality']]
    df['state'] =  pd.to_numeric(df['municipality'].astype(str).str[:2], errors='coerce').astype('Int64')
    # Create jid only for rows where both fakeid_estab and occ4 are not missing
    df['fakeid_estab'] = df['fakeid_estab'].astype('Int64')
    df.loc[df['fakeid_estab'].notna() & df['cbo'].notna(), 'jid'] = df['fakeid_estab'].astype(str) + '_' + df['cbo'].astype(str).str[0:4]
    df.loc[df['fakeid_estab'].isna() | df['cbo'].isna(), 'jid'] = pd.NA
    dfs.append(df)


stacked = pd.concat(dfs, ignore_index=True)
stacked.rename(columns={'fakeid_worker':'wid'}, inplace=True)
edgelist = stacked.loc[stacked.jid.notna(),['wid','jid']]
edgelist_3states = stacked.loc[(stacked.jid.notna())& (stacked.state.isin([31,33,35])),['wid','jid']]
edgelist.to_pickle(root + '/Data/derived/mayara_edgelist_1986_1990.p')
edgelist_3states.to_pickle(root + '/Data/derived/mayara_edgelist_1986_1990_3states.p')

del edgelist, edgelist_3states, stacked, dfs, df
gc.collect()


print('Starting SBM section at ', datetime.now())
modelname = 'sbm_mayara_1986_1990_3states'
model = bisbm.bisbm()                                                                       
model.create_graph(filename=root + '/Data/derived/mayara_edgelist_1986_1990_3states.p',min_workers_per_job=1, drop_giant=True)
model.fit(n_init=1)
# In theory it makes more sense to save these as pickles than as csvs but I keep getting an error loading the pickle and the csv works fine
model.export_blocks(output=root + 'Data/derived/sbm_output/model_'+modelname+'_blocks.csv', joutput=root + 'Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', woutput=root + 'Data/derived/sbm_output/model_'+modelname+'_wblocks.csv', max_level=2)
model.edgelist_w_blocks[['wid', 'jid', 'workers_per_job', 'jobs_per_worker', 'jid_py', 'wid_py', 'job_blocks_level_0', 'job_blocks_level_1', 'job_blocks_level_2', 'worker_blocks_level_0', 'worker_blocks_level_1', 'worker_blocks_level_2']].to_parquet(root + 'Data/derived/sbm_output/'+modelname+'_edgelist_w_blocks.p')
pickle.dump( model, open(root + 'Data/derived/sbm_output/model_'+modelname+'.p', "wb" ))
print('SBM section complete at ', datetime.now())





'''
Checking size of components

# Identify the connected components of the (undirected) graph
comp, hist = gt.label_components(model.g)
# Find which component is largest
largest_comp_idx = np.argmax(hist)

in_giant = (comp.a == largest_comp_idx)  # comp.a is the numpy array of indices

fraction_in_giant = in_giant.mean()
print("Overall fraction of vertices in giant component:", fraction_in_giant)
kind_array = model.g.vp["kind"].a  # 0 for jobs, 1 for workers
frac_jobs_in_giant   = in_giant[(kind_array == 0)].mean()
frac_workers_in_giant = in_giant[(kind_array == 1)].mean()

print("Fraction of job nodes in GC:", frac_jobs_in_giant)
print("Fraction of worker nodes in GC:", frac_workers_in_giant)


# "comp.a" is a numpy array with length == number_of_vertices in model.g
# job_blocks['jid_py'] indicates the "graph-tool vertex index" for each job
job_blocks = pd.read_csv(root + 'Data/derived/sbm_output/model_'+modelname+'_jblocks.csv')
worker_blocks = pd.read_csv(root + 'Data/derived/sbm_output/model_'+modelname+'_wblocks.csv')
component_index = comp.a  # same length as g.num_vertices()
job_blocks["component"] = job_blocks["jid_py"].apply(lambda x: component_index[x])
worker_blocks["component"] = worker_blocks["wid_py"].apply(lambda x: component_index[x])
# Boolean: is this node in the giant component?
job_blocks["in_giant"] = (job_blocks["component"] == largest_comp_idx)
worker_blocks["in_giant"] = (worker_blocks["component"] == largest_comp_idx)


jobs_per_block = (
    job_blocks
    .groupby("job_blocks_level_0")
    ["jid_py"]  # or "jid"
    .count()
    .rename("count_of_jobs")
    .reset_index()
)
workers_per_block = (
    worker_blocks
    .groupby("worker_blocks_level_0")
    ["wid_py"]
    .count()
    .rename("count_of_workers")
    .reset_index()
)

jobs_per_block_giant = (
    job_blocks
    .query("in_giant == True")
    .groupby("job_blocks_level_0")
    ["jid_py"]
    .count()
    .rename("count_of_jobs_gc")
    .reset_index()
)

jobs_per_block_not_giant = (
    job_blocks
    .query("in_giant == False")
    .groupby("job_blocks_level_0")
    ["jid_py"]
    .count()
    .rename("count_of_jobs_not_gc")
    .reset_index()
)
'''
