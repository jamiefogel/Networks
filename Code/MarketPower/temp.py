# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 18:28:33 2025

@author: p13861161
"""

import pandas as pd
import numpy as np
from config import root, rais
import time
import psutil
import graph_tool.all as gt
import gc
import matplotlib.pyplot as plt
import pickle
import os
import multiprocessing
import threading

def build_bipartite_graph(df, 
                          worker_col="fakeid_worker", 
                          job_col="jid",
                          drop_multi_edges=True,
                          min_job_degree=1):
    """
    Build a bipartite graph-tool.Graph from a pandas DataFrame containing
    [worker_col, job_col] edges.
    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns [worker_col, job_col].
    worker_col : str
        Name of the column containing worker IDs.
    job_col : str
        Name of the column containing job IDs.
    drop_multi_edges : bool
        If True, drop duplicate (worker, job) pairs.
    min_job_degree : int
        If > 1, filter out job nodes with fewer than this number of distinct workers.
    Returns
    -------
    g : graph_tool.Graph
        Bipartite graph-tool graph.
    worker_map : dict
        Maps worker ID -> graph-tool vertex index.
    job_map : dict
        Maps job ID -> graph-tool vertex index.
    """
    # 1) Clean the DataFrame
    df = df[[worker_col, job_col]].dropna()
    # 2) Optionally remove job nodes with fewer than min_job_degree edges
    if min_job_degree > 1:
        job_counts = df.groupby(job_col)[worker_col].nunique()
        valid_jobs = job_counts[job_counts >= min_job_degree].index
        df = df[df[job_col].isin(valid_jobs)]
    # 3) Optionally drop multi-edges
    if drop_multi_edges:
        df = df.drop_duplicates([worker_col, job_col])
    # 4) Create the bipartite graph
    g = gt.Graph(directed=False)
    worker_map = {}
    job_map = {}
    v_type = g.new_vertex_property("int")
    g.vertex_properties["bipartite_type"] = v_type
    # Helpers to add worker/job vertices
    def get_worker_vertex(wid):
        if wid in worker_map:
            return worker_map[wid]
        else:
            v = g.add_vertex()
            worker_map[wid] = v
            v_type[v] = 0   # worker type
            return v
    def get_job_vertex(jid):
        if jid in job_map:
            return job_map[jid]
        else:
            v = g.add_vertex()
            job_map[jid] = v
            v_type[v] = 1   # job type
            return v
    # 5) Add edges
    for w, j in zip(df[worker_col], df[job_col]):
        v_w = get_worker_vertex(w)
        v_j = get_job_vertex(j)
        g.add_edge(v_w, v_j)
    return g, worker_map, job_map


def get_giant_component(g):
    """
    Restrict the graph to its giant component (largest connected component).
    Parameters
    ----------
    g : graph_tool.Graph
        The input graph.
    
    Returns
    -------
    g_gc : graph_tool.Graph
        The subgraph containing only the largest connected component.
    """
    # Get the component labels
    comp_labels = gt.label_largest_component(g, directed=False)
    # Filter the graph based on the component labels
    g_gc = gt.GraphView(g, vfilt=comp_labels)
    # Make a copy of the subgraph (to remove references to the original)
    g_gc = gt.Graph(g_gc, prune=True)
    return g_gc


# ------------------------------------------------------------------------------
# 1) LOADING DATA (original function, unchanged)
# ------------------------------------------------------------------------------
dfs = []
# Should this start with 86?
for year in range(1987, 1991):
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

edgelist = stacked.loc[stacked.jid.notna(),['fakeid_worker','jid']]
edgelist_3states = stacked.loc[(stacked.jid.notna())& (stacked.state.isin([31,33,35])),['fakeid_worker','jid']]
edgelist.to_pickle(root + '/Data/derived/mayara_edgelist_1987_1990.p')




# Build the bipartite graph
g_3states, worker_map_3states, job_map_3states = build_bipartite_graph(edgelist_3states)
g_gc_3states = get_giant_component(g_3states)
print(g_3states)
print(g_gc_3states)

# Build the bipartite graph
g, worker_map, job_map = build_bipartite_graph(edgelist)
g_gc = get_giant_component(g)
print(g)
print(g_gc)

