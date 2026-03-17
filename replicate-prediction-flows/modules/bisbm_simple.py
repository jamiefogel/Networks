"""
Simplified bipartite stochastic block model implementation for Section 5.3.

This module contains only the essential functionality needed to identify
network-based labor markets (gamma) for the job transition prediction analysis.
"""

import os
import graph_tool.all as gt
import numpy as np
import pandas as pd
import pickle
import datetime


class bisbm_simple():
    """Simplified bipartite SBM class focused on market identification."""

    def __init__(self):
        self.g = None
        self.edgelist = pd.DataFrame()
        self.state = None
        self.num_jobs = np.nan
        self.num_workers = np.nan
        self.num_edges = np.nan
        self.num_job_blocks = []
        self.num_worker_blocks = []

    def create_graph(self, filename, min_workers_per_job=None):
        """
        Create bipartite graph from worker-job edgelist.

        Parameters:
        - filename: Path to pickled edgelist DataFrame
        - min_workers_per_job: Minimum workers per job filter
        """
        print("Loading edgelist...")
        self.edgelist = pd.read_pickle(filename)

        # Initial counts
        nj = self.edgelist['jid'].nunique()
        nw = self.edgelist['wid'].nunique()
        edges = self.edgelist.shape[0]
        print(f'Raw data: {nw} workers, {nj} jobs, {edges} edges')

        # Count workers per job
        self.edgelist['workers_per_job'] = self.edgelist.groupby('jid')['jid'].transform('count')

        # Apply minimum workers per job filter
        if min_workers_per_job is not None:
            before = len(self.edgelist)
            self.edgelist = self.edgelist[self.edgelist['workers_per_job'] >= min_workers_per_job]
            after = len(self.edgelist)
            print(f'Applied min_workers_per_job={min_workers_per_job}: {before} -> {after} edges')

        # Final counts
        self.num_jobs = self.edgelist['jid'].nunique()
        self.num_workers = self.edgelist['wid'].nunique()
        self.num_edges = self.edgelist.shape[0]

        # Create sequential IDs for graph-tool
        # Jobs: 0 to num_jobs-1
        # Workers: num_jobs to (num_jobs + num_workers - 1)
        self.edgelist['jid_py'] = self.edgelist.groupby(['jid']).ngroup()
        self.edgelist['wid_py'] = self.edgelist.groupby(['wid']).ngroup() + self.num_jobs

        print(f'Final graph: {self.num_workers} workers, {self.num_jobs} jobs, {self.num_edges} edges')

        # Create graph-tool bipartite graph
        g = gt.Graph(directed=False)
        g.add_edge_list(self.edgelist[['jid_py', 'wid_py']].to_numpy().astype('float64'))

        # Create vertex kind property (0=job, 1=worker)
        kind = g.vp["kind"] = g.new_vp("int")
        kind.a[:self.num_jobs] = 0    # Jobs
        kind.a[self.num_jobs:] = 1    # Workers

        # Store original IDs
        joblist = self.edgelist[['jid', 'jid_py']].drop_duplicates().sort_values('jid_py')
        workerlist = self.edgelist[['wid', 'wid_py']].drop_duplicates().sort_values('wid_py')

        # Create vertex property with original IDs
        total_vertices = self.num_jobs + self.num_workers
        all_ids = np.empty(total_vertices, dtype=object)
        all_ids[:self.num_jobs] = joblist['jid'].values.astype(str)
        all_ids[self.num_jobs:] = workerlist['wid'].values.astype(str)

        ids = g.new_vertex_property("string", vals=all_ids)
        g.vp['ids'] = ids

        self.g = g
        print("Graph created successfully")

    def fit(self, n_init=1):
        """
        Fit bipartite stochastic block model.

        Parameters:
        - n_init: Number of random initializations to try
        """
        if self.g is None:
            raise ValueError("No graph available. Run create_graph() first.")

        print(f"Starting SBM estimation at {datetime.datetime.now()}")

        clabel = self.g.vp['kind']  # Bipartite constraint
        state_args = {'clabel': clabel, 'pclabel': clabel, 'deg_corr': True}

        # Try multiple initializations to avoid local minima
        mdl = np.inf
        best_state = None

        for i in range(n_init):
            if n_init > 1:
                print(f"  Initialization {i+1}/{n_init}")

            state_tmp = gt.minimize_nested_blockmodel_dl(self.g, state_args=state_args)
            mdl_tmp = state_tmp.entropy()

            if mdl_tmp < mdl:
                mdl = mdl_tmp
                best_state = state_tmp.copy(sampling=True)

        self.state = best_state
        self.mdl = mdl

        # Extract number of blocks at each hierarchy level
        L = len(self.state.levels)
        if L == 2:
            self.L = 1
            print("Found only trivial bipartite structure")
        else:
            self.L = L - 2  # Omit trivial levels

        # Extract block counts
        if self.L >= 1:
            # Level 0 blocks (finest partition)
            level_0_state = self.state.get_levels()[0]
            blocks_level_0 = level_0_state.get_blocks()

            # Count job and worker blocks separately
            job_blocks = blocks_level_0.a[:self.num_jobs]
            worker_blocks = blocks_level_0.a[self.num_jobs:]

            self.num_job_blocks = [len(np.unique(job_blocks))]
            self.num_worker_blocks = [len(np.unique(worker_blocks))]

            print(f"SBM estimation complete:")
            print(f"  Job blocks (gamma markets): {self.num_job_blocks[0]}")
            print(f"  Worker blocks (iota types): {self.num_worker_blocks[0]}")
            print(f"  MDL: {self.mdl:.2f}")

    def export_blocks(self, joutput=None, woutput=None):
        """
        Export block assignments to CSV files.

        Parameters:
        - joutput: Filename for job block assignments
        - woutput: Filename for worker block assignments
        """
        if self.state is None:
            raise ValueError("No SBM state available. Run fit() first.")

        # Get job and worker lists with original IDs
        joblist = self.edgelist[['jid', 'jid_py']].drop_duplicates().sort_values('jid_py')
        workerlist = self.edgelist[['wid', 'wid_py']].drop_duplicates().sort_values('wid_py')

        # Extract blocks from finest level (level 0)
        level_0_state = self.state.get_levels()[0]
        blocks_level_0 = level_0_state.get_blocks()

        job_blocks = blocks_level_0.a[:self.num_jobs]
        worker_blocks = blocks_level_0.a[self.num_jobs:]

        # Create job blocks DataFrame
        if joutput is not None:
            job_df = joblist[['jid']].copy()
            job_df['job_blocks_level_0'] = job_blocks
            job_df.to_csv(joutput, index=False)
            print(f"Job blocks exported to {joutput}")

        # Create worker blocks DataFrame
        if woutput is not None:
            worker_df = workerlist[['wid']].copy()
            worker_df['worker_blocks_level_0'] = worker_blocks
            worker_df.to_csv(woutput, index=False)
            print(f"Worker blocks exported to {woutput}")

        print("Block export complete")