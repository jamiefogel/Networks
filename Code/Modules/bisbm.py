################################################################################################################
# This builds off of hSBM_w_sbmtm_11142019.py but is an attempt to rewrite that code as a bunch of functions
# and generally make it more flexible and robust

# This might be useful: https://www.mail-archive.com/graph-tool@skewed.de/msg03397.html. Discusses memory requirements in hierarchical SBM
################################################################################################################

# Load packages
import os
import graph_tool.all as gt
import numpy
import matplotlib
import csv
import datetime
import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pickle
import re
import uuid

class bisbm():
# Need to indent everything below this
    def __init__(self):
        self.g = None ## network
        self.edgelist = pd.DataFrame()
        self.edgelist_w_blocks = pd.DataFrame()
        self.state = None
        self.state_mcmc = None
        self.num_jobs = np.nan
        self.num_workers = np.nan
        self.num_edges = np.nan
        self.P = np.matrix([])
        self.num_job_blocks = []
        self.num_worker_blocks = []


    #################################################################################
    # Simply load an edgelist and possibly allow for restrictions on workers per job
    #################################################################################
    def create_graph(self, filename=None, min_workers_per_job=None, drop_giant=False): #, min_jobs_per_workers=None
        if filename is None:
            raise ValueError("You must specify a filename containing an edge list.")
        else:
            # Load the edgelist from a csv. I should add a check to make sure it contains worker and job ID variables
            self.edgelist = pd.read_pickle(filename)


            # Count total number of workers and jobs before restricting based on workers per job
            nj = self.edgelist['jid'].nunique()
            nw = self.edgelist['wid'].nunique()
            edges = self.edgelist.shape[0]
            print(nw, 'unique workers,',nj, 'unique jobs, and', edges, 'edges in raw data' )

            # Count the number of workers per job and jobs per worker
            self.edgelist['workers_per_job'] = self.edgelist.groupby('jid')['jid'].transform('count')
            self.edgelist['jobs_per_worker'] = self.edgelist.groupby('wid')['wid'].transform('count')

            # Optionally, drop jobs with few workers
            if min_workers_per_job is not None:
                  indexNames = self.edgelist[ (self.edgelist['workers_per_job'] < min_workers_per_job ) ].index
                  self.edgelist.drop(indexNames , inplace=True)

            # Create new worker and job ID variables with jobs running from 0 to num_jobs-1 and workers running from num_jobs to (num_jobs+num_workers-1). This allows me to specify the bipartite structure more easily
            self.num_jobs    = self.edgelist['jid'].nunique()
            self.num_workers = self.edgelist['wid'].nunique()
            self.num_edges   = self.edgelist.shape[0]
            self.edgelist['jid_py']=self.edgelist.groupby(['jid']).ngroup()
            self.edgelist['wid_py']=self.edgelist.groupby(['wid']).ngroup() + self.num_jobs
            print(self.num_workers, 'unique workers,',self.num_jobs, 'unique jobs, and', self.num_edges, 'edges in graph after restricting to at least', min_workers_per_job, 'workers per job' )
            
            g = gt.Graph(directed=False)
            g.add_edge_list(self.edgelist[['jid_py','wid_py']].to_numpy().astype('float64')  )
            print("Graph created")
            
            # Compute connected components
            comp, hist = gt.label_components(g)
            giant_label = np.argmax(hist)
            is_giant = (comp.a == giant_label)  # Boolean array for vertices in giant component
            # Flag giant component in edgelist
            jid_indices = self.edgelist['jid_py'].to_numpy().astype(int)
            self.edgelist['giant'] = is_giant[jid_indices]
            
            if drop_giant==True:
                g = gt.extract_largest_component(g, prune=True)
               
                print("Graph restricted to giant component")
                            
                # Update counts using only the edges that belong to the giant component
                giant_edges = self.edgelist[self.edgelist['giant']]
                self.num_edges   = giant_edges.shape[0]
                self.num_jobs    = giant_edges['jid_py'].nunique()
                self.num_workers = giant_edges['wid_py'].nunique()
    
                # Update self.edgelist to contain only giant component edges
                self.edgelist = giant_edges.copy()
                print("Updated counts after restricting to giant component:",
                      self.num_workers, "unique workers,", self.num_jobs, "unique jobs, and",
                      self.num_edges, "edges.")

            # Create a vertex property indicating whether a vertex is a job (kind=0) or worker (kind=1)
            self.g = g
            # Vectorized assignment for vertex properties:
            kind = g.vp["kind"] = g.new_vp("int")
            kind.a[:self.num_jobs] = 0
            kind.a[self.num_jobs:] = 1
    
            # Get sorted job and worker lists based on the new numeric IDs
            joblist = self.edgelist[['jid', 'jid_py']].drop_duplicates().sort_values(by=['jid_py'])
            workerlist = self.edgelist[['wid', 'wid_py']].drop_duplicates().sort_values(by=['wid_py'])
    
            ids_py = g.vp['ids_py'] = g.new_vp('int')
            ids_py.a[:self.num_jobs] = joblist['jid_py'].values
            ids_py.a[self.num_jobs:] = workerlist['wid_py'].values
    
            # Assuming joblist and workerlist are already computed correctly
            job_ids = joblist['jid'].values.astype(str)
            worker_ids = workerlist['wid'].values.astype(str)
            # Create an array of the correct length
            total_vertices = self.num_jobs + self.num_workers  # should match g.num_vertices()
            all_ids = np.empty(total_vertices, dtype=object)
            all_ids[:self.num_jobs] = job_ids
            all_ids[self.num_jobs:] = worker_ids
            # Now, create the string vertex property in one step using these values.
            ids = g.new_vertex_property("string", vals=all_ids)
            g.vp['ids'] = ids



    #################################################################################
    # Fit the model
    #################################################################################
    def fit(self, overlap = False, hierarchical = True, n_init = 1, B_min=1, B_max=np.iinfo(np.uint64).max):
        '''
        Fit the bisbm
        - overlap, bool (default: False). Overlapping or Non-overlapping groups.
            Overlapping not implemented yet
        - hierarchical, bool (default: True). Hierarchical SBM or Flat SBM.
            Flat SBM not implemented yet.
        - Bmin, int (default:None): pass an option to the graph-tool inference specifying the minimum number of blocks.
        - n_init, int (default:1): number of different initial conditions to run in order to avoid local minimum of MDL.
        '''
        g = self.g
        if g is None:
            print('No data to fit the SBM. Load some data first (make_graph)')
        else:
            if overlap and "count" in g.ep:
                raise ValueError("When using overlapping SBMs, the graph must be constructed with 'counts=False'")
            clabel = g.vp['kind']

            state_args = {'clabel': clabel, 'pclabel': clabel, 'deg_corr':True}
            if "count" in g.ep:
                state_args["eweight"] = g.ep.count
                print("I'm not sure I have implemented edge weights properly yet")

            ## the inference
            ## JSF: I think this loops over a bunch of initial conditions in order to pick the one with the best (smallest) minimum description length. This is to avoid finding local minima
            starttime = datetime.datetime.now()
            print("Starting BiSBM estimation block at ", datetime.datetime.now())
            mdl = np.inf ##
            for i_n_init in range(n_init):
                state_tmp = gt.minimize_nested_blockmodel_dl(g, state_args=state_args,multilevel_mcmc_args={'B_min': B_min, 'B_max': B_max})
                mdl_tmp = state_tmp.entropy()
                if mdl_tmp < mdl:
                    mdl = 1.0*mdl_tmp
                    state = state_tmp.copy(sampling=True)



            print("BiSBM estimation completed at ", datetime.datetime.now())
            print("Estimation time: ", datetime.datetime.now()-starttime)

            # In a previous version I printed the number of worker and job blocks. This relied on code from sbmtm.get_groups() that I could add back in

            self.state = state
            ## minimum description length
            self.mdl = state.entropy()
            ## collect group membership for each level in the hierarchy
            L = len(state.levels)

            ## only trivial bipartite structure
            if L == 2:
                self.L = 1
                print("Found only trivial bipartite structure")

            ## omit trivial levels: l=L-1 (single group), l=L-2 (bipartite)
            else:
                self.L = L-2





    #################################################################################
    #
    #################################################################################
    '''
    def export_blocks(self, output=None, joutput=None, woutput=None, max_level=None, export_mcmc=False):

        df = pd.DataFrame({
            'worker_node': self.g.vp.kind.a.astype(np.int8),  # 0/1 flag
            'id_py': self.g.vp.ids_py.a,
            'id': self.g.vp.ids.get_2d_array([0]).flatten()
        })
        num_job_blocks = []
        num_worker_blocks = []
        mlevel = self.L if max_level is None else max_level + 1

      

        # Loop over levels and compute new block IDs using efficient numpy operations.
        for l in range(mlevel):
            # Select the state to use
            if not export_mcmc:
                blocks = self.state.project_level(l).get_blocks().a
            else:
                blocks = self.state_mcmc.project_level(l).get_blocks().a
            blocks = np.asarray(blocks, dtype=np.int32)
            
            # Use numpy arrays to avoid grouping the entire DataFrame:
            worker_arr = df['worker_node'].to_numpy(dtype=np.int8)
            new_ids = np.empty_like(blocks, dtype=np.int32)
    
            # For job nodes (worker_node==0): factorize and assign new IDs.
            job_mask = worker_arr == 0
            job_ids, job_uniques = pd.factorize(blocks[job_mask])
            new_ids[job_mask] = job_ids.astype(np.int32)
            
            # For worker nodes (worker_node==1): factorize and offset by number of unique job groups.
            worker_mask = ~job_mask
            worker_ids, worker_uniques = pd.factorize(blocks[worker_mask])
            new_ids[worker_mask] = worker_ids.astype(np.int32) + len(job_uniques)
    
            # Record block counts (the new numbering ensures job groups come first)
            njb = len(job_uniques)
            nwb = len(worker_uniques)
            num_job_blocks.append(njb)
            num_worker_blocks.append(nwb)
            print(f'Level {l}: Num job blocks {njb}; Num worker blocks {nwb}')
    
            # Instead of concatenating, simply add a new column for this level
            df[f'blocks_level_{l}'] = new_ids
    
            # Free temporary arrays
            del blocks, new_ids
            
        # Separate vertex info into job and worker blocks
        job_blocks = df.loc[df['worker_node'] == 0].copy().drop(columns=['worker_node'])
        job_blocks.rename(columns={"id": "jid", "id_py": "jid_py"}, inplace=True)
        job_blocks = job_blocks.rename(columns=lambda cname: "job_" + cname if cname not in ["jid", "jid_py"] else cname)
    
        worker_blocks = df.loc[df['worker_node'] == 1].copy().drop(columns=['worker_node'])
        worker_blocks.rename(columns={"id": "wid", "id_py": "wid_py"}, inplace=True)
        worker_blocks = worker_blocks.rename(columns=lambda cname: "worker_" + cname if cname not in ["wid", "wid_py"] else cname)
    
        # Merge job and worker block assignments into the original edgelist.
        # Using copy=False when possible to reduce memory overhead.
        edgelist_w_blocks = self.edgelist.merge(job_blocks, on='jid_py', validate='m:1', copy=False)
        edgelist_w_blocks = edgelist_w_blocks.merge(worker_blocks, on='wid_py', validate='m:1', copy=False)
        edgelist_w_blocks.drop(columns=['wid_y', 'jid_y'], inplace=True)
        edgelist_w_blocks.rename(columns={'wid_x': 'wid', 'jid_x': 'jid'}, inplace=True)
        self.edgelist_w_blocks = edgelist_w_blocks
    
        self.num_job_blocks = num_job_blocks
        self.num_worker_blocks = num_worker_blocks

        if output is not None:
            if output.endswith(".csv"):
                edgelist_w_blocks.to_csv(output, index=False)
            elif output.endswith(".parquet"):
                edgelist_w_blocks.to_parquet(output)
            elif output.endswith(".p"):
                pickle.dump( edgelist_w_blocks, open(output, "wb" ) )
                #edgelist_w_blocks.to_pickle(output)

        if joutput is not None:
            if joutput.endswith(".csv"):
                job_blocks.to_csv(joutput, index=False)
            elif joutput.endswith(".parquet"):
                job_blocks.to_parquet(joutput)
            elif joutput.endswith(".p"):
                job_blocks.to_pickle(joutput)

        if woutput is not None:
            if woutput.endswith(".csv"):
                worker_blocks.to_csv(woutput, index=False)
            elif woutput.endswith(".parquet"):
                worker_blocks.to_parquet(woutput)
            if woutput.endswith(".p"):
                worker_blocks.to_pickle(woutput)
    '''
    def export_blocks(self, output=None, joutput=None, woutput=None, max_level=None, export_mcmc=False):
        """
        Create two DataFrames (job_blocks, worker_blocks) for all levels,
        then merge them onto self.edgelist. This is more memory-efficient
        than building a single big df with all vertices and levels.
        """
        # Determine how many hierarchy levels to export
        mlevel = self.L if max_level is None else max_level + 1
    
        # Prepare arrays for all vertices
        worker_node_arr = self.g.vp.kind.a.astype(np.int8)      # 0 for job, 1 for worker
        id_py_arr       = self.g.vp.ids_py.a                    # numeric ID
        id_arr          = self.g.vp.ids.get_2d_array([0]).flatten()  # string ID (jid or wid)
    
        # Separate job vs. worker vertices
        job_mask    = (worker_node_arr == 0)
        worker_mask = (worker_node_arr == 1)
    
        # We'll store factorized block IDs for each level in dictionaries
        job_block_assignments    = {}
        worker_block_assignments = {}
    
        self.num_job_blocks    = []
        self.num_worker_blocks = []
    
        # For each level, factorize blocks for jobs and for workers separately
        for l in range(mlevel):
            # Get the block assignments for this level
            if not export_mcmc:
                blocks = self.state.project_level(l).get_blocks().a
            else:
                blocks = self.state_mcmc.project_level(l).get_blocks().a
            blocks = blocks.astype(np.int32, copy=False)
    
            # Factorize the job-node blocks
            job_vals = blocks[job_mask]
            job_ids, job_uniques = pd.factorize(job_vals)  # job_ids is an array of "dense" IDs
            # Factorize the worker-node blocks
            worker_vals = blocks[worker_mask]
            worker_ids, worker_uniques = pd.factorize(worker_vals)
            # Offset worker block IDs so job blocks come first
            worker_ids_offset = worker_ids + len(job_uniques)
    
            # Store in dictionaries
            job_block_assignments[l]    = job_ids
            worker_block_assignments[l] = worker_ids_offset
    
            # Keep track of how many job/worker blocks at this level
            njb = len(job_uniques)
            nwb = len(worker_uniques)
            self.num_job_blocks.append(njb)
            self.num_worker_blocks.append(nwb)
            print(f'Level {l}: Num job blocks {njb}; Num worker blocks {nwb}')
    
        # Now build job_blocks DataFrame
        # --------------------------------
        job_cols = {
            "jid_py": id_py_arr[job_mask],
            "jid":    id_arr[job_mask]
        }
        # Add one column per level, e.g. blocks_level_0, blocks_level_1, ...
        for l in range(mlevel):
            job_cols[f"blocks_level_{l}"] = job_block_assignments[l]
    
        job_blocks = pd.DataFrame(job_cols)
        # Rename columns to match your original naming scheme
        # i.e. "job_blocks_level_0" instead of "blocks_level_0"
        job_blocks.rename(columns=lambda c:
                          "job_" + c if c not in ("jid", "jid_py") else c,
                          inplace=True)
    
        # Now build worker_blocks DataFrame
        # -----------------------------------
        worker_cols = {
            "wid_py": id_py_arr[worker_mask],
            "wid":    id_arr[worker_mask]
        }
        for l in range(mlevel):
            worker_cols[f"blocks_level_{l}"] = worker_block_assignments[l]
    
        worker_blocks = pd.DataFrame(worker_cols)
        worker_blocks.rename(columns=lambda c:
                             "worker_" + c if c not in ("wid", "wid_py") else c,
                             inplace=True)
    
        # Merge onto the edgelist
        # -------------------------
        # This is the same logic as your original code, but merges only once for jobs, once for workers.
        # 'm:1' validate means: many edges can map to one job/worker row.
        edgelist_w_blocks = self.edgelist.merge(job_blocks,
                                                left_on='jid_py', right_on='jid_py',
                                                validate='m:1', copy=False)
        edgelist_w_blocks = edgelist_w_blocks.merge(worker_blocks,
                                                    left_on='wid_py', right_on='wid_py',
                                                    validate='m:1', copy=False)
    
        # If you want to drop or rename columns to avoid collisions, do so here.
        # The refactor avoids "wid_x", "wid_y" collisions because we used distinct column names,
        # but if needed:
        edgelist_w_blocks.drop(columns=['wid_y', 'jid_y'], inplace=True)
        edgelist_w_blocks.rename(columns={'wid_x': 'wid', 'jid_x': 'jid'}, inplace=True)
    
        # Store final DataFrame
        self.edgelist_w_blocks = edgelist_w_blocks
    
        # Optionally save to disk
        if output is not None:
            if output.endswith(".csv"):
                edgelist_w_blocks.to_csv(output, index=False)
            elif output.endswith(".parquet"):
                edgelist_w_blocks.to_parquet(output)
            elif output.endswith(".p"):
                with open(output, "wb") as f:
                    pickle.dump(edgelist_w_blocks, f)
    
        if joutput is not None:
            if joutput.endswith(".csv"):
                job_blocks.to_csv(joutput, index=False)
            elif joutput.endswith(".parquet"):
                job_blocks.to_parquet(joutput)
            elif joutput.endswith(".p"):
                job_blocks.to_pickle(joutput)
    
        if woutput is not None:
            if woutput.endswith(".csv"):
                worker_blocks.to_csv(woutput, index=False)
            elif woutput.endswith(".parquet"):
                worker_blocks.to_parquet(woutput)
            elif woutput.endswith(".p"):
                worker_blocks.to_pickle(woutput)



    ###############################################################################
    # Compute mean earnings by worker type
    #   - We will use this as the basis for choosing the normalization phi_i0
    ###############################################################################

    def mean_earnings_by_iota(self, level, earnings_file, earnings_var, output=None):

        if self.edgelist_w_blocks.empty is True:
            print('Warning: export_blocks() must be run before mean_earnings_by_iota(). Running export_blocks() now.')
            self.export_blocks()

        if level >= self.L:
            print('Error: level must be less than ',self.L)
            return -1

        block_var = 'worker_blocks_level_' + str(level)

        worker_blocks = self.edgelist_w_blocks[['wid',block_var]].drop_duplicates().rename(columns={block_var:'iota'})

        # From SAS
        df = pd.read_csv(earnings_file, usecols=['wid',earnings_var])

        df2 = df.merge(worker_blocks, on='wid',validate='m:1', indicator='True')

        # df2 = df.merge(job_blocks, how='left', on='jid',validate='m:1', indicator='True')
        # df2['True'].value_counts()
        # 3.6 million matched, 1.9 million not matched for Rio. My guess is that this is because we dropped jids with fewer than 10 workers

        iota_mean_earnings = df2.groupby(['iota'])[earnings_var].mean().to_frame().reset_index().rename(columns={earnings_var:"mean_earnings"})

        if output is not None:
            iota_mean_earnings.to_csv(output, index=False)

        self.iota_mean_earnings=iota_mean_earnings


    def mcmc_sweeps(self, savefile, tempsavedir='./', numiter=1000, seed=734,
                    checkpoint_every=10, entropy_every=1):
        print('Starting MCMC sweeps at ', datetime.datetime.now())
        gt.seed_rng(seed)
        self.state_mcmc = self.state.copy()
        
        entropy = [self.state_mcmc.entropy()]
        t0 = datetime.datetime.now()
    
        for i in range(numiter):
            self.state_mcmc.multiflip_mcmc_sweep(beta=np.inf, niter=1)
            entropy.append(self.state_mcmc.entropy())
    
            if i % entropy_every == 0:
                # Entropy saving is lightweight
                with open(os.path.join(tempsavedir, 'entropy.p'), "wb") as ef:
                    pickle.dump(entropy, ef)
    
            if (i+1) % checkpoint_every == 0 or i == numiter-1:
                # Periodically save full state (heavy), ideally every 3-5 iterations
                temp_filename = os.path.join(tempsavedir, f'tmp_state_mcmc_{uuid.uuid4().hex}.p')
                pickle.dump([self.state_mcmc, i, entropy], open(temp_filename, "wb"))
                os.rename(temp_filename, savefile)
    
            if i % entropy_every == 0:
                print(f"Iter {i}: Entropy = {entropy[-1]:.2f}, Time elapsed: {datetime.datetime.now() - t0}")
            
            
            
    ###############################################################
    # Posterior vertex–block probabilities (“soft assignments”)
    # https://chatgpt.com/share/687d7877-0468-800d-a730-3de2bf5a0231
    ###############################################################
    def _save_checkpoint(self, path: str, sweep_no: int, entropy: float, pv):
        """Overwrite the single‑file checkpoint."""
        # Serialise pv via graph‑tool’s internal routine -> bytes object
        pv_bytes = gt.serialization.dumps_dict({"pv": pv})
        # Grab block labels (1‑D NumPy array) – enough to rebuild state
        labels = self.state_mcmc.get_blocks().a.copy()
        meta = dict(sweep_no=sweep_no,
                    entropy=entropy,
                    pv_bytes=pv_bytes,
                    labels=labels)
        with open(path, "wb") as fh:
            pickle.dump(meta, fh, protocol=pickle.HIGHEST_PROTOCOL)

    def _load_checkpoint(self, path: str):
        """Return (sweep_no, entropy, pv, rebuilt_state) from file."""
        with open(path, "rb") as fh:
            meta = pickle.load(fh)
        pv     = gt.serialization.loads_dict(meta["pv_bytes"])["pv"]
        labels = meta["labels"]
        # Rebuild a BlockState from stored labels (cheap)
        st = gt.BlockState(self.g, b=labels, deg_corr=True, clabel=self.g.vp.type)
        return meta["sweep_no"], meta["entropy"], pv, st

    # ──────────────────────────────────────────────────────────────
    def collect_soft_assignments(self, *,
                                 nsweeps: int = 1200,
                                 burnin: int = 300,
                                 thin: int = 10,
                                 beta: float = 1.0,
                                 checkpoint_path: str = "soft_assignment_ckpt.pkl",
                                 checkpoint_every: int = 5,
                                 resume: bool = False,
                                 verbose: bool = True):
        """
        Collect posterior vertex–block probabilities, saving a *label‑aligned,
        normalised* probability map every `checkpoint_every` recordings.
        """
    
        if self.state is None:
            raise RuntimeError("Run fit() first!")
    
        gt.seed_rng(42)
        state_mcmc = self.state.copy()
    
        # ── Burn‑in ─────────────────────────────────────────────────
        if verbose:
            print(f"[Burn‑in] {burnin} sweeps at β={beta}")
        gt.mcmc_equilibrate(state_mcmc,
                            wait=burnin,
                            mcmc_args=dict(niter=1, beta=beta))
    
        # ── Sampling setup ─────────────────────────────────────────
        bs = []                                          # collected partitions
        total_rec = nsweeps // thin                      # # recorded partitions
    
        if verbose:
            print(f"[Sampling] {nsweeps} sweeps, thin={thin}, "
                  f"checkpt every {checkpoint_every} recs, β={beta}")
    
        # ── Sampling loop ──────────────────────────────────────────
        for rec in range(1, total_rec + 1):
            gt.mcmc_equilibrate(state_mcmc,
                                force_niter=thin,
                                mcmc_args=dict(niter=1, beta=beta))
            bs.append(state_mcmc.b.a.copy())
    
            # periodic checkpoint with full alignment + normalisation
            if rec % checkpoint_every == 0 or rec == total_rec:
                pmode_ck = gt.PartitionModeState(bs, converge=True)
                pv_ck = pmode_ck.get_marginal(self.g)
    
                # per‑vertex normalisation
                probs_ck = pv_ck.copy("double")
                for v in self.g.vertices():
                    s = probs_ck[v].a.sum()
                    if s:
                        probs_ck[v].a /= s
    
                # lightweight save
                with open(checkpoint_path, "wb") as f:
                    pickle.dump(probs_ck, f)
    
                if verbose:
                    print(f"[Checkpoint] {rec}/{total_rec} recorded "
                          f"(saved normalised pv)")
    
        # ── Final result ───────────────────────────────────────────
        pmode = gt.PartitionModeState(bs, converge=True)
        pv = pmode.get_marginal(self.g)
    
        probs = pv.copy("double")
        for v in self.g.vertices():
            s = probs[v].a.sum()
            if s:
                probs[v].a /= s
    
        self.vertex_soft_probs = probs
    
        # overwrite checkpoint with final probabilities
        with open(checkpoint_path, "wb") as f:
            pickle.dump(probs, f)
        if verbose:
            print("[Done] soft assignments aligned, normalised and saved.")
    
        return probs

            
            
'''
#Code to plot entropy improvmenet from MCMC sweeps

import pandas as pd
import matplotlib.pyplot as plt

# Assuming entropy is your list of entropy values from each round
e = pd.DataFrame(entropy, columns=["Entropy"])

# Calculate cumulative improvement relative to the first round
e["Cumulative Improvement (%)"] = (e["Entropy"].iloc[0] - e["Entropy"]) / e["Entropy"].iloc[0] * 100

# Calculate marginal improvement relative to the previous round
e["Marginal Improvement (%)"] = (e["Entropy"].shift(1) - e["Entropy"]) / e["Entropy"].shift(1) * 100

# Optionally round for display
e["Cumulative Improvement (%)"] = e["Cumulative Improvement (%)"].round(4)
e["Marginal Improvement (%)"] = e["Marginal Improvement (%)"].round(4)

# Plot using two axes
fig, ax1 = plt.subplots(figsize=(10, 6))

# Plot cumulative improvement on primary y-axis
ax1.plot(e.index, e["Cumulative Improvement (%)"], color="blue", label="Cumulative Improvement")
ax1.set_xlabel("Training Round")
ax1.set_ylabel("Cumulative Improvement (%)", color="blue")
ax1.tick_params(axis="y", labelcolor="blue")

# Create a secondary y-axis for marginal improvement
ax2 = ax1.twinx()
ax2.plot(e.index, e["Marginal Improvement (%)"], color="red", marker="o", label="Marginal Improvement")
ax2.set_ylabel("Marginal Improvement (%)", color="red")
ax2.tick_params(axis="y", labelcolor="red")

# Title and layout
plt.title("Training Improvement per Round")
fig.tight_layout()
plt.show()

'''

            