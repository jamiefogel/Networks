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

class bisbm():
# Need to indent everything below this    
    def __init__(self):
        self.g = None ## network
        self.edgelist = pd.DataFrame()
        self.edgelist_w_blocks = pd.DataFrame()
        self.state = None
        self.num_jobs = np.nan
        self.num_workers = np.nan
        self.num_edges = np.nan
        self.P = np.matrix([])
        self.num_job_blocks = []
        self.num_worker_blocks = []
        self.alphas = pd.DataFrame()


        

    #################################################################################
    # Simply load an edgelist and possibly allow for restrictions on workers per job
    #################################################################################
    def create_graph(self, filename=None, min_workers_per_job=None): #, min_jobs_per_workers=None
        if filename is None:
            print("You must specify a filename containing an edge list.")
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

            # Create a vertex property indicating whether a vertex is a job (kind=0) or worker (kind=1)
            kind = g.vp["kind"] = g.new_vp("int")
            ids = g.vp['ids'] = g.new_vp('string')
            ids_py = g.vp['ids_py'] = g.new_vp('int')

            joblist    = self.edgelist[['jid','jid_py']].drop_duplicates().sort_values(by=['jid_py'])
            workerlist = self.edgelist[['wid','wid_py']].drop_duplicates().sort_values(by=['wid_py'])

            worker_counter=0
            job_counter=0
            for v in g.vertices():
                if v < self.num_jobs:
                    #print(v,job_counter,jobstats['occ6'].iat[job_counter],jobstats['college'].iat[job_counter])
                    kind[v] = 0
                    ids[v]=joblist['jid'].iat[job_counter]
                    ids_py[v]=joblist['jid_py'].iat[job_counter]
                    job_counter = job_counter+1
                else:
                    kind[v] = 1
                    ids[v]=workerlist['wid'].iat[worker_counter]
                    ids_py[v]=workerlist['wid_py'].iat[worker_counter]
                    worker_counter = worker_counter+1

            self.g=g




    #################################################################################
    # Fit the model
    #################################################################################
    def fit(self,overlap = False, hierarchical = True, n_init = 1):
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
                state_tmp = gt.minimize_nested_blockmodel_dl(g, state_args=state_args)
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
    def export_blocks(self, output=None, joutput=None, woutput=None):

        df=pd.DataFrame()
        df['worker_node']=self.g.vp.kind.a
        df['id_py'] = self.g.vp.ids_py.a
        df['id'] = self.g.vp.ids.get_2d_array([0]).flatten() #String vertex properties have to be handled differently
        num_job_blocks = []
        num_worker_blocks = []

        for l in range(self.L):
            # I don't know why copy=True is necessary but without it some of the 0s get converted to very large numbers for some reason
            temp = pd.DataFrame(self.state.project_level(l).get_blocks().a, columns=['blocks_level_'], copy=True)
            # Store the number of worker and job blocks at each level
            jmax = temp[['blocks_level_']][df['worker_node']==0].max()[0]
            wmax = temp[['blocks_level_']][df['worker_node']==1].max()[0]
            njb = jmax+1
            nwb = wmax-jmax
            num_job_blocks.append(njb)
            num_worker_blocks.append(nwb)
            print('Level', l,': Num job blocks', njb, '; Num worker blocks', nwb )

            temp = temp.rename(columns=lambda cname: cname + str(l))
            df = pd.concat([df,temp], axis=1)

            del temp        
        

        job_blocks    = df[df['worker_node']==0].drop(columns=['worker_node']).rename(columns={"id":"jid","id_py":"jid_py"}).rename(columns=lambda cname: "job_"+cname if (cname!="jid_py" and cname!="jid") else cname )
        worker_blocks = df[df['worker_node']==1].drop(columns=['worker_node']).rename(columns={"id":"wid","id_py":"wid_py"}).rename(columns=lambda cname: "worker_"+cname if (cname!="wid_py" and cname!="wid") else cname )

        edgelist_w_blocks = self.edgelist.merge(job_blocks,on='jid_py',validate='m:1')
        edgelist_w_blocks = edgelist_w_blocks.merge(worker_blocks,on='wid_py',validate='m:1')
        edgelist_w_blocks = edgelist_w_blocks.drop(columns=['wid_y','jid_y']).rename(columns={'wid_x':'wid','jid_x':'jid'}) #The merge creates duplicate ID columns
        self.edgelist_w_blocks = edgelist_w_blocks

        self.num_job_blocks = num_job_blocks
        self.num_worker_blocks = num_worker_blocks
        
        if output is not None:
            if re.search('.csv', output) is not None:
                edgelist_w_blocks.to_csv(output, index=False)
            if re.search('.p', output) is not None:
                pickle.dump( edgelist_w_blocks, open(output, "wb" ) )
                #edgelist_w_blocks.to_pickle(output)

        if joutput is not None:
            if re.search('.csv', joutput) is not None:
                job_blocks.to_csv(joutput, index=False)
            if re.search('.p', joutput) is not None:
                job_blocks.to_pickle(joutput)

        if woutput is not None:
            if re.search('.csv', woutput) is not None:
                worker_blocks.to_csv(woutput, index=False)
            if re.search('.p', woutput) is not None:
                worker_blocks.to_pickle(woutput)




    #################################################################################
    # Calculate the share of sector s earnings for each gamma (we call these alpha_gs)
    #   - This imports a separate earnings data set because we kept only the most recent
    #      worker-job pair when compiling the edgelist
    #################################################################################
    def compute_alphas(self, level, earnings_file, earnings_var, output=None):
        
        if self.edgelist_w_blocks.empty is True:
            print('Warning: export_blocks() must be run before compute_alphas(). Running export_blocks() now.')
            self.export_blocks()

        if level >= self.L:
            print('Error: level must be less than ',self.L)
            return -1

        block_var = 'job_blocks_level_' + str(level)
        	
        job_blocks = self.edgelist_w_blocks[['jid',block_var]].drop_duplicates().rename(columns={block_var:'gamma'})
        
        # From SAS
        df = pd.read_csv(earnings_file, usecols=['jid','sector',earnings_var])
        
        df2 = df.merge(job_blocks, on='jid',validate='m:1', indicator='True')
        
        # df2 = df.merge(job_blocks, how='left', on='jid',validate='m:1', indicator='True')
        # df2['True'].value_counts()
        # 3.6 million matched, 1.9 million not matched for Rio. My guess is that this is because we dropped jids with fewer than 10 workers

        gs_sum = df2.groupby(['gamma','sector'])[earnings_var].sum().to_frame().reset_index().rename(columns={earnings_var:"gs"})
        s_sum = df2.groupby(['sector'])[earnings_var].sum().to_frame().reset_index().rename(columns={earnings_var:"s"})
        temp = gs_sum.merge(s_sum, on='sector')
        temp['alpha'] = temp['gs']/temp['s']
        alphas = temp[['gamma','sector','alpha']]
        
        if output is not None:
            alphas.to_csv(output, index=False)
            
        self.alphas=alphas
                


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

#model_rio.compute_alphas(level=2, earnings_file='./data/edges_rio_occ4_indv_data_10.csv', earnings_var='salario', output='./data/rio_alphas_level_2.csv')





