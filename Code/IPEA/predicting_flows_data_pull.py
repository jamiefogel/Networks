
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import graph_tool.all as gt
import scipy.sparse as sp
import copy
import sys

homedir = os.path.expanduser('~')
root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
os.chdir(root)

import bisbm
import create_df_trans
from pull_one_year import pull_one_year


run_df=True
run_sbm = False
state_codes = [31, 33, 35]
rio_codes = [330045, 330170, 330185, 330190, 330200, 330227, 330250, 330285, 330320, 330330, 330350, 330360, 330414, 330455, 330490, 330510, 330555, 330575]
region_codes = pd.read_csv('Data/raw/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})

firstyear = 2013
lastyear = 2018

#maxrows = 100000
maxrows=None

#modelname='junk'
modelname = 'pred_flows'
if maxrows!=None:
    modelname = modelname + '_small'

# 2004 appears to be the first year in which we have job start end dates (data_adm and data_deslig)

# CPI: 06/2015=100
cpi = pd.read_csv('./Data/raw/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
cpi['cpi'] = cpi.cpi/100
cpi['date'] = cpi['date'].dt.to_period('M')

''' Don't need to re-run every time
# Load original bisbm model
estimated_sbm = pickle.load( open('./Data/model_3states_2013to2016.p', "rb" ))

# Load state from mcmc sweeps and create a new bisbm object
[state_mcmc,i] = pickle.load( open('./Data/state_mcmc_iters.p', "rb" ))
estimated_sbm_mcmc = copy.deepcopy(estimated_sbm)
estimated_sbm_mcmc.state = state_mcmc
estimated_sbm_mcmc.export_blocks(output='./Data/model_3states_2013to2016_mcmc_blocks.csv', joutput='./Data/model_3states_2013to2016_mcmc_jblocks.csv', woutput='./Data/model_3states_2013to2016_mcmc_wblocks.csv')
pickle.dump( estimated_sbm_mcmc, open('./Data/model_3states_2013to2016_mcmc.p', "wb" ), protocol=4 )
'''

estimated_sbm_mcmc = pickle.load( open('./Data/derived/sbm_output/model_3states_2013to2016_mcmc.p', "rb" ) )
gammas = pd.read_csv('./Data/derived/sbm_output/model_3states_2013to2016_mcmc_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
gammas['gamma'] = gammas.gamma.fillna(-1)
iotas = pd.read_csv('./Data/derived/sbm_output/model_3states_2013to2016_mcmc_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid': object}).rename(columns={'worker_blocks_level_0':'iota'})
iotas['iota'] = iotas.iota.fillna(-1)
    
########################################################################################
########################################################################################
# Create a wid-jid-month panel
########################################################################################
########################################################################################

if run_df==True:
    for year in range(firstyear,lastyear+1):
        raw = pull_one_year(year, 'cbo2002', othervars=['data_adm'], state_codes=state_codes, age_lower=25, age_upper=55, parse_dates=['data_adm'], nrows=maxrows)
        # Deflate
        raw['start_date'] = pd.to_datetime(raw['data_adm'])
        raw = raw.merge(muni_meso_cw, how='left', on='codemun', copy=False) # validate='m:1', indicator=True)
        raw['occ2Xmeso'] = raw.cbo2002.str[0:2] + '_' + raw['code_meso'].astype('str')
        raw = raw.merge(gammas, on='jid', how='left')
        raw['gamma'] = raw.gamma.fillna(-1)
        raw = raw.merge(iotas, on='wid', how='left')
        raw['iota'] = raw.iota.fillna(-1)
        raw = raw.drop(columns=['yob','occ4','tipo_vinculo','idade','codemun','id_estab'])
        raw.to_pickle('./Data/derived/' + modelname + '_raw_' + str(year) + '.p')
        gc.collect()
    
    raw2013 = pd.read_pickle('./Data/derived/' + modelname + '_raw_2013.p')
    raw2014 = pd.read_pickle('./Data/derived/' + modelname + '_raw_2014.p')
    raw2015 = pd.read_pickle('./Data/derived/' + modelname + '_raw_2015.p')
    raw2016 = pd.read_pickle('./Data/derived/' + modelname + '_raw_2016.p')
    raw2017 = pd.read_pickle('./Data/derived/' + modelname + '_raw_2017.p')
    raw2018 = pd.read_pickle('./Data/derived/' + modelname + '_raw_2018.p')
    
    #####################
    # Create data frame of transitions


df_trans_ins = create_df_trans([raw2013,raw2014,raw2015,raw2016])
df_tran_ins.to_pickle('./Data/derived/predicting_flows/' + modelname + '_df_trans_ins.p')

df_trans_oos = create_df_trans([raw2017,raw2018])
df_tran_oos.to_pickle('./Data/derived/predicting_flows/' + modelname + '_df_trans_oos.p')



########################################################################################
########################################################################################
# Create the group-level adjacency matrices that we will use to draw simulated flows
########################################################################################
########################################################################################



##################
# Unipartite flows
# -- The full version of our model would be undirected, so I'm going with undirected to be consistent with that.
#
# This section creates two graphs --- g_gamma and g_occ2Xmeso --- representing gamma->gamma and occ2Xmeso->occ2Xmeso flows, respectively.
# - We restrict to observations where jid!=jid_prev and none of jid_prev, gamma_prev, and occ2Xmeso_prev are missing
# - Create an edgelist where the columns are mkt_prev and mkt
# - Create a graph from the edgelist using gt.add_edge_list()
# - Save a crosswalk between the vertex ids created when making the graph and the original IDs (which correspond to gamma or occ2Xmeso)

####################################################
# Note 7/20/2023
#  - the section of code below does 2 things: creates a market-to-market adjacency matrix (akmts, or ag and ao), and creates market-level degree counts (cjid)
#  - I need to turn this into a function that takes as arguments (i) what type of market to compute it for (jid, gamma or occ2Xmeso), and (ii) which dataset to use (df_trans or df_trans_oos)
# The function
def create_unipartite_adjacency_and_degrees(mkt, df_trans):
    # Compute the adjacency matrix
    g = gt.Graph(directed=False)
    g_vertices = g.add_edge_list(df_trans[[mkt+'_prev',mkt]].values, hashed=True)
    adjacency = gt.adjacency(g)
    # Compute the total degrees associated  with each mkt. Note that the number of degrees is 2x the number of edges b/c it counts both in- and out-degrees
    mkt_id = g.new_vertex_property("string")
    g.vp[mkt] = mkt_id
    for g in g.vertices():
        mkt_id[g] = g_vertices[g]
    mkt_degreecount = pd.DataFrame({mkt:g.vp.mkt_id.get_2d_array([0]).ravel(),mkt+'_degreecount':g.degree_property_map('total').a}).reset_index()
    return[adjacency,mkt_degreecount]   

    


Run this 4 times for each of [ins,oos]x[gamma,occ2Xmeso]

XX One issue: in gamma_degreecount_new the gammas will I think be a string rather than a float because I dropped the .astype('float')
gamma_degreecount_new['gamma'] = gamma_degreecount_new['gamma'].astype('float')

[ag_new,    gamma_degreecount_new]      = create_unipartite_adjacency_and_degrees('gamma',      df_tran_ins)
[ajid_new,  jid_degreecount_new]        = create_unipartite_adjacency_and_degrees('jid',        df_tran_ins)
[ao_new,    occ2Xmeso_degreecount_new]  = create_unipartite_adjacency_and_degrees('occ2Xmeso',  df_tran_ins)

# Confirming that the new version equals the old
[ag, ao, ajid] = pickle.load(open('./Data/derived/predicting_flows/adjacencies_no_graphtool.p', 'rb'))
ag==ag_new
ao==ao_new
ajid==ajid_new




########################################################################################
########################################################################################
# Dataframe for jobs
#
# - The idea here is to create two things: (i) a job-to-job adjacency matrix that is the object we are trying to
#   predict (ajid), and (ii) create a job degree dataframe d_j (cjid). The job degree dataframe will also need to
#   have columns for the job's gamma and occ2Xmeso so that we can merge things on later. 
# - Will want to write a function that takes which version of df_trans as an argument so we can do this for 
#   13-16 or 17-18
########################################################################################
########################################################################################

XX Next: convert this to a function
- I actually think that I can use the same function for both market2market and job2job. The one possibly non-trivial difference is that market2market has the extra command .astype('float'). Im not sure if this matters or if theres any reason why it should work differently for one versus the other. 

def create_job2job_adjacency_and_degrees(mkt, df_trans):
    g_jid = gt.Graph(directed=False)
    g_jid_vertices = g_jid.add_edge_list(df_trans[['jid_prev','jid']].values, hashed=True)
    adjacency = gt.adjacency(g_jid)           

    # Save the jids as internal vertex properties
    # For some reason vmap.get_2d_array([0]) gives me an error but the below code works. It feels like a hack, but it gets the job done and looping over 1.5 million vertices should be very fast. 
    jid = g_jid.new_vertex_property("string")
    g_jid.vp["jid"] = jid
    for g in g_jid.vertices():
        jid[g] = g_jid_vertices[g]
    jid_degreecount = pd.DataFrame({'jid':g_jid.vp.jid.get_2d_array([0]).ravel(),'jid_degreecount':g_jid.degree_property_map('total').a}).reset_index()
    return[adjacency,jid_degreecount]   


# Compute a mapping of jids to market definitions (gammas and occ2Xmeso). Do this by taking the set of jids, occ2mesos, and gammas that are ever origins and stack the set of jids, occ2Xmesos, and gammas that are ever destinations. Then drop duplicates on jid and keep the columns jid, gamma, and occ2Xmeso. 
jid_mkt_cw = pd.concat([df_trans.loc[(df_trans.jid!=df_trans.jid_prev) & (df_trans.jid_prev.notnull())][['jid', 'gamma', 'occ2Xmeso']], df_trans.loc[(df_trans.jid!=df_trans.jid_prev) & (df_trans.jid_prev.notnull())][['jid_prev', 'gamma_prev', 'occ2Xmeso_prev']].rename(columns={'jid_prev':'jid','gamma_prev':'gamma', 'occ2Xmeso_prev':'occ2Xmeso'})]).drop_duplicates(subset=['jid']) #XX gamma and occ2Xmeso aren~t unique within jid here. Figure out what's going on there

# Merge the jid to market crosswalk back on to the jid degree counts
jid_degreecount = jid_degreecount.merge(jid_mkt_cw, on='jid', how='outer', validate='1:1')


#########################################################
# Compute total degrees by market and merge back on to the jid degree counts

jid_degreecount = jid_degreecount.merge(gamma_degreecount[['gamma',        'gamma_degreecount']],     on='gamma',     how='left', validate='m:1')
jid_degreecount = jid_degreecount.merge(occ2Xmeso_degreecount[['occ2Xmeso','occ2Xmeso_degreecount']], on='occ2Xmeso', how='left', validate='m:1')
jid_degreecount = jid_degreecount.sort_values(by='index')
jid_degreecount.to_pickle('./Data/derived/predicting_flows/' + modelname + '_jid_degreecount.p')





# Create a df with job degree, gamma, occ2Xmeso, that is sorted according to the rows of the matrix. Probably worth also including jid to be safe.






# alternative that does the same thing as the loop above but stores it a numpy array instead of a vertex property map
#prop_array = np.array([vmap[v] for v in g_jid.vertices()])
def add_ids_as_property(graph, ids):
    id_prop = graph.new_vertex_property("string")
    id_prop = np.array([ids[v] for v in graph.vertices()])
    graph.vp["ids"] = id_prop
    return graph, id_prop

pickle.dump( g_jid, open('./Data/derived/predicting_flows/' + modelname + '_g_jid.p', "wb" ) )    
    


'''
This should all be able to be incorporated into the functions I need to write above
########################################################################################
########################################################################################
# P_F (Out-of-sample flows, 2017-2018)
########################################################################################
########################################################################################

raw2017 = pd.read_pickle('./Data/derived/' + modelname + '_raw_2017.p')
raw2018 = pd.read_pickle('./Data/derived/' + modelname + '_raw_2018.p')
df_1718 = pd.concat([raw2017,raw2018], axis=0)
df_1718 = df_1718.sort_values(by=['wid','start_date'])
df_1718['jid_prev'] = df_1718.groupby('wid')['jid'].shift(1)
df_1718['gamma_prev'] = df_1718.groupby('wid')['gamma'].shift(1)
df_1718['occ2Xmeso_prev'] = df_1718.groupby('wid')['occ2Xmeso'].shift(1)

# Restrict to jids that appear in the 13-16 data
# Only keep jids that appear in the 13-16 data and thus have gammas and occ2Xmesos

jid_oos_degreecount =
df_1718.merge(jid_mkt_cw, on='jid', how='inner', validate='1:1')


# Restrict to obs for which we have a valid gamma
df_1718 = df_1718[(df_1718['gamma'].notnull()) & (df_1718['gamma_prev'].notnull()) & (df_1718['gamma'] != -1) & (df_1718['gamma_prev'] != -1) & (df['gamma_prev'].notnull()) & (df['iota'] != -1) & (df['occ2Xmeso'].notnull()) & (df_1718['jid'].notnull()) & (df_1718['jid_prev'].notnull())]

#    df_trans = df[(df['gamma'].notnull()) & (df['gamma_prev'].notnull()) & (df['gamma'] != -1) & (df['gamma_prev'] != -1) & (df['iota'] != -1) & (df['occ2Xmeso'].notnull()) & (df['jid'].notnull())][['jid','jid_prev','wid','iota','gamma','gamma_prev','occ2Xmeso','occ2Xmeso_prev']]


# "oos" denotes "out of sample"

##############################################################################
# Compute market-level degrees that I will merge onto job-level later
                  
g_gamma_oos = gt.Graph(directed=False)
g_occ2Xmeso_oos = gt.Graph(directed=False)
# Add Edges
cond = (df_1718.jid!=df_1718.jid_prev) & (df_1718.jid_prev.notnull()) & (df_1718.gamma_prev.notnull()) & (df_1718.occ2Xmeso_prev.notnull())
g_gamma_oos_vertices     = g_gamma_oos.add_edge_list(    df_1718.loc[cond][['gamma_prev',    'gamma'    ]].values, hashed=True)
pickle.dump( g_gamma_oos, open('./Data/derived/predicting_flows/' + modelname + '_g_gamma_oos.p', "wb" ) )
g_occ2Xmeso_oos_vertices = g_occ2Xmeso_oos.add_edge_list(df_1718.loc[cond][['occ2Xmeso_prev','occ2Xmeso']].values, hashed=True)
pickle.dump( g_occ2Xmeso_oos, open('./Data/derived/predicting_flows/' + modelname + '_g_occ2Xmeso_oos.p', "wb" ) )



# Compute the total degrees associated  with each gamma. Note that the number of degrees is 2x the number of edges b/c it counts both in- and out-degrees
gamma_oos = g_gamma_oos.new_vertex_property("string")
g_gamma_oos.vp["gamma"] = gamma_oos
for g in g_gamma_oos.vertices():
    gamma_oos[g] = g_gamma_oos_vertices[g]

gamma_oos_degreecount = pd.DataFrame({'gamma':g_gamma_oos.vp.gamma.get_2d_array([0]).ravel().astype('float'),'gamma_oos_degreecount':g_gamma_oos.degree_property_map('total').a}).reset_index()
gamma_oos_degreecount.to_pickle('./Data/derived/predicting_flows/' + modelname + '_gamma_oos_degreecount.p')

# Compute the total degrees associated with each occ2Xmeso. Note that the number of degrees is 2x the number of edges b/c it counts both in- and out-degrees
occ2Xmeso_oos = g_occ2Xmeso_oos.new_vertex_property("string")
g_occ2Xmeso_oos.vp["occ2Xmeso"] = occ2Xmeso_oos
for g in g_occ2Xmeso_oos.vertices():
    occ2Xmeso_oos[g] = g_occ2Xmeso_oos_vertices[g]

occ2Xmeso_oos_degreecount = pd.DataFrame({'occ2Xmeso':g_occ2Xmeso_oos.vp.occ2Xmeso.get_2d_array([0]).ravel(),'occ2Xmeso_oos_degreecount':g_occ2Xmeso_oos.degree_property_map('total').a}).reset_index()
occ2Xmeso_oos_degreecount.to_pickle('./Data/derived/predicting_flows/' + modelname + '_occ2Xmeso_oos_degreecount.p')





                  
                
edgelist_oos = df_1718.loc[cond][['jid','jid_prev']]
g_jid_oos = gt.Graph(directed=False)
# Add Edges
vmap_oos = g_jid_oos.add_edge_list(edgelist_oos.values, hashed=True)

jid_oos = g_jid_oos.new_vertex_property("string")
g_jid_oos.vp["jid"] = jid_oos
for g in g_jid_oos.vertices():
    jid_oos[g] = vmap_oos[g]


# vmap = g_jid.add_edge_list(df_trans.loc[cond][['jid_prev','jid']].values, hashed=True)

# # For some reason vmap.get_2d_array([0]) gives me an error but the below code works. It feels like a hack, but it gets the job done and looping over 1.5 million vertices should be very fast. 
# jid = g_jid.new_vertex_property("string")
# g_jid.vp["jid"] = jid
# for g in g_jid.vertices():
#     jid[g] = vmap[g]


    
    
pickle.dump( g_jid_oos, open('./Data/derived/predicting_flows/' + modelname + '_g_jid_oos.p', "wb" ) )


jid_oos_degreecount = pd.DataFrame({'jid':g_jid_oos.vp.jid.get_2d_array([0]).ravel(),'degree':g_jid_oos.degree_property_map('total').a}).reset_index()

# Merge the jid to market crosswalk back on to the jid degree counts

jid_oos_degreecount = jid_oos_degreecount.merge(jid_mkt_cw, on='jid', how='inner', validate='1:1')
jid_oos_degreecount = jid_oos_degreecount.merge(gamma_oos_degreecount[['gamma',        'gamma_oos_degreecount']],     on='gamma',     how='left', validate='m:1')
jid_oos_degreecount = jid_oos_degreecount.merge(occ2Xmeso_oos_degreecount[['occ2Xmeso','occ2Xmeso_oos_degreecount']], on='occ2Xmeso', how='left', validate='m:1')
jid_oos_degreecount = jid_oos_degreecount.sort_values(by='index')
jid_oos_degreecount.to_pickle('./Data/derived/predicting_flows/' + modelname + '_jid_oos_degreecount.p')


                  

#jid_degreecount = pd.DataFrame({'jid':g_jid.vp.jid.get_2d_array([0]).ravel(),'degree':g_jid.degree_property_map('total').a}).reset_index()


# The code below loads objects used for predicting flows that require graph-tool and stores them as an object that doesn't require using graph-tool to load. Only run it on the python server.

ajid = gt.adjacency(pickle.load(open('./Data/derived/predicting_flows/pred_flows_g_jid.p', 'rb')))           
objects = [ag, ao, ajid]
pickle.dump(objects, open('./Data/derived/predicting_flows/adjacencies_no_graphtool.p', 'wb'))

'''
########################################################################################
########################################################################################
# iota-gamma predictions
#
# - I think this section is independent of everything above
########################################################################################
########################################################################################

# Creating the object $\tilde d_{m \omega}$, a GxI matrix of iota-gamma match counts

I = estimated_sbm_mcmc.num_worker_blocks[0]
G = estimated_sbm_mcmc.num_job_blocks[0]

# this matrix will have one row and column for each node in the bipartite network, but most rows/columns will be empty. The next step is to extract only the non-empty rows and columns. The result will be of size (I+G)x(I+G)
A_ig_big = estimated_sbm_mcmc.state.get_levels()[0].get_matrix()
# Find the row indices that contain at least one non-zero element
nonzero_rows = A_ig_big.getnnz(axis=1).nonzero()[0]
# Find the column indices that contain at least one non-zero element
nonzero_columns = A_ig_big.getnnz(axis=0).nonzero()[0]
# Extract the non-zero rows and columns and then extract the upper right block.
# - The upper left and lower right are all 0s
# - The lower left is the transpose of the upper right and thus redundant. 
A_ig = A_ig_big[nonzero_rows][:, nonzero_columns].toarray()[0:G,G:I+G]

# Take the row sums, reshape them to be a column vector using [:, np.newaxis], and then divide each row by the row sum to convert the counts into probabilities. 
d_g_tilde = A_ig.sum(axis=1)
d_i_tilde = A_ig.sum(axis=0)
d_g = np.ravel(ag.sum(axis=1)/2)
d_g_div_d_g_tilde = np.diag(d_g/d_g_tilde)

d_i_tilde_inv = np.linalg.inv(np.diag(d_i_tilde))

# This gives us what we've been calling \tilde d_{m'|m}. What we really want is \tilde d_{mm'}=\tilde d_{m'|m} + \tilde d_{m|m'}. Compute this on the following line
d_gg_tilde_asymm = d_g_div_d_g_tilde @ A_ig @ d_i_tilde_inv @ np.transpose(A_ig)
d_gg_tilde = d_gg_tilde_asymm + d_gg_tilde_asymm.T
# d_gg_tilde.sum() = ag.sum() = 31671758
pickle.dump(d_gg_tilde, open('./Data/derived/predicting_flows/'+modelname+'_d_gg_tilde.p', 'wb'))


# Sanity check: the sum of the d_gg_tilde matrix equals the number of edges in the gamma-to-gamma transition matrix. Without the rescaling factor it would sum to the number of edges in the bipartite adjacency matrix. 
print(d_gg_tilde.sum())
print(g_gamma)








