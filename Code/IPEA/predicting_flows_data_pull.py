
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


''' Issue: data_adm uses different date formats for different years. e.g. M/D/Y in 2013 and D/M/Y in 2017. This could be true for other date variables as well.

df[['wid','start_date','jid','jid_prev','gamma','gamma_prev']]
               wid start_date                  jid             jid_prev   gamma  gamma_prev
9352   10000627817 2010-04-12  00000000042277_4132                  NaN  1208.0         NaN
27255  10000628066 1998-06-08  00000000394700_4132                  NaN     0.0         NaN
19756  10000631938 1997-12-22  00000000264849_4132                  NaN  1291.0         NaN
9594   10000637758 2010-08-24  00000000043915_4132                  NaN     1.0         NaN
16714  10000640848 2008-04-09  00000000198790_4132                  NaN     1.0         NaN
...            ...        ...                  ...                  ...     ...         ...
5160   22808914945 2017-02-06  00000000013412_4132  00000000120928_4132     1.0         1.0
4372   22808914945 2017-06-02  00000000013412_4132  00000000013412_4132     1.0         1.0
21361  22808914945 2018-02-14  00000000311278_4132  00000000013412_4132     1.0         1.0
11165  23600334324 2014-05-20  00000000044725_4132                  NaN     1.0         NaN
'9613   23600334324 2017-04-03  00000000044725_4132  00000000044725_4132     1.0         1.0
'''

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
    df = pd.concat([raw2013,raw2014,raw2015,raw2016], axis=0)
    df = df.sort_values(by=['wid','start_date'])
    df['jid_prev'] = df.groupby('wid')['jid'].shift(1)
    df['gamma_prev'] = df.groupby('wid')['gamma'].shift(1)
    df['occ2Xmeso_prev'] = df.groupby('wid')['occ2Xmeso'].shift(1)
    df.to_pickle('./Data/derived/predicting_flows/' + modelname + '_df.p')
    
    
    # Restrict to obs with non-missing gammas, occ2Xmesos, jid, and jid_prev.
    # XX should I actually be cutting on non-missing jid_prev? I think I should actually wait to do that until making the unipartite transition matrices below. For the bipartite there is no reason why we need to have observed a previous jid. 
    df_trans = df[(df['gamma'].notnull()) & (df['gamma'] != -1) & (df['gamma_prev'] != -1) & (df['gamma'].notnull()) & (df['iota'] != -1) & (df['occ2Xmeso'].notnull()) & (df['jid'].notnull())][['jid','jid_prev','wid','iota','gamma','gamma_prev','occ2Xmeso','occ2Xmeso_prev']]
    df_trans.to_pickle('./Data/derived/predicting_flows/' + modelname + '_df_trans.p')


df_trans = pd.read_pickle('./Data/derived/predicting_flows/' + modelname + '_df_trans.p')

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
g_gamma = gt.Graph(directed=False)
g_occ2Xmeso = gt.Graph(directed=False)
# Add Edges
cond = (df_trans.jid!=df_trans.jid_prev) & (df_trans.jid_prev.notnull()) & (df_trans.gamma_prev.notnull()) & (df_trans.occ2Xmeso_prev.notnull())
g_gamma_vertices     = g_gamma.add_edge_list(    df_trans.loc[cond][['gamma_prev',    'gamma'    ]].values, hashed=True)
pickle.dump( g_gamma, open('./Data/derived/predicting_flows/' + modelname + '_g_gamma.p', "wb" ) )
g_occ2Xmeso_vertices = g_occ2Xmeso.add_edge_list(df_trans.loc[cond][['occ2Xmeso_prev','occ2Xmeso']].values, hashed=True)
pickle.dump( g_occ2Xmeso, open('./Data/derived/predicting_flows/' + modelname + '_g_occ2Xmeso.p', "wb" ) )


# Just printing the crosswalk between the original vertex IDs (gammas or occ2Xmesos) and the vertex IDs created by graph-tool. This isn't really necessary, just saving it for reference.
for vertex in g_gamma.vertices():
    vertex_id = g_gamma_vertices[vertex]
    print("Vertex ID for vertex", vertex, ":", vertex_id)

for vertex in g_occ2Xmeso.vertices():
    vertex_id = g_occ2Xmeso_vertices[vertex]
    print("Vertex ID for vertex", vertex, ":", vertex_id)

    

##################
# Cross-sectional bipartite networks (iota-gamma, iota-occ2Xmeso)
g_iota_occ2Xmeso = gt.Graph(directed=False)
g_iota_gamma = gt.Graph(directed=False)
g_iota_occ2Xmeso.add_edge_list(df_trans.drop_duplicates(subset=['wid','jid'])[['iota','occ2Xmeso']].values, hashed=True)
g_iota_gamma.add_edge_list(df_trans.drop_duplicates(subset=['wid','jid'])[['iota','gamma']].values, hashed=True)
pickle.dump( g_iota_gamma, open('./Data/derived/predicting_flows/' + modelname + '_g_iota_gamma.p', "wb" ) )
pickle.dump( g_iota_occ2Xmeso, open('./Data/derived/predicting_flows/' + modelname + '_g_iota_occ2Xmeso.p', "wb" ) )



########################################################################################
########################################################################################
# Dataframe for jobs
########################################################################################
########################################################################################


# Create a df with job degree, gamma, occ2Xmeso, that is sorted according to the rows of the matrix. Probably worth also including jid to be safe.
g_jid = gt.Graph(directed=False)
vmap = g_jid.add_edge_list(df_trans.loc[cond][['jid_prev','jid']].values, hashed=True)

# For some reason vmap.get_2d_array([0]) gives me an error but the below code works. It feels like a hack, but it gets the job done and looping over 1.5 million vertices should be very fast. 
jid = g_jid.new_vertex_property("string")
g_jid.vp["jid"] = jid
for g in g_jid.vertices():
    jid[g] = vmap[g]

# alternative that does the same thing as the loop above but stores it a numpy array instead of a vertex property map
#prop_array = np.array([vmap[v] for v in g_jid.vertices()])
def add_ids_as_property(graph, ids):
    id_prop = graph.new_vertex_property("string")
    id_prop = np.array([ids[v] for v in graph.vertices()])
    graph.vp["ids"] = id_prop
    return graph, id_prop

pickle.dump( g_jid, open('./Data/derived/predicting_flows/' + modelname + '_g_jid.p', "wb" ) )    
    
jid_cw = pd.DataFrame({'jid':g_jid.vp.jid.get_2d_array([0]).ravel(),'degree':g_jid.degree_property_map('total').a}).reset_index()
a = df_trans[['jid','gamma','occ2Xmeso']].drop_duplicates(subset=['jid','gamma','occ2Xmeso'])
df_stacked = pd.concat([df_trans.loc[(df_trans.jid!=df_trans.jid_prev) & (df_trans.jid_prev.notnull())][['jid', 'gamma', 'occ2Xmeso']], df_trans.loc[(df_trans.jid!=df_trans.jid_prev) & (df_trans.jid_prev.notnull())][['jid_prev', 'gamma_prev', 'occ2Xmeso_prev']].rename(columns={'jid_prev':'jid','gamma_prev':'gamma', 'occ2Xmeso_prev':'occ2Xmeso'})]).drop_duplicates(subset=['jid']) #XX gamma and occ2Xmeso aren~t unique within jid here. Figure out what's going on there
jid_cw = jid_cw.merge(df_stacked, on='jid', how='outer', validate='1:1')

############################
# Compute cardinality of job transitions by market
''' Don't need to do it this way. This produces the same result as g_gamma.degree_property_map('total').a
gt.adjacency(g_gamma).todense().sum(axis=0)
gt.adjacency(g_occ2Xmeso).todense().sum(axis=0)
'''


gamma = g_gamma.new_vertex_property("string")
g_gamma.vp["gamma"] = gamma
for g in g_gamma.vertices():
    gamma[g] = g_gamma_vertices[g]

gamma_cw = pd.DataFrame({'gamma':g_gamma.vp.gamma.get_2d_array([0]).ravel().astype('float'),'cardinality_gamma':g_gamma.degree_property_map('total').a}).reset_index()
gamma_cw.to_pickle('./Data/derived/predicting_flows/' + modelname + '_gamma_cw.p')

occ2Xmeso = g_occ2Xmeso.new_vertex_property("string")
g_occ2Xmeso.vp["occ2Xmeso"] = occ2Xmeso
for g in g_occ2Xmeso.vertices():
    occ2Xmeso[g] = g_occ2Xmeso_vertices[g]

occ2Xmeso_cw = pd.DataFrame({'occ2Xmeso':g_occ2Xmeso.vp.occ2Xmeso.get_2d_array([0]).ravel(),'cardinality_occ2Xmeso':g_occ2Xmeso.degree_property_map('total').a}).reset_index()
occ2Xmeso_cw.to_pickle('./Data/derived/predicting_flows/' + modelname + '_occ2Xmeso_cw.p')


jid_cw = jid_cw.merge(gamma_cw[['gamma','cardinality_gamma']], on='gamma', how='outer', validate='m:1')
jid_cw = jid_cw.merge(occ2Xmeso_cw[['occ2Xmeso','cardinality_occ2Xmeso']], on='occ2Xmeso', how='outer', validate='m:1')
jid_cw = jid_cw.sort_values(by='index')
jid_cw.to_pickle('./Data/derived/predicting_flows/' + modelname + '_jid_cw.p')



# The code below loads objects used for predicting flows that require graph-tool and stores them as an object that doesn't require using graph-tool to load. Only run it on the python server.
ag = gt.adjacency(pickle.load(open('./Data/derived/predicting_flows/pred_flows_g_gamma.p', 'rb')))
ao = gt.adjacency(pickle.load(open('./Data/derived/predicting_flows/pred_flows_g_occ2Xmeso.p', 'rb')))
ajid = gt.adjacency(pickle.load(open('./Data/derived/predicting_flows/pred_flows_g_jid.p', 'rb')))           
objects = [ag, ao, ajid]
pickle.dump(objects, open('./Data/derived/predicting_flows/adjacencies_no_graphtool.p', 'wb'))


########################################################################################
########################################################################################
# iota-gamma predictions
########################################################################################
########################################################################################

# Creating the object $\tilde d_{m \omega}$, a GxI matrix of iota-gamma match counts
d_mw 


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
d_gg_tilde = d_g_div_d_g_tilde @ A_ig @ d_i_tilde_inv @ np.transpose(A_ig)

pickle.dump(d_gg_tilde, open('./Data/derived/predicting_flows/'+modelname+'_d_gg_tilde.p', 'wb'))


# Sanity check: the sum of the d_gg_tilde matrix equals the number of edges in the gamma-to-gamma transition matrix. Without the rescaling factor it would sum to the number of edges in the bipartite adjacency matrix. 
print(d_gg_tilde.sum())
print(g_gamma)




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
# Restrict to obs for which we have a valid gamma
df_1718 = df_1718[(df_1718['gamma'].notnull()) & (df_1718['gamma'] != -1) & (df_1718['jid'].notnull()) & (df_1718['jid_prev'].notnull())]
# "gt" denotes "ground truth"
edgelist_gt = df_1718.loc[df_1718['jid'!]=df_1718['jid_prev']][['jid','jid_prev']]
g_gt = gt.Graph(directed=False)
# Add Edges
ids = g_gt.add_edge_list(edgelist_gt.values, hashed=True)
pickle.dump( g_gt, open('./Data/derived/predicting_flows/' + modelname + '_g_gt.p', "wb" ) )







