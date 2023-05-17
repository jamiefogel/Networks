
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import graph_tool.all as gt
import scipy.sparse as sp
import copy

homedir = os.path.expanduser('~')
os.chdir(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/code/')


import bisbm
from pull_one_year import pull_one_year


run_df=True
run_sbm = False

state_codes = [31, 33, 35]
rio_codes = [330045, 330170, 330185, 330190, 330200, 330227, 330250, 330285, 330320, 330330, 330350, 330360, 330414, 330455, 330490, 330510, 330555, 330575]
region_codes = pd.read_csv(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/external/munic_microregion_rm.csv', encoding='latin1')
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
cpi = pd.read_csv(homedir + '/labormkt/labormkt_rafaelpereira/ExternalData/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
cpi['cpi'] = cpi.cpi/100
cpi['date'] = cpi['date'].dt.to_period('M')

''' Don't need to re-run every time
# Load original bisbm model
estimated_sbm = pickle.load( open('../data/model_3states_2013to2016.p', "rb" ))

# Load state from mcmc sweeps and create a new bisbm object
[state_mcmc,i] = pickle.load( open('../data/state_mcmc_iters.p', "rb" ))
estimated_sbm_mcmc = copy.deepcopy(estimated_sbm)
estimated_sbm_mcmc.state = state_mcmc
estimated_sbm_mcmc.export_blocks(output='../data/model_3states_2013to2016_mcmc_blocks.csv', joutput='../data/model_3states_2013to2016_mcmc_jblocks.csv', woutput='../data/model_3states_2013to2016_mcmc_wblocks.csv')
pickle.dump( estimated_sbm_mcmc, open('../data/model_3states_2013to2016_mcmc.p', "wb" ), protocol=4 )
'''

estimated_sbm_mcmc = pickle.load( open('../data/model_3states_2013to2016_mcmc.p', "rb" ) )
gammas = pd.read_csv('../data/model_3states_2013to2016_mcmc_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
gammas['gamma'] = gammas.gamma.fillna(-1)
iotas = pd.read_csv('../data/model_3states_2013to2016_mcmc_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid': object}).rename(columns={'worker_blocks_level_0':'iota'})
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
9613   23600334324 2017-04-03  00000000044725_4132  00000000044725_4132     1.0         1.0
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
        raw.to_pickle('../dump/' + modelname + '_raw_' + str(year) + '.p')
        gc.collect()
    
    raw2013 = pd.read_pickle('../dump/' + modelname + '_raw_2013.p')
    raw2014 = pd.read_pickle('../dump/' + modelname + '_raw_2014.p')
    raw2015 = pd.read_pickle('../dump/' + modelname + '_raw_2015.p')
    raw2016 = pd.read_pickle('../dump/' + modelname + '_raw_2016.p')
    raw2017 = pd.read_pickle('../dump/' + modelname + '_raw_2017.p')
    raw2018 = pd.read_pickle('../dump/' + modelname + '_raw_2018.p')
    df = pd.concat([raw2013,raw2014,raw2015,raw2016], axis=0)
    df = df.sort_values(by=['wid','start_date'])
    df['jid_prev'] = df.groupby('wid')['jid'].shift(1)
    df['gamma_prev'] = df.groupby('wid')['gamma'].shift(1)
    df['occ2Xmeso_prev'] = df.groupby('wid')['occ2Xmeso'].shift(1)
    df.to_pickle('../dump/' + modelname + '_df.p')
    
    
    # Restrict to obs with non-missing gammas, occ2Xmesos, jid, and jid_prev.
    # XX should I actually be cutting on non-missing jid_prev? I think I should actually wait to do that until making the unipartite transition matrices below. For the bipartite there is no reason why we need to have observed a previous jid. 
    df_trans = df[(df['gamma'].notnull()) & (df['gamma'] != -1) & (df['gamma'].notnull()) & (df['iota'] != -1) & (df['occ2Xmeso'].notnull()) & (df['jid'].notnull())][['jid','jid_prev','wid','iota','gamma','gamma_prev','occ2Xmeso','occ2Xmeso_prev']]
    df_trans.to_pickle('../dump/' + modelname + '_df_trans.p')


df_trans = pd.read_pickle('../dump/' + modelname + '_df_trans.p')

########################################################################################
########################################################################################
# Create the group-level adjacency matrices that we will use to draw simulated flows
########################################################################################
########################################################################################

##################
# Unipartite flows
# -- The full version of our model would be undirected, so I'm going with undirected to be consistent with that.
g_gamma = gt.Graph(directed=False)
g_occ2Xmeso = gt.Graph(directed=False)
# Add Edges
g_gamma_vertices = g_gamma.add_edge_list(df_trans.loc[(df_trans.jid!=df_trans.jid_prev) & (df_trans.jid_prev.notnull()) & (df_trans.gamma_prev.notnull()) & (df_trans.occ2Xmeso_prev.notnull())][['gamma_prev','gamma']].values, hashed=True)
pickle.dump( g_gamma, open('../dump/' + modelname + '_g_gamma.p', "wb" ) )
g_occ2Xmeso_vertices = g_occ2Xmeso.add_edge_list(df_trans.loc[(df_trans.jid!=df_trans.jid_prev) & (df_trans.jid_prev.notnull()) & (df_trans.gamma_prev.notnull()) & (df_trans.occ2Xmeso_prev.notnull())][['occ2Xmeso_prev','occ2Xmeso']].values, hashed=True)
pickle.dump( g_occ2Xmeso, open('../dump/' + modelname + '_g_occ2Xmeso.p', "wb" ) )

'''
gamma = g_gamma.new_vertex_property("string")
g_gamma.vp["gamma"] = gamma
for g in g_gamma.vertices():
    gamma[g] = g_gamma_vertices[g]

occ2Xmeso = g_occ2Xmeso.new_vertex_property("string")
g_occ2Xmeso.vp["occ2Xmeso"] = occ2Xmeso
for g in g_occ2Xmeso.vertices():
    occ2Xmeso[g] = g_occ2Xmeso_vertices[g]

'''

##################
# Cross-sectional bipartite networks (iota-gamma, iota-occ2Xmeso)
g_iota_occ2Xmeso = gt.Graph(directed=False)
g_iota_gamma = gt.Graph(directed=False)
g_iota_occ2Xmeso.add_edge_list(df_trans.drop_duplicates(subset=['wid','jid'])[['iota','occ2Xmeso']].values, hashed=True)
g_iota_gamma.add_edge_list(df_trans.drop_duplicates(subset=['wid','jid'])[['iota','gamma']].values, hashed=True)
pickle.dump( g_iota_gamma, open('../dump/' + modelname + '_g_iota_gamma.p', "wb" ) )
pickle.dump( g_iota_occ2Xmeso, open('../dump/' + modelname + '_g_iota_occ2Xmeso.p', "wb" ) )



########################################################################################
########################################################################################
# Dataframe for jobs
########################################################################################
########################################################################################


# Create a df with job degree, gamma, occ2Xmeso, that is sorted according to the rows of the matrix. Probably worth also including jid to be safe.
g_jid = gt.Graph(directed=False)
vmap = g_jid.add_edge_list(df_trans.loc[(df_trans.jid!=df_trans.jid_prev) & (df_trans.jid_prev.notnull()) & (df_trans.gamma_prev.notnull()) & (df_trans.occ2Xmeso_prev.notnull())][['jid_prev','jid']].values, hashed=True)

# For some reason vmap.get_2d_array([0]) gives me an error but the below code works. It feels like a hack, but it gets the job done and looping over 1.5 million vertices should be very fast. 
jid = g_jid.new_vertex_property("string")
g_jid.vp["jid"] = jid
for g in g_jid.vertices():
    jid[g] = vmap[g]

pickle.dump( g_jid, open('../dump/' + modelname + '_g_jid.p', "wb" ) )    
    
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
gamma_cw.to_pickle('../dump/' + modelname + '_gamma_cw.p')

occ2Xmeso = g_occ2Xmeso.new_vertex_property("string")
g_occ2Xmeso.vp["occ2Xmeso"] = occ2Xmeso
for g in g_occ2Xmeso.vertices():
    occ2Xmeso[g] = g_occ2Xmeso_vertices[g]

occ2Xmeso_cw = pd.DataFrame({'occ2Xmeso':g_occ2Xmeso.vp.occ2Xmeso.get_2d_array([0]).ravel(),'cardinality_occ2Xmeso':g_occ2Xmeso.degree_property_map('total').a}).reset_index()
occ2Xmeso_cw.to_pickle('../dump/' + modelname + '_occ2Xmeso_cw.p')


jid_cw = jid_cw.merge(gamma_cw[['gamma','cardinality_gamma']], on='gamma', how='outer', validate='m:1')
jid_cw = jid_cw.merge(occ2Xmeso_cw[['occ2Xmeso','cardinality_occ2Xmeso']], on='occ2Xmeso', how='outer', validate='m:1')
jid_cw = jid_cw.sort_values(by='index')
jid_cw.to_pickle('../dump/' + modelname + '_jid_cw.p')

'''
# Just checking stuff. I confirmed that the degrees computed by summing rows and columns of the adjacency matrix equal the degrees computed using degree_property_map('total')
jid_cw
adj = gt.adjacency(g_jid).todense()
adj[0,:].sum()
adj[:,0].sum()
adj[1,:].sum()
adj[2,:].sum()
'''


########################################################################################
########################################################################################
# P_F (Out-of-sample flows, 2017-2018)
########################################################################################
########################################################################################

raw2017 = pd.read_pickle('../dump/' + modelname + '_raw_2017.p')
raw2018 = pd.read_pickle('../dump/' + modelname + '_raw_2018.p')
df_1718 = pd.concat([raw2017,raw2018], axis=0)
df_1718 = df_1718.sort_values(by=['wid','start_date'])
df_1718['jid_prev'] = df_1718.groupby('wid')['jid'].shift(1)
# Restrict to obs for which we have a valid gamma
df_1718 = df_1718[(df_1718['gamma'].notnull()) & (df_1718['gamma'] != -1) & (df_1718['jid'].notnull()) & (df_1718['jid_prev'].notnull())]
# "gt" denotes "ground truth"
edgelist_gt = df_1718.loc[df_1718['jid']!=df_1718['jid_prev']][['jid','jid_prev']]
g_gt = gt.Graph(directed=False)
# Add Edges
ids = g_gt.add_edge_list(edgelist_gt.values, hashed=True)
pickle.dump( g_gt, open('../dump/' + modelname + '_g_gt.p', "wb" ) )



# Example code taken from graph-tool docs
#g = gt.collection.data["polblogs"]
#g = gt.GraphView(g, vfilt=gt.label_largest_component(g))
#g = gt.Graph(g, prune=True)
#state = gt.minimize_blockmodel_dl(g)
#u = gt.generate_sbm(state.b.a, gt.adjacency(state.get_bg(), state.get_ers()).T, g.degree_property_map("out").a, g.de#gree_property_map("in").a, directed=True)
#gt.similarity(g,u)

########################################################################################
########################################################################################
# Attempt at the prediction exercise
########################################################################################
########################################################################################


level = 0
e_sbm_level_0 = estimated_sbm_mcmc.state.project_level(0)
import time

start_time = time.time()
g_sbm_ig = gt.generate_sbm(e_sbm_level_0.b.a, gt.adjacency(e_sbm_level_0.get_bg(), e_sbm_level_0.get_ers()).T, estimated_sbm_mcmc.g.degree_property_map("out").a, directed=False)
end_time = time.time()
elapsed_time = end_time - start_time
print("Elapsed time: {:.2f} seconds".format(elapsed_time))

start_time = time.time()
gt.similarity(estimated_sbm_mcmc.g, g_sbm_ig)
end_time = time.time()
elapsed_time = end_time - start_time
print("Elapsed time: {:.2f} seconds".format(elapsed_time))

# Generating the matrix was fast.




# Create the sparse edge list
edgelist_grouped = edgelist_gt.groupby(['jid', 'jid_prev']).size().reset_index(name='count')
# Add upper triangular to lower triangular and vice versa
edgelist_grouped = pd.concat([edgelist_grouped, edgelist_grouped.rename(columns={'jid_prev':'jid','jid':'jid_prev'})], axis=0)

edgelist_grouped['jid_prev'] = pd.Categorical(edgelist_grouped['jid_prev'])
edgelist_grouped['jid'] = pd.Categorical(edgelist_grouped['jid'])
edgelist_grouped['jid_prev_code'] = edgelist_grouped['jid_prev'].cat.codes
edgelist_grouped['jid_code'] = edgelist_grouped['jid'].cat.codes

edgelist_sparse = sp.coo_matrix((edgelist_grouped['count'].values, (edgelist_grouped['jid_prev_code'].values, edgelist_grouped['jid_code'].values)))
# XX Need to save a crsswalk of jid and jid_code and ensure we have the rows and columns in the correct order

pickle.dump(edgelist_sparse, open('../dump/' + modelname + '_edgelist_sparse.p', "wb" ) )

# convert to csr format if necessary
edgelist_sparse = edgelist_sparse.tocsr()

pickle.dump(edgelist_sparse, open('../dump/' + modelname + '_edgelist_sparse_csr.p', "wb" ) )

# sample_graph is how we draw a new network
# Next steps: generate the probabilities from which we generate networks.
# - iota-occ2Xmeso




g = gt.lattice([5,5])
is_biparitite, part = gt.is_bipartite(g, partition=True)
gt.graph_draw(g, vertex_fill_color=part)  # to view the full graph coloured by set

from itertools import combinations

g_temp = g.copy()  # this is a deepcopy

for v, bipartite_label in enumerate(part):
    if bipartite_label == 0:
        neighbours = list(g.vertex(v).all_neighbours())
        for s, t in combinations(neighbours, 2):
            g_temp.add_edge(s, t)

g_projected = gt.Graph(gt.GraphView(g_temp, vfilt=part.a==1), prune=True)

gt.graph_draw(g_projected)
