
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
from create_df_trans import create_df_trans
from create_unipartite_adjacency_and_degrees import create_unipartite_adjacency_and_degrees
from pull_one_year import pull_one_year


run_pull= True 

state_codes = [31, 33, 35]
rio_codes = [330045, 330170, 330185, 330190, 330200, 330227, 330250, 330285, 330320, 330330, 330350, 330360, 330414, 330455, 330490, 330510, 330555, 330575]
region_codes = pd.read_csv('Data/raw/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})


#maxrows = 10000
maxrows=None


# 2004 appears to be the first year in which we have job start end dates (data_adm and data_deslig)

# CPI: 06/2015=100
cpi = pd.read_csv('./Data/raw/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
cpi['cpi'] = cpi.cpi/100
cpi['date'] = cpi['date'].dt.to_period('M')


''' Don't need to re-run every time. Hopefully can delete but need to confirm
# Load original bisbm model
estimated_sbm = pickle.load( open('./Data/model_3states_2013to2016.p', "rb" ))

# Load state from mcmc sweeps and create a new bisbm object
[state_mcmc,i] = pickle.load( open('./Data/state_mcmc_iters.p', "rb" ))
estimated_sbm_mcmc = copy.deepcopy(estimated_sbm)
estimated_sbm_mcmc.state = state_mcmc
estimated_sbm_mcmc.export_blocks(output='./Data/model_3states_2013to2016_mcmc_blocks.csv', joutput='./Data/model_3states_2013to2016_mcmc_jblocks.csv', woutput='./Data/model_3states_2013to2016_mcmc_wblocks.csv')
pickle.dump( estimated_sbm_mcmc, open('./Data/model_3states_2013to2016_mcmc.p', "wb" ), protocol=4 )
'''


estimated_sbm_mcmc = pickle.load( open('./Data/derived/sbm_output/model_'+modelname+'.p', "rb" ) )
gammas = pd.read_csv('./Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
gammas['gamma'] = gammas.gamma.fillna(-1)
iotas = pd.read_csv('./Data/derived/sbm_output/model_'+modelname+'_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid': object}).rename(columns={'worker_blocks_level_0':'iota'})
iotas['iota'] = iotas.iota.fillna(-1)
    
########################################################################################
########################################################################################
# Create a wid-jid-month panel
########################################################################################
########################################################################################

if run_pull==True:
    raw_dict_ins = {}
    raw_dict_oos = {}
    for year in ins_years + oos_years:
        raw = pull_one_year(year, 'cbo2002', othervars=['data_adm'], state_codes=state_codes, age_lower=25, age_upper=55, parse_dates=['data_adm'], nrows=maxrows, filename=rais_filename_stub + str(year) + '.csv')
        # Because dates aren't stored correctly in some years. Also we had a very small number of invalid dates (5 out of hundreds of millions) and this sets them to missing rather than failing.
        raw['start_date'] = pd.to_datetime(raw['data_adm'], errors='coerce')
        raw['codemun'] = raw['codemun'].astype(int)
        raw = raw.merge(muni_meso_cw, how='left', on='codemun', copy=False) # validate='m:1', indicator=True)
        raw['occ2Xmeso'] = raw.cbo2002.str[0:2] + '_' + raw['code_meso'].astype('str')
        raw = raw.merge(gammas, on='jid', how='left')
        raw['gamma'] = raw.gamma.fillna(-1)
        raw = raw.merge(iotas, on='wid', how='left')
        raw['iota'] = raw.iota.fillna(-1)
        raw = raw.drop(columns=['yob','occ4','tipo_vinculo','idade','codemun','id_estab'])
        raw.to_pickle('./Data/derived/' + modelname + '_pred_flows_raw_' + str(year) + '.p')
        gc.collect()
        if year in ins_years:
            raw_dict_ins[year] = raw
        elif year in oos_years:
            raw_dict_oos[year] = raw
    pickle.dump(raw_dict_ins, open('./Data/derived/' + modelname + '_pred_flows_raw_dict_ins.p', 'wb'))
    pickle.dump(raw_dict_oos, open('./Data/derived/' + modelname + '_pred_flows_raw_dict_oos.p', 'wb'))



#####################
# Create data frame of transitions

df_trans_ins = create_df_trans([raw_dict_ins[year] for year in ins_years])
df_trans_ins.to_pickle('./Data/derived/predicting_flows/' + modelname + '_pred_flows_df_trans_ins.p')

df_trans_oos = create_df_trans([raw_dict_oos[year] for year in oos_years])
df_trans_oos.to_pickle('./Data/derived/predicting_flows/' + modelname + '_pred_flows_df_trans_oos.p')

# Compute a mapping of jids to market definitions (gammas and occ2Xmeso). Do this by taking the set of jids, occ2mesos, and gammas that are ever origins and stack the set of jids, occ2Xmesos, and gammas that are ever destinations. Then drop duplicates on jid and keep the columns jid, gamma, and occ2Xmeso. 
jid_mkt_cw_ins = pd.concat([df_trans_ins[['jid', 'gamma', 'occ2Xmeso']], df_trans_ins[['jid_prev', 'gamma_prev', 'occ2Xmeso_prev']].rename(columns={'jid_prev':'jid','gamma_prev':'gamma', 'occ2Xmeso_prev':'occ2Xmeso'})]).drop_duplicates(subset=['jid'])
jid_mkt_cw_oos = pd.concat([df_trans_oos[['jid', 'gamma', 'occ2Xmeso']], df_trans_oos[['jid_prev', 'gamma_prev', 'occ2Xmeso_prev']].rename(columns={'jid_prev':'jid','gamma_prev':'gamma', 'occ2Xmeso_prev':'occ2Xmeso'})]).drop_duplicates(subset=['jid']) 



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



[ag_ins,    gamma_degreecount_ins]      = create_unipartite_adjacency_and_degrees('gamma',      df_trans_ins)
[ao_ins,    occ2Xmeso_degreecount_ins]  = create_unipartite_adjacency_and_degrees('occ2Xmeso',  df_trans_ins)
[ajid_ins,  jid_degreecount_ins]        = create_unipartite_adjacency_and_degrees('jid',        df_trans_ins)

[ag_oos,    gamma_degreecount_oos]      = create_unipartite_adjacency_and_degrees('gamma',      df_trans_oos)
[ajid_oos,  jid_degreecount_oos]        = create_unipartite_adjacency_and_degrees('jid',        df_trans_oos)
[ao_oos,    occ2Xmeso_degreecount_oos]  = create_unipartite_adjacency_and_degrees('occ2Xmeso',  df_trans_oos)

# Drop the "index" column from the market-level degree counts because we don't actually use it and keeping it around could cause confusion.
gamma_degreecount_ins.drop(columns='index', inplace=True)
occ2Xmeso_degreecount_ins.drop(columns='index', inplace=True)
gamma_degreecount_oos.drop(columns='index', inplace=True)
occ2Xmeso_degreecount_oos.drop(columns='index', inplace=True)

# Some very small occ2Xmesos appear in the ins data but not oos. Add rows to the degree counts dataframe for these with a degreecount value of 0. 
occ2Xmeso_degreecount_oos = occ2Xmeso_degreecount_oos.merge(occ2Xmeso_degreecount_ins['occ2Xmeso'], on='occ2Xmeso', how='outer', validate='1:1').fillna(0)

pickle.dump(ag_ins,     open('./Data/derived/predicting_flows/'+modelname+'_ag_ins.p', 'wb'))
pickle.dump(ao_ins,     open('./Data/derived/predicting_flows/'+modelname+'_ao_ins.p', 'wb'))
pickle.dump(ajid_ins,   open('./Data/derived/predicting_flows/'+modelname+'_ajid_ins.p', 'wb'))

pickle.dump(ag_oos,     open('./Data/derived/predicting_flows/'+modelname+'_ag_oos.p', 'wb'))
pickle.dump(ao_oos,     open('./Data/derived/predicting_flows/'+modelname+'_ao_oos.p', 'wb'))
pickle.dump(ajid_oos,   open('./Data/derived/predicting_flows/'+modelname+'_ajid_oos.p', 'wb'))

pickle.dump(gamma_degreecount_oos,         open('./Data/derived/predicting_flows/'+modelname+'_gamma_degreecount_oos.p', 'wb'))
pickle.dump(occ2Xmeso_degreecount_oos,     open('./Data/derived/predicting_flows/'+modelname+'_occ2Xmeso_degreecount_oos.p', 'wb'))


############################################################
# Merge market-level degrees back on to jid_degreecount

# Merge the jid to market crosswalk back on to the jid degree counts
jid_degreecount_ins = jid_degreecount_ins.merge(jid_mkt_cw_ins, on='jid', how='outer', validate='1:1')

# Merge total degrees by market onto the jid degree counts
jid_degreecount_ins = jid_degreecount_ins.merge(gamma_degreecount_ins[['gamma',        'gamma_degreecount']],     on='gamma',     how='left', validate='m:1')
jid_degreecount_ins = jid_degreecount_ins.merge(occ2Xmeso_degreecount_ins[['occ2Xmeso','occ2Xmeso_degreecount']], on='occ2Xmeso', how='left', validate='m:1')
jid_degreecount_ins = jid_degreecount_ins.sort_values(by='index')
jid_degreecount_ins.to_pickle('./Data/derived/predicting_flows/' + modelname + '_jid_degreecount_ins.p')



# Merge the jid to market crosswalk back on to the jid degree counts
jid_degreecount_oos = jid_degreecount_oos.merge(jid_mkt_cw_oos, on='jid', how='outer', validate='1:1')

# Merge total degrees by market onto the jid degree counts
jid_degreecount_oos = jid_degreecount_oos.merge(gamma_degreecount_oos[['gamma',        'gamma_degreecount']],     on='gamma',     how='left', validate='m:1')
jid_degreecount_oos = jid_degreecount_oos.merge(occ2Xmeso_degreecount_oos[['occ2Xmeso','occ2Xmeso_degreecount']], on='occ2Xmeso', how='left', validate='m:1')
jid_degreecount_oos = jid_degreecount_oos.sort_values(by='index')
jid_degreecount_oos.to_pickle('./Data/derived/predicting_flows/' + modelname + '_jid_degreecount_oos.p')



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
# -The upper left and lower right are all 0s
# - The lower left is the transpose of the upper right and thus redundant. 
A_ig = A_ig_big[nonzero_rows][:, nonzero_columns].toarray()[0:G,G:I+G]

# Take the row sums, reshape them to be a column vector using [:, np.newaxis], and then divide each row by the row sum to convert the counts into probabilities. 
d_g_tilde = A_ig.sum(axis=1)
d_i_tilde = A_ig.sum(axis=0)
d_g = np.ravel(ag_ins.sum(axis=1)/2)
d_g_div_d_g_tilde = np.diag(d_g/d_g_tilde)

d_i_tilde_inv = np.linalg.inv(np.diag(d_i_tilde))

# This gives us what we've been calling \tilde d_{m'|m}. What we really want is \tilde d_{mm'}=\tilde d_{m'|m} + \tilde d_{m|m'}. Compute this on the following line
d_gg_tilde_asymm = d_g_div_d_g_tilde @ A_ig @ d_i_tilde_inv @ np.transpose(A_ig)
d_gg_tilde = d_gg_tilde_asymm + d_gg_tilde_asymm.T
# d_gg_tilde.sum() = ag.sum() = 31671758
pickle.dump(d_gg_tilde, open('./Data/derived/predicting_flows/'+modelname+'_d_gg_tilde.p', 'wb'))










# alternative that does the same thing as the loop above but stores it a numpy array instead of a vertex property map
#prop_array = np.array([vmap[v] for v in g_jid.vertices()])
def add_ids_as_property(graph, ids):
    id_prop = graph.new_vertex_property("string")
    id_prop = np.array([ids[v] for v in graph.vertices()])
    graph.vp["ids"] = id_prop
    return graph, id_prop



