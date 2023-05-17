# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 16:25:54 2023

@author: p13861161
"""

from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
from scipy.sparse import coo_matrix
import scipy.sparse as sparse
import graph_tool.all as gt

'''
if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')

os.chdir(homedir + '/labormkt_rafaelpereira/aug2022/code/')
'''
os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions')

os.listdir()

'''
# ADJACENCY MATRICES
ag = gt.adjacency(pickle.load(open('pred_flows_g_gamma.p', 'rb')))
ao = gt.adjacency(pickle.load(open('pred_flows_g_occ2Xmeso.p', 'rb')))
ajid = gt.adjacency(pickle.load(open('pred_flows_g_jid.p', 'rb')))           


# CROSS-WALKS
cg = pickle.load(open('pred_flows_gamma_cw.p', 'rb'))
co = pickle.load(open('pred_flows_occ2Xmeso_cw.p', 'rb'))
cjid = pickle.load(open('pred_flows_jid_cw.p', 'rb'))
'''

#### LOADING AN EXAMPLE
ag = sparse.csr.csr_matrix(pd.read_csv('ag_example.csv', header=None))
ag.toarray()
ajid = sparse.csr.csr_matrix(pd.read_csv('ajid_example.csv', header=None))
ajid.toarray()
cg = pd.read_csv('cg_example.csv')
cg
cjid = pd.read_csv('cjid_example.csv')
cjid
cjid = cjid.rename({'index':'idx'}, axis=1)

# check that all networks are consistent
np.sum(cjid['degree'])
np.sum(ag)
#np.sum(ao)

D_outsample = np.sum(ajid)
D_insample = np.sum(ag)
J = ajid.shape[0]

cg['within_flow_g'] = ag.diagonal()

# chance of choosing an edge connecting to j within its market
cjid['djdg'] = cjid['degree'] / cjid['cardinality_gamma']
#cjid['djdo'] = cjid['degree'] / cjid['cardinality_occ2Xmeso']
cjid['gg_count_temp'] = np.NaN

# get the number of matches between the current gamma to all other gammas (assuming cg is sorted according to ag)
cjid = cjid.reset_index().merge(cg[['within_flow_g','gamma']], left_on='gamma', right_on='gamma').set_index('idx')

# APPROACH 1 TO DISTRIBUTE SELF EDGE PROBS TO OTHER EDGES
# getting the predition for the diagonal (i.e. for a job to itself), which has to be distributed accros jobs
cjid['pred_prob_diag'] = (cjid['within_flow_g'] / D_insample) * np.square(cjid['djdg'])
# accumulating the diagonal prob per gamma
cjid['pred_prob_diag_gamma'] = cjid.loc[:,['gamma','pred_prob_diag']].groupby('gamma').transform('sum')

start_time = datetime.now()  # get the start time

#pred_matrix = pd.DataFrame(None)

######## START LOOP HERE: FOR ALL JOBS
j = 0
print_percentage = .00005
print_increment = np.round(J // 100/print_percentage)
print_increment = 1

pred_error = []

for j in range(J):
    # get the market of job j
    g = cjid['gamma'][j]
    
    # get the gamma index in the gamma crosswalk df
    g_index = cg.loc[cg['gamma']==g].index[0]
    
    # get the number of matches between the current gamma to all other gammas (assuming cg is sorted according to ag)
    cg['gg_count_temp'] = ag.getrow(g_index).toarray()[0]
    
    # XXBM: EFFICIENCY IMPROVEMENT: sort obs per market and/or do the merge below only if the gamma for the previous job is different than the gamma for the current job
    # merge the number of matches between gamma_j and gamma_j' to the job crosswalk df
    cjid = cjid.drop(['gg_count_temp'], axis=1).reset_index().merge(cg[['gg_count_temp','gamma']], left_on='gamma', right_on='gamma').set_index('idx')
    
    # MAIN THING: predicted probability for a match between j and i
    cjid['pred_prob'] = (cjid['gg_count_temp'] / D_insample) * cjid['djdg'] * cjid['djdg'][j]
    
    # distributing self edge probability
    #cjid = cjid.sort_values('idx')
    cjid['self_pred_prob_distributed'] = (cjid['degree'][j] + cjid['degree']) / (cjid['cardinality_gamma']) / (2**(np.sum(cjid['gamma']==g)-1)) * cjid['pred_prob_diag_gamma']  * (cjid['gamma']==g)
    
    #cjid.set_index('index')
    #cjid = cjid.sort_values('idx')
    pred = cjid['pred_prob'] + cjid['self_pred_prob_distributed']
    pred[j] = 0
    #pred_matrix.loc[:,j] = pred
    
    # prediction error
    pred_error.append(np.average((pred.sort_index()*D_outsample).values - ajid.getrow(j).toarray()))

    if j % print_increment == 0:
        # calculate the percentage progress and print it
        progress_percent = j / J * 100
        now = datetime.now()
        elapsed_time = now - start_time  # calculate the elapsed time
        pred_error_pd = pd.DataFrame(pred_error)
        pred_error_pd.to_csv('pred_error.csv')
        print('j = ' + str(j) + ' / ' + str(J) + ', ' + f"{progress_percent}% complete. Elapsed time: {elapsed_time}. Time: {now}")    
      
    

#pred_matrix = pred_matrix.sort_index()
#print(pred_matrix)
#print(pred_matrix.to_numpy().sum())    

def check_symmetric(a, rtol=1e-05, atol=1e-08):
    return np.allclose(a, a.T, rtol=rtol, atol=atol)

#check_symmetric(pred_matrix.to_numpy())
#print(pred_error)










