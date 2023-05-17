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

os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions')


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

# market to market transition matrix (adjacency matrix)
ag = sparse.csr.csr_matrix(pd.read_csv('ag_example.csv', header=None))
ag.toarray()

# job to job transition matrix (adjacency matrix)
ajid = sparse.csr.csr_matrix(pd.read_csv('ajid_example.csv', header=None))
ajid.toarray()

# market data (e.g. cardinality)
cg = pd.read_csv('cg_example.csv')
cg

# job data (e.g. degree, market, market cardinality)
cjid = pd.read_csv('cjid_example.csv')
cjid
cjid = cjid.rename({'index':'idx'}, axis=1)

# check that all networks are consistent
np.sum(cjid['degree'])
np.sum(ag)
#np.sum(ao)

# D_insample is the degree of the matrix we are using to predict
# D_outsample is the degree of the matrix we are predicting
D_outsample = np.sum(ajid)
D_insample = np.sum(ag)

# getting total number of jobs
J = ajid.shape[0]

# Getting the flows of the market to itself and storing them at the market dataset
cg['within_flow_g'] = ag.diagonal()

## CREATING VARIABLES TO THE JOB DATASET (we're going to be retriving them in the for loop)
# chance of choosing an edge connecting to j within its market
cjid['djdg'] = cjid['degree'] / cjid['cardinality_gamma']
#cjid['djdo'] = cjid['degree'] / cjid['cardinality_occ2Xmeso']
# this is just to do computations inside the for loop
cjid['gg_count_temp'] = np.NaN

# Merging the within flow gamma into the job dataset
cjid = cjid.reset_index().merge(cg[['within_flow_g','gamma']], left_on='gamma', right_on='gamma').set_index('idx')

# APPROACH TO DISTRIBUTE SELF EDGE PROBS TO OTHER EDGES
# getting the probability predition for the diagonal (i.e. for a job to itself), which has to be distributed accros jobs
cjid['pred_prob_diag'] = (cjid['within_flow_g'] / D_insample) * np.square(cjid['djdg'])
# accumulating the diagonal prob per gamma
cjid['pred_prob_diag_gamma'] = cjid.loc[:,['gamma','pred_prob_diag']].groupby('gamma').transform('sum')

# get the start time
start_time = datetime.now()  

# PRE FOR LOOP CONTROLS
j = 0  # this was just to test the for loop (can be deleted later)
print_percentage = .40   # choose the percetage of the total # of iterations for which you want to print a timestamp
print_increment = np.round(J * print_percentage)
#print_increment = 1

# objects to store prediction error
pred_error = []
pred_error_sq = []


def prediction_error(j,cjid, cg, ajid, ag):
    # get the market of job j
    g = cjid['gamma'][j]
    
    # get the gamma index in the gamma dataset, in order to retrive market information later
    g_index = cg.loc[cg['gamma']==g].index[0]
    ## JASMINE: try to incorporate the index to the cjid before the loop, as opposed to get the index here    
    
    # get the number of matches between the current gamma to all gammas (assuming cg is sorted according to ag)
    # Alternatively: get the 1st row of the market to market adjacency matrix and append it to the market dataset
    cg['gg_count_temp'] = ag.getrow(g_index).toarray()[0]
    
    # XXBM: EFFICIENCY IMPROVEMENT: sort obs per market and/or do the merge below only if the gamma for the previous job is different than the gamma for the current job
    # merge the number of matches between gamma_j and gamma_j' to the job dataset
    cjid = cjid.drop(['gg_count_temp'], axis=1).reset_index().merge(cg[['gg_count_temp','gamma']], left_on='gamma', right_on='gamma').set_index('idx')
    
    # MAIN THING: predicted probability for a match between j and j' (see spreadsheet and/or overleaf)
    cjid['pred_prob'] = (cjid['gg_count_temp'] / D_insample) * cjid['djdg'] * cjid['djdg'][j]
    
    # distributing self edge probability
    #cjid = cjid.sort_values('idx')   # sort the job dataset just to check calculations
    cjid['self_pred_prob_distributed'] = (cjid['degree'][j] + cjid['degree']) / (cjid['cardinality_gamma']) / (2**(np.sum(cjid['gamma']==g)-1)) * cjid['pred_prob_diag_gamma']  * (cjid['gamma']==g)
    # finalizing the predicted probability after distributing the self edge probabilities
    pred = cjid['pred_prob'] + cjid['self_pred_prob_distributed']
    pred[j] = 0   # the diagonal should be zero (i.e. no flows from a job to itself)
    
    error1 = np.sum(np.abs((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    error2 = np.sum(np.square((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    
    
    
    # this would be to track time
    if j % print_increment == 0:
        # calculate the percentage progress and print it
        progress_percent = j / J * 100
        elapsed_time = datetime.now() - start_time  # calculate the elapsed time
        # intermediate step saving outputs
        #pred_error_pd = pd.DataFrame(errors)
        #pred_error_pd.to_csv('pred_error.csv')
        print('j = ' + str(j) + ' / ' + str(J) + ', ' + f"{progress_percent}% complete. Elapsed time: {elapsed_time}. Time: {now}")    

    return([error1, error2])


# it would be nice to be able to choose the number of cores we want to use
cores = 3

######## START LOOP HERE: FOR ALL JOBS
for j in range(J):

    errors = prediction_error(j,cjid, cg, ajid, ag)    
    
    pred_error.append(errors[0])
    pred_error_sq.append(errors[1])
    
      
    
print(pred_error)
print(pred_error_sq)








