# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 09:40:25 2023

@author: p13861161
"""
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
from scipy.sparse import coo_matrix
import scipy.sparse as sparse
import sys


######################################################################
### PREDICTION ERROR FUNCTION FOR UNIPARTITE NETWORK
######################################################################

# Function to output the probability prediction error of transitioning from job 'j' to all other jobs, based on a market configuration given by the objects ag and cg
    # it returns a vector of length j
    # C represents crosswalk, A represents adjacency
def prediction_error_uni(j,cjid, c_mkt, ajid, a_mkt, D_insample,D_outsample,J,mkt):
    # getting the 1st letter of the market
    m = mkt[0]
    
    # get the market of job j
    g = cjid[mkt][j]
   
    # get the gamma index in the gamma dataset, in order to retrive market information later
    g_index = c_mkt.loc[c_mkt[mkt]==g].index[0]
   
    # get the number of matches between the current gamma to all gammas (assuming c_mkt is sorted according to a_mkt)
    # This info is contained in the gamma adjacency matrix, but we want to extract a vector out of it to make computations easier
    # Alternatively: get the 1st row of the market to market adjacency matrix and append it to the market dataset
    c_mkt['mm_count_temp'+m] = a_mkt[g_index,:].reshape(-1,1)
   
    # XXBM: EFFICIENCY IMPROVEMENT: sort obs per market and/or do the merge below only if the gamma for the previous job is different than the gamma for the current job
    # merge a vector of length J (total number of jobs) in which each element is the number of transitions between gamma_j (current job j's gamma) to gamma_j' (all other jobs' gammas) to the job dataset
    # XXJSF: looks like we are creating a new column called cjid['mm_count_temp'+m], then immediately dropping it, then merging it back on. Why not delete the line cjid['mm_count_temp'+m] = np.NaN and create the column in the merge on the next line? Bernardo said he was getting some sort of error and this solved it so maybe necessary. Can test.
    cjid['mm_count_temp'+m] = np.NaN
    cjid = cjid.drop(['mm_count_temp'+m], axis=1).reset_index().merge(c_mkt[['mm_count_temp'+m,mkt]], left_on=mkt, right_on=mkt).set_index('idx')
   
    # MAIN THING: predicted probability for a match between j and j' (see spreadsheet and/or overleaf)
        # spreadsheet: /Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions/
        # overleaf: https://www.overleaf.com/project/6400d14789d986d7ed453675
    # for each job j, we will have a vector of length j with the # of transitions expected from job j to all other jobs
    cjid['pred_prob'] = (cjid['mm_count_temp'+m] / D_insample) * cjid['djd'+m] * cjid['djd'+m][j]
    # cjid['mm_count_temp'+m] corresponds to dmm' in the eq' at the bottom of section 2.3.1 on Overleaf.
    # D_insample is total number of transitions in the data on which we estimated model
    #  cjid['pred_prob'] is a matrix of probabilities. Multiplied by d_outsample below to get total number of transitions. So cjid['pred_prob'] should sum to 1 (in each row? Not sure exactly how it sums to 1.). 
   
    # CORRECTION TO DISTRIBUTE SELF-EDGE PROBABILITIES
        # OUTSTANDING PROBLEM: This piece in the denominator increases exponentially for some market 2**(np.sum(cjid['gamma']==g)-1). However, python assigns very large numbers to zero, when it can't compute it.
        # CURRENT SOLUTION: using an if statement using the condition (2**(np.sum(cjid['gamma']==g)-1) == 0), and if that is true, hardcode the highest possible integer for
        # according to the formula we are using, we would predict a non-zero transition from a job to itself, which is not matched in the real job to job transition matrix ajid
        # the code below distributes the predicted #transitions from a job to itself roughly according to all other job degrees, forcing the self-transitions to be zero
    # distributing self edge probability
    term_denominator = 2**(np.sum(cjid[mkt]==g)-1)
    if term_denominator == 0:
        term_denominator = sys.maxsize
    cjid = cjid.sort_values('idx')   # sort the job dataset just to check calculations
    cjid['self_pred_prob_distributed'] = (cjid['jid_degreecount'][j] + cjid['jid_degreecount']) / (cjid[mkt+'_degreecount']) / (term_denominator) * cjid['pred_prob_diag_'+m]  * (cjid[mkt]==g)
    
    # finalizing the predicted probability after distributing the self edge probabilities
    cjid.loc[j,'pred_prob'] = 0
    pred = cjid['pred_prob'] #+ cjid['self_pred_prob_distributed']
   
    # Compute two vectors of length J with the L1 and L2 errors
        # compare the predicted flows from j to all other j' to the actual number of transitions in the job-to-job adjacency matrix ajid
    error1 = np.sum(np.abs((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    error2 = np.sum(np.square((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    
    return([error1, error2])
