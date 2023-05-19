
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
from scipy.sparse import coo_matrix
import scipy.sparse as sparse
#import graph_tool.all as gt
import sys


if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')

os.chdir(homedir + '/labormkt_rafaelpereira/aug2022/dump/')

#os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions')
#os.listdir()


# ADJACENCY MATRICES
##ag = gt.adjacency(pickle.load(open('pred_flows_g_gamma.p', 'rb')))
##ao = gt.adjacency(pickle.load(open('pred_flows_g_occ2Xmeso.p', 'rb')))
##ajid = gt.adjacency(pickle.load(open('pred_flows_g_jid.p', 'rb')))           
##objects = [ag, ao, ajid]
##pickle.dump(objects, open('adjacencies_no_graphtool.p', 'wb'))
objects = pickle.load(open('adjacencies_no_graphtool.p', 'rb'))
ag = objects[0]
ao = objects[1]
ajid = objects[2]

# CROSS-WALKS
cg = pickle.load(open('pred_flows_gamma_cw.p', 'rb'))
co = pickle.load(open('pred_flows_occ2Xmeso_cw.p', 'rb'))
cjid = pickle.load(open('pred_flows_jid_cw.p', 'rb'))


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
print_percentage = .00025
print_increment = int(np.round(J // (100/print_percentage)))
print_increment = 1

pred_error = []


def prediction_error(j,cjid, cg, ajid, ag, D_insample,D_outsample, print_increment,J, start_time):
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
    #cjid['self_pred_prob_distributed'] = (cjid['degree'][j] + cjid['degree']) / (cjid['cardinality_gamma']) / (2**(np.sum(cjid['gamma']==g)-1)) * cjid['pred_prob_diag_gamma']  * (cjid['gamma']==g)
    
    # finalizing the predicted probability after distributing the self edge probabilities
    pred = cjid['pred_prob'] #+ cjid['self_pred_prob_distributed']
    pred[j] = 0   # the diagonal should be zero (i.e. no flows from a job to itself)
   
    error1 = np.sum(np.abs((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    error2 = np.sum(np.square((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    
    # this would be to track time
    if j % print_increment == 0:
    #if 0 == 0:        
        # calculate the percentage progress and print it
        progress_percent = j / J * 100
        now = datetime.now()
        elapsed_time = now - start_time  # calculate the elapsed time
        # intermediate step saving outputs
        #pred_error_pd = pd.DataFrame(errors)
        #pred_error_pd.to_csv('pred_error.csv')
        
        print('j = ' + str(j) + ' / ' + str(J) + ', ' + f"{progress_percent}% complete. Elapsed time: {elapsed_time}. Time: {now}")
    
    return([error1, error2,j])




#using multiprocessing package
import multiprocessing

if __name__ == '__main__':
    J = 2
    pool = multiprocessing.Pool(processes=2)
    results = []
    for j in range(J):
        result = pool.apply_async(prediction_error, args=(j, cjid, cg, ajid, ag , D_insample, D_outsample, print_increment, J, start_time))
        results.append(result)
    pool.close()
    pool.join()

    # Retrieve the results
    pred_error = []
    pred_error_sq = []
    for result in results:
        errors = result.get()
        pred_error.append(errors[0])
        pred_error_sq.append(errors[1])
   
    print(pred_error)
    print(pred_error_sq)




