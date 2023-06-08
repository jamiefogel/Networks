from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
from scipy.sparse import coo_matrix
import scipy.sparse as sparse
#import graph_tool.all as gt
import sys

# DIRECTORY WHERE THE DATA IS
# Automatically chooses the folder between windows vs linux servers
# if os.name == 'nt':
#     homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
# else:
#     homedir = os.path.expanduser('~/labormkt')

# This should be deleted in the future
# os.chdir(homedir + '/labormkt_rafaelpereira/aug2022/dump/')
os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions')


## QUESTIONS/COMMENTS
# are we holding the degree distribution of each job fixed in our predictions? i.e. is the # number of transitions predicted for a job equal to its degree?
# we want to create a way to verify the sorting of the adjacency matrices ag, ao, ajid --- have the indices used in their sorting


# ADJACENCY MATRICES
# Loading the following adjacency matrices:
    # ajid: job to job transitions
    # ag: gamma to gamma transitions
    # ao: occ2-meso to occ2-meso transition

# The commented code below loads these objects using graph-tool and store them object that doesn't require using graph-tool to load
##ag = gt.adjacency(pickle.load(open('pred_flows_g_gamma.p', 'rb')))
##ao = gt.adjacency(pickle.load(open('pred_flows_g_occ2Xmeso.p', 'rb')))
##ajid = gt.adjacency(pickle.load(open('pred_flows_g_jid.p', 'rb')))           
##objects = [ag, ao, ajid]
##pickle.dump(objects, open('adjacencies_no_graphtool.p', 'wb'))

# Loading the adjacency objects without the need of graphtool
objects = pickle.load(open('adjacencies_no_graphtool.p', 'rb'))
ag = objects[0]
ao = objects[1]
ajid = objects[2]


# CROSS-WALKS
# Loading important information to compute transition probabilities
# It basically has the gamma/occ2-meso/job-id cardinalities
cg = pickle.load(open('pred_flows_gamma_cw.p', 'rb'))
co = pickle.load(open('pred_flows_occ2Xmeso_cw.p', 'rb'))
cjid = pd.read_pickle(open('pred_flows_jid_cw.p', 'rb'))

# Data prep
cjid = cjid.rename({'index':'idx'}, axis=1)


# this is to be deleted later
# check that all networks are consistent
if not ((int(np.sum(cjid['degree'])) == int(np.sum(ag))) and (int(np.sum(ao)) == int(np.sum(cjid['degree'])))):
    raise Exception('Dimensions do not match')

# Computes total number of transitions in-sample, out-of-sample and and total number of jobs
D_outsample = np.sum(ajid)
D_insample = np.sum(ag)
J = ajid.shape[0]


# NOTE FOR THE BLOCK OF CODE BELOW:
# we are adding new columns to the cross-walk objects from swiftly getting info necessary to the probability computation. The idea is to have all relevant info in the same row
# The for loop below goes over all jobs, so we don't want to compute all of the info below for every job in the loop


# Storing gamma to same gamma flows (i.e. internal flows)
cg['within_flow_g'] = ag.diagonal()

# chance of choosing an edge connecting to j within its market: degree divided by cardinality of the market    
    # for gamma markets
cjid['djdg'] = cjid['degree'] / cjid['cardinality_gamma']
    # for occ2-meso markets
#cjid['djdo'] = cjid['degree'] / cjid['cardinality_occ2Xmeso']

# Temporary variable
cjid['gg_count_temp'] = np.NaN

# get the number of matches between the current gamma to all other gammas (assuming cg is sorted according to ag)
    # this quantity is necessary for the probability correction on overleaf (i.e. to DISTRIBUTE SELF EDGE PROBS TO OTHER EDGES)
cjid = cjid.reset_index().merge(cg[['within_flow_g','gamma']], left_on='gamma', right_on='gamma').set_index('idx')

# APPROACH 1 TO DISTRIBUTE SELF EDGE PROBS TO OTHER EDGES
# getting the prediction for the diagonal (i.e. for a job to itself), which has to be distributed accros jobs
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

# Function to output the probability prediction error of transitioning from job 'j' to all other jobs, based on a market configuration given by the objects ag and cg
    # it returns a vector of length j
def prediction_error(j,cjid, cg, ajid, ag, D_insample,D_outsample, print_increment,J, start_time):
    # get the market of job j
    g = cjid['gamma'][j]
   
    # get the gamma index in the gamma dataset, in order to retrive market information later
    g_index = cg.loc[cg['gamma']==g].index[0]
   
    # get the number of matches between the current gamma to all gammas (assuming cg is sorted according to ag)
    # This info is contained in the gamma adjacency matrix, but we want to extract a vector out of it to make computations easier
    # Alternatively: get the 1st row of the market to market adjacency matrix and append it to the market dataset
    cg['gg_count_temp'] = ag.getrow(g_index).toarray()[0]
   
    # XXBM: EFFICIENCY IMPROVEMENT: sort obs per market and/or do the merge below only if the gamma for the previous job is different than the gamma for the current job
    # merge a vector of length J (total number of jobs) in which each element is the number of transitions between gamma_j (current job j's gamma) to gamma_j' (all other jobs' gammas) to the job dataset
    cjid = cjid.drop(['gg_count_temp'], axis=1).reset_index().merge(cg[['gg_count_temp','gamma']], left_on='gamma', right_on='gamma').set_index('idx')
   
    # MAIN THING: predicted probability for a match between j and j' (see spreadsheet and/or overleaf)
        # spreadsheet: /Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions/
        # overleaf: https://www.overleaf.com/project/6400d14789d986d7ed453675
    # for each job j, we will have a vector of length j with the # of transitions expected from job j to all other jobs
    cjid['pred_prob'] = (cjid['gg_count_temp'] / D_insample) * cjid['djdg'] * cjid['djdg'][j]
   
    # CORRECTION TO DISTRITUBE SELF-EDGE PROBABILITIES
        # OUTSTANDING PROBLEM: This piece in the denominator increases exponentially for some market 2**(np.sum(cjid['gamma']==g)-1). However, python assigns very large numbers to zero, when it can't compute it.
        # CURRENT SOLUTION: Ignore the probability correction. Maybe markets are big enough so it doesn't matter that much
        # ALTERNATIVE SOLUTION: write an if statement using the condition (2**(np.sum(cjid['gamma']==g)-1) == 0), and if that is true, hardcode a really high number for it
        # according to the formula we are using, we would predict a non-zero transition from a job to itself, which is not matched in the real job to job transition matrix ajid
        # the code below distributes the predicted #transitions from a job to itself roughly according to all other job degrees, forcing the self-transitions to be zero
    # distributing self edge probability
    #cjid = cjid.sort_values('idx')   # sort the job dataset just to check calculations
    #cjid['self_pred_prob_distributed'] = (cjid['degree'][j] + cjid['degree']) / (cjid['cardinality_gamma']) / (2**(np.sum(cjid['gamma']==g)-1)) * cjid['pred_prob_diag_gamma']  * (cjid['gamma']==g)
    
    # finalizing the predicted probability after distributing the self edge probabilities
    pred = cjid['pred_prob'] #+ cjid['self_pred_prob_distributed']
    pred[j] = 0   # the diagonal should be zero (i.e. no flows from a job to itself)
   
    # Compute two vectors of length J with the L1 and L2 errors
        # compare the predicted flows from j to all other j' to the actual number of transitions in the job-to-job adjacency matrix ajid
    error1 = np.sum(np.abs((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    error2 = np.sum(np.square((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    
    # this would be to track time
    # OUTSTANDING PROBLEM: when this function is being passed using the multiprocessing package below, it is not printing anything
    # POSSIBLE SOLUTION: move the printing out of this function and inside the multiprocessing loop below
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




# COMPUTING THE JOB TO JOB TRANSITIONS LOOPING OVER ALL JOBS WITH THE HELP OF MULTIPROCESSING PACKAGE
    # the function prediction_error above computes the transition prediction error for each job. We loop over all jobs with parallelization below
    # OUTSTANDING PROBLEM: on the windows server (on spyder), it uses more cores. On the linux server, it doesn't seem to be using the number of cores specified below
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




