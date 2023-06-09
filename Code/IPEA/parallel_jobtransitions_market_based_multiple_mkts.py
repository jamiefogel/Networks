from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
#from scipy.sparse import coo_matrix
#import scipy.sparse as sparse
#import graph_tool.all as gt
import sys

# DIRECTORY WHERE THE DATA IS
# Automatically chooses the folder between windows vs linux servers
if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')

# This should be deleted in the future
os.chdir(homedir + '/labormkt_rafaelpereira/aug2022/dump/')
#os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions')


## QUESTIONS/COMMENTS
# are we holding the degree distribution of each job fixed in our predictions? i.e. is the # number of transitions predicted for a job equal to its degree?
# we want to create a way to verify the sorting of the adjacency matrices ag, ao, ajid --- have the indices used in their sorting



###################################
### DATA LOAD
###################################

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
amkts = {'g': objects[0], 'o': objects[1]}
ajid = objects[2]


# CROSS-WALKS
# Loading important information to compute transition probabilities
# It basically has the gamma/occ2-meso/job-id cardinalities
cmkts = {'g': pickle.load(open('pred_flows_gamma_cw.p', 'rb')), 'o': pickle.load(open('pred_flows_occ2Xmeso_cw.p', 'rb'))}
cjid = pd.read_pickle(open('pred_flows_jid_cw.p', 'rb'))


###################################
### DATA PREP
###################################

# Renaming index to avoid errors later
cjid = cjid.rename({'index':'idx'}, axis=1)


# this is to be deleted later
# check that all networks are consistent
if not ((int(np.sum(cjid['degree'])) == int(np.sum(amkts['g']))) and (int(np.sum(amkts['o'])) == int(np.sum(cjid['degree'])))):
    raise Exception('Dimensions do not match')

# Computes total number of transitions in-sample, out-of-sample and and total number of jobs
D_outsample = np.sum(ajid)
D_insample = np.sum(amkts['g'])   # this should be the same as np.sum(amkts['o'])
J = ajid.shape[0]

# Temporary variable
#cjid['mm_count_temp'] = np.NaN


# NOTE FOR THE BLOCK OF CODE BELOW:
# we are adding new columns to the cross-walk objects from swiftly getting info necessary to the probability computation. The idea is to have all relevant info in the same row
# The for loop below goes over all jobs, so we don't want to compute all of the info below for every job in the loop

# The lines above are j specific. The lines below prepare the data for each market:
mkts = ['gamma', 'occ2Xmeso']

for mkt in mkts:

    m = mkt[0]

    # Storing gamma to same gamma flows (i.e. internal flows)
    cmkts[m]['within_flow_' + m] = amkts[m].diagonal()

    # probability of choosing an edge connecting to j within its market: degree divided by cardinality of the market    
    cjid['djd' + m] = cjid['degree'] / cjid['cardinality_' + mkt]

    # get the number of matches between the current gamma to all other gammas (assuming cg is sorted according to ag)
    # this quantity is necessary for the probability correction on overleaf (i.e. to DISTRIBUTE SELF EDGE PROBS TO OTHER EDGES)
    cjid = cjid.reset_index().merge(cmkts[m][['within_flow_'+m,mkt]], left_on=mkt, right_on=mkt).set_index('idx')

    # APPROACH 1 TO DISTRIBUTE SELF EDGE PROBS TO OTHER EDGES
    # getting the prediction for the diagonal (i.e. for a job to itself), which has to be distributed accros jobs
    cjid['pred_prob_diag_' + m] = (cjid['within_flow_'+m] / D_insample) * np.square(cjid['djd'+m])

    # accumulating the diagonal prob per gamma
    cjid['pred_prob_diag_acc_'+m] = cjid.loc[:,[mkt,'pred_prob_diag_'+m]].groupby(mkt).transform('sum')


####### TEMP!!!!! just to test the function
mkt = 'gamma'
m = mkt[0]
a_mkt = amkts[m]
c_mkt = cmkts[m]


###################################
### PREDICTION ERROR FUNCTION
###################################


# Function to output the probability prediction error of transitioning from job 'j' to all other jobs, based on a market configuration given by the objects ag and cg
    # it returns a vector of length j
    # C represents crosswalk, A represents adjacency
def prediction_error(j,cjid, c_mkt, ajid, a_mkt, D_insample,D_outsample,J,mkt):
    # getting the 1st letter of the market
    m = mkt[0]
    
    # get the market of job j
    g = cjid[mkt][j]
   
    # get the gamma index in the gamma dataset, in order to retrive market information later
    g_index = c_mkt.loc[c_mkt[mkt]==g].index[0]
   
    # get the number of matches between the current gamma to all gammas (assuming c_mkt is sorted according to a_mkt)
    # This info is contained in the gamma adjacency matrix, but we want to extract a vector out of it to make computations easier
    # Alternatively: get the 1st row of the market to market adjacency matrix and append it to the market dataset
    c_mkt['mm_count_temp'+m] = a_mkt.getrow(g_index).toarray()[0]
   
    # XXBM: EFFICIENCY IMPROVEMENT: sort obs per market and/or do the merge below only if the gamma for the previous job is different than the gamma for the current job
    # merge a vector of length J (total number of jobs) in which each element is the number of transitions between gamma_j (current job j's gamma) to gamma_j' (all other jobs' gammas) to the job dataset
    cjid['mm_count_temp'+m] = np.NaN
    cjid = cjid.drop(['mm_count_temp'+m], axis=1).reset_index().merge(c_mkt[['mm_count_temp'+m,mkt]], left_on=mkt, right_on=mkt).set_index('idx')
   
    # MAIN THING: predicted probability for a match between j and j' (see spreadsheet and/or overleaf)
        # spreadsheet: /Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions/
        # overleaf: https://www.overleaf.com/project/6400d14789d986d7ed453675
    # for each job j, we will have a vector of length j with the # of transitions expected from job j to all other jobs
    cjid['pred_prob'] = (cjid['mm_count_temp'+m] / D_insample) * cjid['djd'+m] * cjid['djd'+m][j]
   
    # CORRECTION TO DISTRITUBE SELF-EDGE PROBABILITIES
        # OUTSTANDING PROBLEM: This piece in the denominator increases exponentially for some market 2**(np.sum(cjid['gamma']==g)-1). However, python assigns very large numbers to zero, when it can't compute it.
        # CURRENT SOLUTION: using an if statement using the condition (2**(np.sum(cjid['gamma']==g)-1) == 0), and if that is true, hardcode the highest possible integer for
        # according to the formula we are using, we would predict a non-zero transition from a job to itself, which is not matched in the real job to job transition matrix ajid
        # the code below distributes the predicted #transitions from a job to itself roughly according to all other job degrees, forcing the self-transitions to be zero
    # distributing self edge probability
    term_denominator = 2**(np.sum(cjid[mkt]==g)-1)
    if term_denominator == 0:
        term_denominator = sys.maxsize
    cjid = cjid.sort_values('idx')   # sort the job dataset just to check calculations
    cjid['self_pred_prob_distributed'] = (cjid['degree'][j] + cjid['degree']) / (cjid['cardinality_'+mkt]) / (term_denominator) * cjid['pred_prob_diag_'+m]  * (cjid[mkt]==g)
    
    # finalizing the predicted probability after distributing the self edge probabilities
    cjid.loc[j,'pred_prob'] = 0
    pred = cjid['pred_prob'] #+ cjid['self_pred_prob_distributed']
   
    # Compute two vectors of length J with the L1 and L2 errors
        # compare the predicted flows from j to all other j' to the actual number of transitions in the job-to-job adjacency matrix ajid
    error1 = np.sum(np.abs((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    error2 = np.sum(np.square((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
    
    return([error1, error2])


###################################
### LOOP OVER ALL JOBS, COMPUTING THE TRANSITIONS FROM EACH JOB TO ALL OTHER JOBS, AND THE PREDICTION ERROR BASED ON ACTUAL VS PREDICTED
###################################

### TEMPORARY VALUE JUST TO TEST
J = 200
core = 3
total_cores = 8
#core = int(sys.argv[1])
#total_cores = int(sys.argv[2])


workload_size = J // total_cores

# for c in range(1,total_cores+1):
#     first_job = (c-1)*workload_size
#     last_job = c*workload_size-1
#     if c == total_cores:
#         last_job = J
#     print(str(first_job) + ' - ' + str(last_job) + ', core = ' + str(c))

first_job = (core-1)*workload_size
last_job = core*workload_size-1
if core == total_cores:
    last_job = J
print(str(first_job) + ' - ' + str(last_job) + ', core = ' + str(core))

# Obs: NO PARALLELIZATION IN THIS LOOP
results = []
start_time = datetime.now()  # get the start time
print_increment = 5

# Open the file in append mode
file_path = 'job_transitions_results/' 'core' + str(core) + '_multiple_mkts_log.txt'
file = open(file_path, 'w')
file.close()

for j in range(first_job,last_job+1):
    result = prediction_error(j,cjid, cmkts['o'], ajid, amkts['o'], D_insample,D_outsample,J, 'occ2Xmeso')
    results.append(result)

    # this would be to track time
    if j % print_increment == 0:
        # calculate the percentage progress and print it
        progress_percent = np.round((j-first_job) / (last_job-first_job) * 100,1)
        now = datetime.now()
        elapsed_time = now - start_time  # calculate the elapsed time
        # intermediate step saving outputs
        results_temp = pd.DataFrame(results)
        results_temp.to_csv('job_transitions_results/results_core' + str(core) + '_multiple_mkts.csv')    
        status = 'j = ' + str(j) + ' / ' + str(last_job) + ', ' + f"{progress_percent}% complete. Elapsed time: {elapsed_time}. Time: {now}\r\n"
        print(status)
        # Append the current step to the file
        file = open(file_path, 'a')
        file.write(status)
        file.close()

progress_percent = np.round((j-first_job) / (last_job-first_job) * 100,1)
status = 'j = ' + str(j) + ' / ' + str(last_job) + ', ' + f"{progress_percent}% complete. Elapsed time: {elapsed_time}. Time: {now}\r\n"
print(status)
file = open(file_path, 'a')
file.write(status)
file.close()


results_temp = pd.DataFrame(results)
results_temp.to_csv('results_core' + str(core) + '_multiple_mkts.csv')    
