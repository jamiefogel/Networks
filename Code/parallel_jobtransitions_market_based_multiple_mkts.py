'''
Inputs: 
    - pred_flows_g_gamma.p        g->g adjacency matrix (1155x1155)
    - pred_flows_g_occ2Xmeso.p    o->o adjacency matrix (1508x1508) 
    - pred_flows_g_jid.p          j->j adjacency matrix (3456611x3456611)
    - pred_flows_gamma_cw.p
    - pred_flows_occ2Xmeso_cw.p
    - pred_flows_jid_cw.p

'''

from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
from scipy.sparse import coo_matrix
import scipy.sparse as sparse
import sys

# DIRECTORY WHERE THE DATA IS
# Automatically chooses the folder between windows vs linux servers
if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
elif os.name=='posix':
    import graph_tool.all as gt
    homedir = os.path.expanduser('~/labormkt')


root = homedir + '/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
from prediction_error_uni import prediction_error_uni
os.chdir(root)


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


# Loading the adjacency objects without the need of graphtool

d_gg_tilde = pickle.load(open('./Data/derived/predicting_flows/pred_flows_d_gg_tilde.p', "rb" ) )

ag_ins = pickle.load(open('./Data/derived/predicting_flows/pred_flows_ag_ins.p', 'rb'))
ao_ins = pickle.load(open('./Data/derived/predicting_flows/pred_flows_ao_ins.p', 'rb'))
ajid_oos = pickle.load(open('./Data/derived/predicting_flows/pred_flows_ajid_oos.p', 'rb'))

amkts = {'g': ag_ins.todense(), 'i':d_gg_tilde, 'o': ao_ins.todense()}


# Degree counts
#   - Loading important information to compute transition probabilities
#   - The iota-gamma ones borrow the same distribution as the gamma-to-gamma flows.
cmkts = {'g': pickle.load(open('./Data/derived/predicting_flows/pred_flows_gamma_degreecount_oos.p', 'rb')), 'i': pickle.load(open('./Data/derived/predicting_flows/pred_flows_gamma_degreecount_oos.p', 'rb')),  'o': pickle.load(open('./Data/derived/predicting_flows/pred_flows_occ2Xmeso_degreecount_oos.p', 'rb'))}
cmkts['i'] = cmkts['i'].rename(columns={'gamma':'iotagamma','gamma_degreecount':'iotagamma_degreecount'})

# Use the out-of-sample job-to-job degrees. For iota-gamma it will be the same degrees as the gamma; only the market-to-market transition probabilities change. 
cjid = pd.read_pickle('./Data/derived/predicting_flows/pred_flows_jid_degreecount_oos.p')
cjid['iotagamma'] = cjid['gamma']
cjid['iotagamma_degreecount'] = cjid['gamma_degreecount']

###################################
### DATA PREP
###################################

# Renaming index to avoid errors later
cjid = cjid.rename({'index':'idx'}, axis=1)


# Computes total number of transitions in-sample, out-of-sample and and total number of jobs
D_outsample = np.sum(ajid_oos)
D_insample = np.sum(amkts['g'])   # this should be the same as np.sum(amkts['o']) and  np.sum(amkts['i'])
J = ajid_oos.shape[0]

# Temporary variable
#cjid['mm_count_temp'] = np.NaN


# NOTE FOR THE BLOCK OF CODE BELOW:
# we are adding new columns to the degree count objects from swiftly getting info necessary to the probability computation. The idea is to have all relevant info in the same row
# The for loop below goes over all jobs, so we don't want to compute all of the info below for every job in the loop

# The lines above are j specific. The lines below prepare the data for each market:
mkts = ['gamma', 'iotagamma', 'occ2Xmeso']

for mkt in mkts:
    m = mkt[0]
    # Storing gamma to same gamma flows (i.e. internal flows)
    cmkts[m]['within_flow_' + m] = amkts[m].diagonal().reshape(-1, 1)
    # probability of choosing an edge connecting to j within its market: degree divided by cardinality of the market    
    cjid['djd' + m] = cjid['jid_degreecount'] / cjid[mkt + '_degreecount']
    # get the number of matches between the current gamma to all other gammas (assuming cg is sorted according to ag)
    # this quantity is necessary for the probability correction on overleaf (i.e. to DISTRIBUTE SELF EDGE PROBS TO OTHER EDGES)
    cjid = cjid.reset_index().merge(cmkts[m][['within_flow_'+m,mkt]], left_on=mkt, right_on=mkt).set_index('idx')
    # APPROACH 1 TO DISTRIBUTE SELF EDGE PROBS TO OTHER EDGES
    # getting the prediction for the diagonal (i.e. for a job to itself), which has to be distributed accros jobs
    cjid['pred_prob_diag_' + m] = (cjid['within_flow_'+m] / D_insample) * np.square(cjid['djd'+m])
    # accumulating the diagonal prob per gamma
    cjid['pred_prob_diag_acc_'+m] = cjid.loc[:,[mkt,'pred_prob_diag_'+m]].groupby(mkt).transform('sum')




###################################
### LOOP OVER ALL JOBS, COMPUTING THE TRANSITIONS FROM EACH JOB TO ALL OTHER JOBS, AND THE PREDICTION ERROR BASED ON ACTUAL VS PREDICTED
###################################

### TEMPORARY VALUE JUST TO TEST
#J = 108
#core = 3
#total_cores = 11


core = int(sys.argv[1])
total_cores = int(sys.argv[2])


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
header = str(first_job) + ' - ' + str(last_job) + ', core = ' + str(core) + '\r\n'
print(header)
print(' \r\n')

# Obs: NO PARALLELIZATION IN THIS LOOP
results = []
results_cols = ['j', 'l1_g', 'l2_g', 'l1_i', 'l2_i', 'l1_o', 'l2_o']

start_time = datetime.now()  # get the start time
print_increment = 200

# Open the file in append mode
file_path = 'job_transitions_results/core' + str(core) + '_multiple_mkts_log.txt'
file = open(file_path, 'w')
file.close()

file = open(file_path, 'a')
file.write(header)
file.write(' \r\n')
file.close()

result_temp = {}

for j in range(first_job,last_job+1):    
    
    for mkt in mkts:
        m = mkt[0]
        result_temp[m] = prediction_error_uni(j,cjid, cmkts[m], ajid_oos, amkts[m], D_insample,D_outsample,J, mkt)
        
    results.append([j] + result_temp['g'] + result_temp['i'] + result_temp['o'])

    # this would be to track time
    if (j-first_job) % print_increment == 0:
        # calculate the percentage progress and print it
        progress_percent = np.round((j-first_job) / (last_job-first_job) * 100,2)
        now = datetime.now()
        elapsed_time = now - start_time  # calculate the elapsed time
        # intermediate step saving outputs
        results_temp = pd.DataFrame(results, columns = results_cols)
        results_temp.to_csv('job_transitions_results/results_core' + str(core) + '_multiple_mkts.csv', index=False)    
        status = 'j = ' + str(j) + ' / ' + str(last_job) + ', ' + f"{progress_percent}% complete. Elapsed time: {elapsed_time}. Time: {now}\r\n"
        print(status)
        # Append the current step to the file
        file = open(file_path, 'a')
        file.write(status)
        file.close()

progress_percent = np.round((j-first_job) / (last_job-first_job) * 100,2)
now = datetime.now()
elapsed_time = now - start_time  # calculate the elapsed time
status = 'j = ' + str(j) + ' / ' + str(last_job) + ', ' + f"{progress_percent}% complete. Elapsed time: {elapsed_time}. Time: {now}\r\n"
print(status)
file = open(file_path, 'a')
file.write(status)
file.close()


results_temp = pd.DataFrame(results, columns = results_cols)
results_temp.to_csv('job_transitions_results/results_core' + str(core) + '_multiple_mkts.csv', index=False)    
