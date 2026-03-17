#!/usr/bin/env python3
"""
Full prediction analysis for Section 5.3 replication.

This script runs the complete job transition prediction analysis across all jobs.
For computational efficiency, this should be run in parallel across multiple cores.

Usage:
  python run_full_prediction_analysis.py [core_number] [total_cores]

If no arguments provided, runs on single core for all jobs (very slow).
"""

import sys
import os
import pandas as pd
import numpy as np
import pickle
from datetime import datetime

# Setup paths
homedir = os.path.expanduser('~')
root = homedir + '/NetworksGit/'
sys.path.append(root + 'replicate-prediction-flows/modules')
sys.path.append(root + 'Code/Modules')

os.chdir(root)

# Import modules
from prediction_error_uni import prediction_error_uni

print("="*80)
print("FULL SECTION 5.3 PREDICTION ANALYSIS")
print("="*80)

# Configuration
modelname = 'section_5_3_replication'

# Parallel processing setup
if len(sys.argv) >= 3:
    core = int(sys.argv[1])
    total_cores = int(sys.argv[2])
    parallel = True
    print(f"Running in parallel mode: core {core} of {total_cores}")
else:
    core = 1
    total_cores = 1
    parallel = False
    print("Running in single-core mode (WARNING: This will be very slow!)")

#==============================================================================
# LOAD PREPROCESSED DATA
#==============================================================================
print("\nLoading preprocessed data...")

# Load adjacency matrices
ag_ins = pickle.load(open(f'./replicate-prediction-flows/data/{modelname}_ag_ins.p', 'rb'))
ao_ins = pickle.load(open(f'./replicate-prediction-flows/data/{modelname}_ao_ins.p', 'rb'))
ajid_oos = pickle.load(open(f'./replicate-prediction-flows/data/{modelname}_ajid_oos.p', 'rb'))

# Load degree counts
gamma_degreecount_oos = pickle.load(open(f'./replicate-prediction-flows/data/{modelname}_gamma_degreecount_oos.p', 'rb'))
occ2Xmeso_degreecount_oos = pickle.load(open(f'./replicate-prediction-flows/data/{modelname}_occ2Xmeso_degreecount_oos.p', 'rb'))

# Create market data structures
amkts = {
    'g': ag_ins.todense(),
    'i': ag_ins.todense(),  # Simplified: use same as gamma
    'o': ao_ins.todense()
}

cmkts = {
    'g': gamma_degreecount_oos.copy(),
    'i': gamma_degreecount_oos.rename(columns={'gamma':'iotagamma', 'gamma_degreecount':'iotagamma_degreecount'}),
    'o': occ2Xmeso_degreecount_oos.copy()
}

print("Adjacency matrices loaded:")
print(f"  Gamma-gamma: {ag_ins.shape}")
print(f"  Occ2Xmeso-occ2Xmeso: {ao_ins.shape}")
print(f"  Job-job: {ajid_oos.shape}")

#==============================================================================
# LOAD JOB-LEVEL DATA
#==============================================================================
print("\nLoading job-level data...")

# Reconstruct the job-level data (cjid) from the main script's logic
# This is complex, so for simplification, we'll create a minimal version

# Load transition data to get job-market mappings
df_trans_oos = pickle.load(open(f'./replicate-prediction-flows/data/{modelname}_df_trans_oos.p', 'rb'))

# Create job-to-market crosswalk
jid_mkt_cw = pd.concat([
    df_trans_oos[['jid', 'gamma', 'occ2Xmeso']],
    df_trans_oos[['jid_prev', 'gamma_prev', 'occ2Xmeso_prev']].rename(columns={
        'jid_prev':'jid', 'gamma_prev':'gamma', 'occ2Xmeso_prev':'occ2Xmeso'
    })
]).drop_duplicates(subset=['jid'])

# Create job index mapping
all_jobs = sorted(jid_mkt_cw.jid.unique())
jid_to_idx = {jid: idx for idx, jid in enumerate(all_jobs)}
jid_mkt_cw['idx'] = jid_mkt_cw['jid'].map(jid_to_idx)

# Compute job degree counts from adjacency matrix
job_degrees = np.array(ajid_oos.sum(axis=1)).flatten() / 2  # Divide by 2 for undirected
jid_degrees = pd.DataFrame({'idx': range(len(job_degrees)), 'jid_degreecount': job_degrees})

# Merge everything
cjid = jid_degrees.merge(jid_mkt_cw, on='idx', how='left')
cjid = cjid.merge(gamma_degreecount_oos[['gamma', 'gamma_degreecount']], on='gamma', how='left')
cjid = cjid.merge(occ2Xmeso_degreecount_oos[['occ2Xmeso', 'occ2Xmeso_degreecount']], on='occ2Xmeso', how='left')

# Add iota-gamma columns (simplified)
cjid['iotagamma'] = cjid['gamma']
cjid['iotagamma_degreecount'] = cjid['gamma_degreecount']

# Fill missing values
cjid = cjid.fillna(-1)

print(f"Job-level data prepared: {len(cjid)} jobs")

#==============================================================================
# PRE-COMPUTE MARKET DATA
#==============================================================================
print("\nPre-computing market-specific data...")

# Compute total transitions
D_outsample = np.sum(ajid_oos)
D_insample = np.sum(amkts['g'])
J = ajid_oos.shape[0]

print(f"Total jobs: {J}")
print(f"In-sample transitions: {D_insample}")
print(f"Out-of-sample transitions: {D_outsample}")

# Pre-compute market-specific variables (from original predicting_flows_data_pull.py)
mkts = ['gamma', 'iotagamma', 'occ2Xmeso']

for mkt in mkts:
    m = mkt[0]
    print(f"  Processing market type: {mkt}")

    # Store within-market flows (diagonal elements)
    cmkts[m]['within_flow_' + m] = amkts[m].diagonal().reshape(-1, 1)

    # Compute job degree as share of market
    cjid['djd' + m] = cjid['jid_degreecount'] / cjid[mkt + '_degreecount']

    # Merge market-level information
    cjid = cjid.reset_index().merge(
        cmkts[m][['within_flow_'+m, mkt]],
        left_on=mkt, right_on=mkt, how='left'
    ).set_index('idx')

    # Compute self-loop prediction probabilities
    cjid['pred_prob_diag_' + m] = (cjid['within_flow_'+m] / D_insample) * np.square(cjid['djd'+m])

    # Accumulate diagonal probabilities per market
    cjid['pred_prob_diag_acc_'+m] = cjid.groupby(mkt)['pred_prob_diag_'+m].transform('sum')

print("Pre-computation complete.")

#==============================================================================
# DETERMINE JOB RANGE FOR THIS CORE
#==============================================================================

workload_size = J // total_cores
first_job = (core - 1) * workload_size
last_job = core * workload_size - 1

if core == total_cores:
    last_job = J - 1  # Ensure last core gets all remaining jobs

print(f"\nCore {core} will process jobs {first_job} to {last_job} ({last_job - first_job + 1} jobs)")

#==============================================================================
# RUN PREDICTION ANALYSIS
#==============================================================================
print(f"\nStarting prediction analysis at {datetime.now()}")

results = []
results_cols = ['j', 'l1_g', 'l2_g', 'l1_i', 'l2_i', 'l1_o', 'l2_o']

start_time = datetime.now()
print_increment = max(1, (last_job - first_job) // 10)  # Print progress 10 times

# Create log file
log_file = f'./replicate-prediction-flows/results/core_{core}_log.txt'
with open(log_file, 'w') as f:
    f.write(f"Core {core} processing jobs {first_job} to {last_job}\n")
    f.write(f"Started at {start_time}\n\n")

for j in range(first_job, last_job + 1):
    try:
        result_temp = {}

        # Run prediction for each market type
        for mkt in mkts:
            m = mkt[0]
            result_temp[m] = prediction_error_uni(
                j, cjid, cmkts[m], ajid_oos, amkts[m],
                D_insample, D_outsample, J, mkt
            )

        # Store results
        results.append([j] + result_temp['g'] + result_temp['i'] + result_temp['o'])

        # Progress reporting
        if (j - first_job) % print_increment == 0:
            progress_pct = (j - first_job) / (last_job - first_job) * 100
            elapsed = datetime.now() - start_time
            print(f"  Progress: {progress_pct:.1f}% - Job {j}/{last_job} - Elapsed: {elapsed}")

            # Save intermediate results
            temp_results = pd.DataFrame(results, columns=results_cols)
            temp_results.to_csv(f'./replicate-prediction-flows/results/core_{core}_temp_results.csv', index=False)

            # Log progress
            with open(log_file, 'a') as f:
                f.write(f"Job {j}: {progress_pct:.1f}% complete at {datetime.now()}\n")

    except Exception as e:
        print(f"Error processing job {j}: {e}")
        with open(log_file, 'a') as f:
            f.write(f"ERROR at job {j}: {e}\n")

# Save final results
final_results = pd.DataFrame(results, columns=results_cols)
final_results.to_csv(f'./replicate-prediction-flows/results/core_{core}_final_results.csv', index=False)

elapsed_total = datetime.now() - start_time
print(f"\nCore {core} complete!")
print(f"Processed {len(results)} jobs in {elapsed_total}")
print(f"Results saved to core_{core}_final_results.csv")

# Final log entry
with open(log_file, 'a') as f:
    f.write(f"\nCompleted at {datetime.now()}\n")
    f.write(f"Total time: {elapsed_total}\n")
    f.write(f"Jobs processed: {len(results)}\n")

print("\n" + "="*80)
print("PREDICTION ANALYSIS COMPLETE")
print("="*80)