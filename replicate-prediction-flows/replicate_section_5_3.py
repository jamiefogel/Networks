#!/usr/bin/env python3
"""
Streamlined replication of Section 5.3: Predicting out-of-sample job-to-job flows

This script contains only the essential components needed to replicate the
job transition prediction analysis from Section 5.3 of the paper.

Pipeline:
1. Pull and process raw RAIS data (2009-2013)
2. Run SBM to identify network-based markets (gamma)
3. Create job transition matrices
4. Run prediction analysis comparing network vs traditional classifications
5. Generate results
"""

import os
import sys
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import getpass

# Setup paths
homedir = os.path.expanduser('~')
if getpass.getuser() == 'p13861161':
    root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
elif getpass.getuser() == 'jfogel':
    root = homedir + '/NetworksGit/'

# Add modules to path
sys.path.append(root + 'replicate-prediction-flows/modules')
sys.path.append(root + 'Code/Modules')  # For any modules we reuse as-is

os.chdir(root)

# Import modules
from pull_one_year_simple import pull_one_year_simple
from bisbm_simple import bisbm_simple
from create_df_trans import create_df_trans  # Reuse existing
from create_unipartite_adjacency_and_degrees import create_unipartite_adjacency_and_degrees  # Reuse existing
from prediction_error_uni import prediction_error_uni  # Reuse existing

print("="*80)
print("REPLICATING SECTION 5.3: PREDICTING OUT-OF-SAMPLE JOB-TO-JOB FLOWS")
print("="*80)

# Configuration
modelname = 'section_5_3_replication'
state_codes = [31, 33, 35]  # Minas Gerais, Espírito Santo, Rio de Janeiro
rais_filename_stub = '~/rais/RAIS/csv/brasil'
ins_years = [2009, 2010, 2011]  # In-sample years for estimating transition probabilities
oos_years = [2012, 2013]        # Out-of-sample years for prediction validation

# Create output directories
os.makedirs('./replicate-prediction-flows/data', exist_ok=True)
os.makedirs('./replicate-prediction-flows/results', exist_ok=True)

print(f"Model name: {modelname}")
print(f"States: {state_codes}")
print(f"In-sample years: {ins_years}")
print(f"Out-of-sample years: {oos_years}")

#==============================================================================
# STEP 1: PULL AND PROCESS RAW DATA
#==============================================================================
print("\n" + "="*50)
print("STEP 1: PULLING RAW DATA")
print("="*50)

raw_dict_ins = {}
raw_dict_oos = {}

for year in ins_years + oos_years:
    print(f"\nProcessing year {year}...")
    raw = pull_one_year_simple(
        year=year,
        state_codes=state_codes,
        age_lower=25,
        age_upper=55,
        filename=rais_filename_stub + str(year) + '.csv'
    )

    # Save individual year file
    raw.to_pickle(f'./replicate-prediction-flows/data/raw_{modelname}_{year}.p')

    # Store in appropriate dictionary
    if year in ins_years:
        raw_dict_ins[year] = raw
    else:
        raw_dict_oos[year] = raw

    print(f"Year {year}: {len(raw)} worker-job observations")

# Save combined dictionaries
pickle.dump(raw_dict_ins, open(f'./replicate-prediction-flows/data/{modelname}_raw_ins.p', 'wb'))
pickle.dump(raw_dict_oos, open(f'./replicate-prediction-flows/data/{modelname}_raw_oos.p', 'wb'))

print(f"\nRaw data processing complete. Files saved to replicate-prediction-flows/data/")

#==============================================================================
# STEP 2: RUN SBM TO IDENTIFY NETWORK-BASED MARKETS
#==============================================================================
print("\n" + "="*50)
print("STEP 2: RUNNING SBM ESTIMATION")
print("="*50)

# Create bipartite edgelist for SBM (using in-sample years only)
print("Creating bipartite edgelist for SBM...")
edgelist_data = []
for year in ins_years:
    year_data = raw_dict_ins[year][['wid', 'jid']].drop_duplicates()
    edgelist_data.append(year_data)

bipartite_edgelist = pd.concat(edgelist_data).drop_duplicates()
bipartite_edgelist.to_pickle(f'./replicate-prediction-flows/data/bipartite_edgelist_{modelname}.p')

print(f"Bipartite edgelist created: {len(bipartite_edgelist)} unique worker-job pairs")

# Run SBM
print("Running SBM estimation...")
sbm = bisbm_simple()
sbm.create_graph(f'./replicate-prediction-flows/data/bipartite_edgelist_{modelname}.p', min_workers_per_job=5)
sbm.fit(n_init=1)

# Export block assignments
sbm.export_blocks(
    joutput=f'./replicate-prediction-flows/data/{modelname}_jblocks.csv',
    woutput=f'./replicate-prediction-flows/data/{modelname}_wblocks.csv'
)

# Save SBM object
pickle.dump(sbm, open(f'./replicate-prediction-flows/data/{modelname}_sbm.p', 'wb'), protocol=4)

print(f"SBM estimation complete:")
print(f"  - {sbm.num_jobs} unique jobs")
print(f"  - {sbm.num_workers} unique workers")
print(f"  - Job markets (gamma): {sbm.num_job_blocks[0] if sbm.num_job_blocks else 'TBD'}")
print(f"  - Worker types (iota): {sbm.num_worker_blocks[0] if sbm.num_worker_blocks else 'TBD'}")

#==============================================================================
# STEP 3: MERGE CLASSIFICATIONS AND CREATE TRANSITION DATA
#==============================================================================
print("\n" + "="*50)
print("STEP 3: CREATING TRANSITION DATA")
print("="*50)

# Load SBM results
gammas = pd.read_csv(f'./replicate-prediction-flows/data/{modelname}_jblocks.csv',
                    usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
iotas = pd.read_csv(f'./replicate-prediction-flows/data/{modelname}_wblocks.csv',
                   usecols=['wid','worker_blocks_level_0'], dtype={'wid': object}).rename(columns={'worker_blocks_level_0':'iota'})

# Fill missing classifications
gammas['gamma'] = gammas.gamma.fillna(-1)
iotas['iota'] = iotas.iota.fillna(-1)

print(f"Gamma markets: {len(gammas[gammas.gamma != -1])} jobs classified")
print(f"Iota worker types: {len(iotas[iotas.iota != -1])} workers classified")

# Load region codes for occ2Xmeso classification
region_codes = pd.read_csv('./Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})

# Add classifications to raw data
print("Merging classifications onto raw data...")

for year_dict, period_name in [(raw_dict_ins, 'ins'), (raw_dict_oos, 'oos')]:
    for year, raw in year_dict.items():
        print(f"  Processing {year} ({period_name})")
        # Add region codes
        raw = raw.merge(muni_meso_cw, how='left', on='codemun')
        # Create occ2Xmeso classification
        raw['occ2Xmeso'] = raw.cbo2002.str[0:2] + '_' + raw['code_meso'].astype('str')
        # Add SBM classifications
        raw = raw.merge(gammas, on='jid', how='left')
        raw = raw.merge(iotas, on='wid', how='left')
        # Fill missing values
        raw['gamma'] = raw.gamma.fillna(-1)
        raw['iota'] = raw.iota.fillna(-1)
        # Update in dictionary
        year_dict[year] = raw

# Create transition dataframes
print("Creating job transition dataframes...")
df_trans_ins = create_df_trans([raw_dict_ins[year] for year in ins_years])
df_trans_oos = create_df_trans([raw_dict_oos[year] for year in oos_years])

# Save transition data
df_trans_ins.to_pickle(f'./replicate-prediction-flows/data/{modelname}_df_trans_ins.p')
df_trans_oos.to_pickle(f'./replicate-prediction-flows/data/{modelname}_df_trans_oos.p')

print(f"Transition data created:")
print(f"  - In-sample transitions: {len(df_trans_ins)}")
print(f"  - Out-of-sample transitions: {len(df_trans_oos)}")

#==============================================================================
# STEP 4: CREATE ADJACENCY MATRICES AND DEGREE COUNTS
#==============================================================================
print("\n" + "="*50)
print("STEP 4: CREATING ADJACENCY MATRICES")
print("="*50)

# Create market-to-market adjacency matrices (in-sample)
print("Creating market-to-market adjacency matrices (in-sample)...")
[ag_ins, gamma_degreecount_ins] = create_unipartite_adjacency_and_degrees('gamma', df_trans_ins)
[ao_ins, occ2Xmeso_degreecount_ins] = create_unipartite_adjacency_and_degrees('occ2Xmeso', df_trans_ins)

print(f"Gamma markets adjacency matrix: {ag_ins.shape}")
print(f"Occ2Xmeso adjacency matrix: {ao_ins.shape}")

# Create job-to-job adjacency matrix (out-of-sample)
print("Creating job-to-job adjacency matrix (out-of-sample)...")
[ajid_oos, jid_degreecount_oos] = create_unipartite_adjacency_and_degrees('jid', df_trans_oos)

print(f"Job-to-job adjacency matrix: {ajid_oos.shape}")

# Create out-of-sample degree counts for markets
[_, gamma_degreecount_oos] = create_unipartite_adjacency_and_degrees('gamma', df_trans_oos)
[_, occ2Xmeso_degreecount_oos] = create_unipartite_adjacency_and_degrees('occ2Xmeso', df_trans_oos)

# Clean up degree count dataframes
gamma_degreecount_ins.drop(columns='index', inplace=True, errors='ignore')
occ2Xmeso_degreecount_ins.drop(columns='index', inplace=True, errors='ignore')
gamma_degreecount_oos.drop(columns='index', inplace=True, errors='ignore')
occ2Xmeso_degreecount_oos.drop(columns='index', inplace=True, errors='ignore')

# Handle cases where small markets appear in-sample but not out-of-sample
occ2Xmeso_degreecount_oos = occ2Xmeso_degreecount_oos.merge(
    occ2Xmeso_degreecount_ins['occ2Xmeso'],
    on='occ2Xmeso', how='outer', validate='1:1'
).fillna(0)

# Save adjacency matrices and degree counts
pickle.dump(ag_ins, open(f'./replicate-prediction-flows/data/{modelname}_ag_ins.p', 'wb'))
pickle.dump(ao_ins, open(f'./replicate-prediction-flows/data/{modelname}_ao_ins.p', 'wb'))
pickle.dump(ajid_oos, open(f'./replicate-prediction-flows/data/{modelname}_ajid_oos.p', 'wb'))
pickle.dump(gamma_degreecount_oos, open(f'./replicate-prediction-flows/data/{modelname}_gamma_degreecount_oos.p', 'wb'))
pickle.dump(occ2Xmeso_degreecount_oos, open(f'./replicate-prediction-flows/data/{modelname}_occ2Xmeso_degreecount_oos.p', 'wb'))

print("Adjacency matrices and degree counts saved.")

#==============================================================================
# STEP 5: PREPARE DATA FOR PREDICTION ANALYSIS
#==============================================================================
print("\n" + "="*50)
print("STEP 5: PREPARING PREDICTION DATA")
print("="*50)

# Create job-to-market crosswalks
jid_mkt_cw_ins = pd.concat([
    df_trans_ins[['jid', 'gamma', 'occ2Xmeso']],
    df_trans_ins[['jid_prev', 'gamma_prev', 'occ2Xmeso_prev']].rename(columns={
        'jid_prev':'jid', 'gamma_prev':'gamma', 'occ2Xmeso_prev':'occ2Xmeso'
    })
]).drop_duplicates(subset=['jid'])

jid_mkt_cw_oos = pd.concat([
    df_trans_oos[['jid', 'gamma', 'occ2Xmeso']],
    df_trans_oos[['jid_prev', 'gamma_prev', 'occ2Xmeso_prev']].rename(columns={
        'jid_prev':'jid', 'gamma_prev':'gamma', 'occ2Xmeso_prev':'occ2Xmeso'
    })
]).drop_duplicates(subset=['jid'])

# Merge market information onto job degree counts
jid_degreecount_oos = jid_degreecount_oos.merge(jid_mkt_cw_oos, on='jid', how='outer', validate='1:1')
jid_degreecount_oos = jid_degreecount_oos.merge(
    gamma_degreecount_oos[['gamma', 'gamma_degreecount']],
    on='gamma', how='left', validate='m:1'
)
jid_degreecount_oos = jid_degreecount_oos.merge(
    occ2Xmeso_degreecount_oos[['occ2Xmeso', 'occ2Xmeso_degreecount']],
    on='occ2Xmeso', how='left', validate='m:1'
)
jid_degreecount_oos = jid_degreecount_oos.sort_values(by='index').rename({'index':'idx'}, axis=1)

# Create market data structures for prediction
amkts = {
    'g': ag_ins.todense(),
    'o': ao_ins.todense()
}

cmkts = {
    'g': gamma_degreecount_oos.copy(),
    'o': occ2Xmeso_degreecount_oos.copy()
}

# For iota-gamma: reuse gamma structure (simplified - no MCMC bipartite matrix)
amkts['i'] = amkts['g']  # Same transition structure
cmkts['i'] = cmkts['g'].rename(columns={'gamma':'iotagamma', 'gamma_degreecount':'iotagamma_degreecount'})
jid_degreecount_oos['iotagamma'] = jid_degreecount_oos['gamma']
jid_degreecount_oos['iotagamma_degreecount'] = jid_degreecount_oos['gamma_degreecount']

# Compute total transitions
D_outsample = np.sum(ajid_oos)
D_insample = np.sum(amkts['g'])
J = ajid_oos.shape[0]

print(f"Data preparation complete:")
print(f"  - Total jobs: {J}")
print(f"  - In-sample transitions: {D_insample}")
print(f"  - Out-of-sample transitions: {D_outsample}")

#==============================================================================
# STEP 6: RUN PREDICTION ANALYSIS
#==============================================================================
print("\n" + "="*50)
print("STEP 6: RUNNING PREDICTION ANALYSIS")
print("="*50)

# Prepare data for prediction function
cjid = jid_degreecount_oos.copy()
mkts = ['gamma', 'iotagamma', 'occ2Xmeso']

# Pre-compute market-specific data (from original code)
for mkt in mkts:
    m = mkt[0]
    # Store within-market flows (diagonal elements)
    cmkts[m]['within_flow_' + m] = amkts[m].diagonal().reshape(-1, 1)
    # Compute job degree as share of market
    cjid['djd' + m] = cjid['jid_degreecount'] / cjid[mkt + '_degreecount']
    # Merge market-level information
    cjid = cjid.reset_index().merge(
        cmkts[m][['within_flow_'+m, mkt]],
        left_on=mkt, right_on=mkt
    ).set_index('idx')
    # Compute self-loop prediction probabilities
    cjid['pred_prob_diag_' + m] = (cjid['within_flow_'+m] / D_insample) * np.square(cjid['djd'+m])
    # Accumulate diagonal probabilities per market
    cjid['pred_prob_diag_acc_'+m] = cjid.loc[:,[mkt,'pred_prob_diag_'+m]].groupby(mkt).transform('sum')

print("Pre-computation complete. Running predictions...")

# Run simplified prediction analysis (sample of jobs for demonstration)
# For full replication, you would run this across all jobs in parallel
sample_jobs = min(1000, J)  # Sample first 1000 jobs for demonstration
results = []
results_cols = ['j', 'l1_g', 'l2_g', 'l1_i', 'l2_i', 'l1_o', 'l2_o']

print(f"Running predictions for {sample_jobs} jobs (sample)...")

for j in range(sample_jobs):
    if j % 100 == 0:
        print(f"  Progress: {j}/{sample_jobs}")

    result_temp = {}
    for mkt in mkts:
        m = mkt[0]
        result_temp[m] = prediction_error_uni(
            j, cjid, cmkts[m], ajid_oos, amkts[m],
            D_insample, D_outsample, J, mkt
        )

    results.append([j] + result_temp['g'] + result_temp['i'] + result_temp['o'])

# Convert results to DataFrame
results_df = pd.DataFrame(results, columns=results_cols)

# Save results
results_df.to_csv(f'./replicate-prediction-flows/results/{modelname}_sample_results.csv', index=False)

print(f"\nPrediction analysis complete. Sample results saved.")

#==============================================================================
# STEP 7: COMPUTE SUMMARY STATISTICS
#==============================================================================
print("\n" + "="*50)
print("STEP 7: SUMMARY STATISTICS")
print("="*50)

# Compute summary statistics
summary_stats = results_df.iloc[:,1:7].describe()
print("Summary statistics for prediction errors:")
print(summary_stats.round(2))

# Compare gamma vs occ2Xmeso performance
for norm in [1, 2]:
    results_df[f'diff_l{norm}'] = results_df[f'l{norm}_g'] - results_df[f'l{norm}_o']
    diff_stats = results_df[f'diff_l{norm}'].describe()
    print(f"\nL{norm} norm: Gamma vs Occ2Xmeso differences")
    print(f"  Mean difference: {diff_stats['mean']:.3f}")
    print(f"  Std difference: {diff_stats['std']:.3f}")
    print(f"  % of cases where gamma < occ2Xmeso: {(results_df[f'diff_l{norm}'] < 0).mean()*100:.1f}%")

# Save final results
final_results = {
    'summary_stats': summary_stats,
    'sample_size': sample_jobs,
    'total_jobs': J,
    'in_sample_transitions': D_insample,
    'out_sample_transitions': D_outsample,
    'results_df': results_df
}

pickle.dump(final_results, open(f'./replicate-prediction-flows/results/{modelname}_final_results.p', 'wb'))

print("\n" + "="*80)
print("SECTION 5.3 REPLICATION COMPLETE")
print("="*80)
print(f"Results saved to: ./replicate-prediction-flows/results/")
print(f"Data files saved to: ./replicate-prediction-flows/data/")
print(f"\nThis was a sample run with {sample_jobs} jobs.")
print(f"For full replication, run the prediction loop across all {J} jobs using parallel processing.")