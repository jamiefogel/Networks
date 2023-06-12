import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt

# DIRECTORY WHERE THE DATA IS
# Automatically chooses the folder between windows vs linux servers
if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')

# Data folder
os.chdir(homedir + '/labormkt_rafaelpereira/aug2022/dump/')
#os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions')


dfs = []

for core in range(1, 28):
    try:
            df = pd.read_csv('job_transitions_results/results_core' + str(core) + '_multiple_mkts.csv')    
            df['core'] = core
            dfs.append(df)
    except FileNotFoundError:
        print(f"Result file from core {core} not found. Skipping...")

combined_df = pd.concat(dfs, ignore_index=True)

combined_df 
np.round(combined_df.iloc[:,1:5].describe(),1)




for i in [1,2]:
    combined_df['diff_l'+str(i)] = combined_df['l'+str(i)+'_g'] - combined_df['l'+str(i)+'_o']

    print('Summary stats for difference between Gamma and Occ2XMeso prediction errors, L'+str(i)+' norm')
    combined_df['diff_l'+str(i)].describe() 


    # Drop the 10, 100,and 1000 largest and smallest obs to see if findings are skewed by outlier
    for c in [10, 100, 1000]:
        sorted_df = combined_df.sort_values('diff_l'+str(i))
        trimmed_df = sorted_df.iloc[c:-c]
        summary_stats = trimmed_df['diff_l'+str(i)].describe()    
        print('Summary stats for difference between Gamma and Occ2XMeso prediction errors, L'+str(i)+' norm. Trimming '+str(c)+' largest and smallest prediction errors')
        print(summary_stats)

    # Histogram of differences
    combined_df['diff_l'+str(i)].plot(kind='hist', bins=100)  # Adjust the number of bins as needed
    plt.xlabel('Difference (l'+str(i)+'_g - l'+str(i)+'_o)')
    plt.ylabel('Frequency')
    plt.title('Histogram of l'+str(i)+'_g - l'+str(i)+'_o')
    plt.show()

    
    # Repeat for 100 random 1% draws from the set of predictions to assess robustness to outliers and small sample size
    num_draws = 100
    # Calculate the mean differences for each subset
    mean_diffs = []
    for _ in range(num_draws):
        subset = combined_df.sample(frac=0.01, replace=False)  # Adjust the fraction as desired
        mean_diff = subset['l2_g'].mean() - subset['l2_o'].mean()
        mean_diffs.append(mean_diff)
    
    # Create a histogram of the mean differences
    plt.hist(mean_diffs, bins=100)  # Adjust the number of bins as needed
    plt.xlabel('Mean Difference (l'+str(i)+'_g - l'+str(i)+'_o)')
    plt.ylabel('Frequency')
    plt.title('Histogram of Mean Differences Across 100 Draws')
    plt.show()
    
    pd.DataFrame(mean_diffs).describe()
    pct_neg = (np.array(mean_diffs)<0).mean() *100
    print(str(pct_neg)+'% of the 100 draws yield a negative mean of l'+str(i)+'_g - l'+str(i)+'_o)')
