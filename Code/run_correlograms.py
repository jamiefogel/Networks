

for idx in classification_list :
    # using wtype_var and jtype_Var instead of worker_type_var and job_type_var so we don't reset the master versions set in do_all.py
    wtype_var = idx[0]
    jtype_var    = idx[1]
    print(wtype_var, jtype_var)
    mle_data_sums_corr = pickle.load(open(root + 'Data/derived/mle_data_sums/' + filename_stub + '_mle_data_sums_'+wtype_var +'_'+jtype_var+'_level_0.p', "rb"), encoding='bytes')
    psi_hat = pickle.load(open(root+'Data/derived/MLE_estimates/' +filename_stub + '_psi_normalized_'+wtype_var +'_'+jtype_var+'_level_0_eta_2.p', "rb"), encoding='bytes')['psi_hat']
    # Merge on mean wages by worker type and sort
    psi_hat_merged = torch.cat((torch.reshape(mle_data_sums_corr['mean_wage_i'],(mle_data_sums_corr['mean_wage_i'].shape[0],1)), psi_hat),dim=1)
    psi_hat_sorted = psi_hat_merged[psi_hat_merged[:, 0].sort()[1]][:,1:]
    corr_name_stub = 'correlograms_' + wtype_var + '_' + jtype_var
    hist_name_stub = 'correlograms_hist_' + wtype_var + '_' + jtype_var
    correlogram(psi_hat,        figuredir+'correlograms/'+corr_name_stub + '.png'                , figuredir+'correlograms/'+hist_name_stub + '.png')
    correlogram(psi_hat_sorted, figuredir+'correlograms/'+corr_name_stub + '_sorted.png'         , figuredir+'correlograms/'+hist_name_stub + '_sorted.png')
    correlogram(psi_hat_sorted, figuredir+'correlograms/'+corr_name_stub + '_sorted_weighted.png', figuredir+'correlograms/'+hist_name_stub + '_sorted_weighted.png', p_ig = mle_data_sums_corr['p_ig_actual'])
    
  
    
####################################################################################
# Extreme benchmarks

# 2 clusters
means = np.ones(50)
diagonal    = np.random.uniform(low=.5, high=2, size=(25,25))
offdiagonal = np.random.uniform(low=-1,  high=-.5,   size=(25,25))
cov = np.vstack((np.hstack((diagonal,offdiagonal)), np.hstack((np.transpose(offdiagonal),diagonal))))
psi_2_clusters = torch.tensor(np.transpose(np.random.multivariate_normal(means,cov, (50))) )
corr_name_stub = 'correlograms_benchmark_2_clusters'
hist_name_stub = 'correlograms_hist_benchmark_2_clusters'
correlogram(psi_2_clusters,        figuredir+'correlograms/'+corr_name_stub + '.png'                , figuredir+'correlograms/'+hist_name_stub + '.png')

# Highly correlated
psi_1_cluster =  torch.tensor(np.transpose(np.random.multivariate_normal(np.ones(50), np.random.uniform(low=10, high=10.5, size=(50,50)), (200))) )
corr_name_stub = 'correlograms_benchmark_1_cluster'
hist_name_stub = 'correlograms_hist_benchmark_1_cluster'
correlogram(psi_1_cluster,        figuredir+'correlograms/'+corr_name_stub + '.png'                , figuredir+'correlograms/'+hist_name_stub + '.png')

# Uncorrelated
psi_specific_skills = torch.tensor(np.diag(np.random.uniform(low=.5, high=2, size=(50))))
corr_name_stub = 'correlograms_benchmark_specific_skills'
hist_name_stub = 'correlograms_hist_benchmark_specific_skills'
correlogram(psi_specific_skills,        figuredir+'correlograms/'+corr_name_stub + '.png'                , figuredir+'correlograms/'+hist_name_stub + '.png')

