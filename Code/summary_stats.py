import graph_tool.all as gt
import matplotlib.pyplot as plt
import seaborn as sns
import csv

model = pickle.load(open('./Data/derived/sbm_output/model_'+modelname+'.p', "rb" ) )

sumstats_dict = {}

print(model.num_workers, 'unique workers,',model.num_jobs, 'unique jobs, and', model.num_edges, 'edges in graph after restricting to at least 10 workers per job' )

sumstats_dict['num_unique_workers'] = model.num_workers
sumstats_dict['num_unique_jobs'] = model.num_jobs
sumstats_dict['num_unique_edges'] = model.num_edges



#############################################
# Degree distribution histograms
#############################################

degrees = model.g.get_out_degrees(model.g.get_vertices())
worker_degs = degrees[model.g.vp.kind.a==1]
job_degs    = degrees[model.g.vp.kind.a==0]

pickle.dump([worker_degs,job_degs], open(root + '/Data/derived/distributions/degree_distribution.p', "wb"))
[worker_degs, job_degs] = pickle.load(open(root + '/Data/derived/distributions/degree_distribution.p' , 'rb'))

sumstats_dict['average_worker_degree'] = worker_degs.mean()
sumstats_dict['average_job_degree'] = job_degs.mean() 
sumstats_dict['Fraction of workers with more than one job'] = (worker_degs>1).mean()

def plot_loghist(x, bins, savefile):
    hist, bins = np.histogram(x, bins=bins)
    logbins = np.logspace(np.log10(bins[0]),np.log10(bins[-1]),len(bins))
    fig, ax = plt.subplots(figsize=(5.76,4.8))
    ax.hist(x, bins=logbins)
    plt.xscale('log')
    plt.yscale('log')
    plt.show()    
    ax.figure.savefig(savefile , dpi=300, bbox_inches="tight")

# Decided it was easier to just hardcode the figures, especially because I only want log-log for jobs
#plot_loghist(worker_degs, 20, figuredir +'worker_degree_distribution_hist.png')
#plot_loghist(job_degs[job_degs>=5], 50, figuredir +'job_degree_distribution_hist.png')


fig, ax = plt.subplots(figsize=(5.76,4.8))
ax.hist(worker_degs, density=False, bins=20)
plt.yscale('log')
ax.set_xlabel('Number of Matches Per Worker')
ax.set_ylabel('Frequency (Log Scale)')
plt.show()    
ax.figure.savefig(figuredir + 'summary_stats/worker_degree_distribution_hist.png', dpi=300, bbox_inches="tight")


hist, bins = np.histogram(job_degs[job_degs>=10], bins=50)
logbins = np.logspace(np.log10(bins[0]),np.log10(bins[-1]),len(bins))
fig, ax = plt.subplots(figsize=(5.76,4.8))
ax.hist(job_degs[job_degs>=10], bins=logbins)
plt.xscale('log')
plt.yscale('log')
ax.set_xlabel('Number of Matches Per Job (Log Scale)')
ax.set_ylabel('Frequency (Log Scale)')
plt.show()    
ax.figure.savefig(figuredir + 'summary_stats/job_degree_distribution_hist.png' , dpi=300, bbox_inches="tight")



########################################################
# Distribution  of iota and gamma sizes histograms
########################################################


workers = model.edgelist_w_blocks[['wid','worker_blocks_level_0']].drop_duplicates()
iota_sizes = workers.groupby(['worker_blocks_level_0']).size()

jobs = model.edgelist_w_blocks[['jid','job_blocks_level_0']].drop_duplicates()
gamma_sizes = jobs.groupby(['job_blocks_level_0']).size()

pickle.dump([iota_sizes,gamma_sizes], open(root + '/Data/derived/distributions/iota_gamma_sizes.p', "wb"))

[iota_sizes, gamma_sizes] = pickle.load(open(root + '/Data/derived/distributions/iota_gamma_sizes.p', 'rb'))

fig, ax = plt.subplots(figsize=(5.76,4.8))
ax.hist(iota_sizes, density=False, bins=50)
#plt.yscale('log')
plt.show()    
ax.figure.savefig(figuredir + 'summary_stats/iota_size_distribution.png', dpi=300, bbox_inches="tight")


fig, ax = plt.subplots(figsize=(5.76,4.8))
ax.hist(gamma_sizes, density=False, bins=50)
#plt.yscale('log')
plt.show()    
ax.figure.savefig(figuredir + 'summary_stats/gamma_size_distribution.png', dpi=300, bbox_inches="tight")


        
iota_sizes_by_worker = iota_sizes.loc[iota_sizes.index.repeat(iota_sizes)]
iota_sizes_by_worker.mean()
iota_sizes_by_worker.median()
sumstats_dict['iota_sizes_by_worker_mean']   = iota_sizes_by_worker.mean()  
sumstats_dict['iota_sizes_by_worker_median'] = iota_sizes_by_worker.median()
              
gamma_sizes_by_job = gamma_sizes.loc[gamma_sizes.index.repeat(gamma_sizes)]
gamma_sizes_by_job.mean()
gamma_sizes_by_job.median()
sumstats_dict['gamma_sizes_by_job_mean']   = gamma_sizes_by_job.mean()  
sumstats_dict['gamma_sizes_by_job_median'] = gamma_sizes_by_job.median()

# Confirming that all workers and jobs are assigned an iota/gamma
model.edgelist_w_blocks.worker_blocks_level_0.isnull().sum()
model.edgelist_w_blocks.job_blocks_level_0.isnull().sum()   


# Not all vertices belong to the giant component but 99% do
g_comp = gt.label_largest_component(model.g)
frac_in_giant_component = g_comp.a.mean()
sumstats_dict['frac_in_giant_component'] = frac_in_giant_component


# It seems like nodes not in the giant component are kind of just randomly assigned but it doesn't matter since 99% of nodes are in the giant component
temp = pd.DataFrame({'block':model.state.project_level(0).get_blocks().a,'g_comp':gt.label_largest_component(model.g),'worker_node':model.g.vp.kind.a})
temp.loc[temp.g_comp==0].block.value_counts()



##############################################################################
# To what extent are moves between gammas symmetric? Our model predicts that they are
#  - This code is copied from what was previously trans_mat_symmetry_analysis.py, which in turn has also been called misc_analysis.py and job_ladder_analysis.py
###############################################################################

df_trans = pd.read_pickle(root + 'Data/derived/predicting_flows/' + modelname + '_pred_flows_df_trans_ins.p')
gamma_trans_mat = pd.crosstab(df_trans['gamma_prev'], df_trans['gamma'])

a = np.log(np.array(gamma_trans_mat))
a[a==np.NINF] = -1

sns.heatmap(a)

np.triu(gamma_trans_mat)


a[np.triu_indices(a.shape[0])]


b = np.array(gamma_trans_mat)


# Correlations between vectorized upper diagonal and lower diagonals of gamma transition matrix
upper = b[np.triu_indices(b.shape[0])]
lower = np.transpose(b)[np.triu_indices(b.shape[0])]


upper_nodiag = b[np.triu_indices(b.shape[0],k=1)]
lower_nodiag = np.transpose(b)[np.triu_indices(b.shape[0],k=1)]

print("Correlation between upper and lower triangular matrices: ", np.corrcoef(upper,lower)[0,1])
print("Correlation between upper and lower triangular matrices (excluding diagonal): ", np.corrcoef(upper_nodiag,lower_nodiag)[0,1])

sumstats_dict['Correlation between upper and lower triangular matrices'] = np.corrcoef(upper,lower)[0,1]
sumstats_dict['Correlation between upper and lower triangular matrices (excluding diagonal)'] = np.corrcoef(upper_nodiag,lower_nodiag)[0,1]

fig, ax = plt.subplots() 
ax.scatter(upper_nodiag,lower_nodiag, s=.1)
ax.set_xlim(0,1000)
ax.set_ylim(0,1000)


c = np.transpose(np.transpose(b)/np.sum(b,axis=1))
sns.heatmap(c)

upper = c[np.triu_indices(c.shape[0])]
lower = np.transpose(c)[np.triu_indices(c.shape[0])]

upper_nodiag = c[np.triu_indices(c.shape[0],k=1)]
lower_nodiag = np.transpose(c)[np.triu_indices(c.shape[0],k=1)]


print("Correlation between normalized upper and lower triangular matrices: ", np.corrcoef(upper,lower)[0,1])
print("Correlation between normalized upper and lower triangular matrices (excluding diagonal): ", np.corrcoef(upper_nodiag,lower_nodiag)[0,1])

sumstats_dict['Correlation between normalized upper and lower triangular matrices'] = np.corrcoef(upper,lower)[0,1]
sumstats_dict['Correlation between normalized upper and lower triangular matrices (excluding diagonal)'] = np.corrcoef(upper_nodiag,lower_nodiag)[0,1]


###############################################################################
# Write contents of sumstats_dict to a csv
###############################################################################

# Open a file for writing
with open(root + 'Results/summary_stats/sumstats.csv', 'w', newline='') as csvfile:
    # Create a CSV writer object
    writer = csv.writer(csvfile)
    # Write header
    writer.writerow(['Key', 'Value'])
    # Write dictionary contents
    for key, value in sumstats_dict.items():
        writer.writerow([key, value])

pickle.dump(sumstats_dict, open(root + 'Results/summary_stats/sumstats_dict.p', "wb"))
