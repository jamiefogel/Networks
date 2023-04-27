#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  3 08:39:29 2021

@author: jfogel
"""

import torch
import pandas as pd
import pickle
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import sklearn
import os

diverging_colors = sns.color_palette("RdBu", 10)
cmap=sns.palplot(diverging_colors)

def correlogram(psi_hat, corrsavefile, histsavefile, cosine=False, p_ig=None):
    
    if p_ig!=None:
        psi_hat = psi_hat * p_ig[:,1:]
    
    if cosine==True:
        corrmatrix = sklearn.metrics.pairwise.cosine_similarity(psi_hat, dense_output=True)
    else: 
        #psi_hat_T = pd.DataFrame(np.array(torch.transpose(psi_hat,0,1)/psi_hat.mean(dim=1)))
        psi_hat_T = pd.DataFrame(np.array(torch.transpose(psi_hat, 0, 1)))
        corrmatrix = psi_hat_T.corr()
        
        
    mask = np.triu(np.ones_like(corrmatrix, dtype=bool))
    # We are currently omitting the diagonal since it will be all ones. This alternative includes the diagonal.
    #mask = 1-np.tril(np.ones_like(corrmatrix, dtype=bool))
    
    fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
    #sns.heatmap(df1.iloc[:, 1:6:], annot=True, linewidths=.5, ax=ax)
    sns.heatmap(
        corrmatrix, 
        mask = mask,
        vmin=-1, vmax=1, center=0,
        cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
        #cmap=sns.color_palette("coolwarm", n_colors=20),
        #cmap=sns.color_palette("RdBu", 100),
        square=False,
        xticklabels=False,
        yticklabels=False,
        ax=ax
    )
    #ax.set_aspect(1.2)
    ax.tick_params(axis='both', which='major', labelsize=18)
    ax.figure.savefig(corrsavefile, dpi=300, bbox_inches="tight")

    #Extract unique correlations coefficients (drop diagonal and upper triangle) and plot histogram
    a = np.tril(corrmatrix).flatten()
    coefs = a[(a!=1) & (a!=0)]
    coefs = np.round(coefs,8)   # Something weird is going on with rounding error in the specific skills benchmark
    
    fig, ax = plt.subplots(figsize=(5.76,4.8))
    ax.hist(coefs, density=False, bins=20)
    ax.set_xlim(-1,1)
    ax.text(.6, .9, r'Std Dev='+str(np.round(np.std(coefs), decimals=3)), fontsize=16, transform = ax.transAxes)
    #ax.tick_params(axis='both', which='major', labelsize=18)
    plt.show()    
    ax.figure.savefig(histsavefile, dpi=300, bbox_inches="tight")



for idx in [('iota','gamma'), ('occ4_first_recode','sector_IBGE'), ('occ4_first_recode','gamma')]:
    # using wtype_var and jtype_Var instead of worker_type_var and job_type_var so we don't reset the master versions set in do_all.py
    wtype_var = idx[0]
    jtype_var    = idx[1]
    print(wtype_var, jtype_var)
    mle_data_sums_corr_filename = homedir + "/Networks/RAIS_exports/earnings_panel/panel_rio_2009_2012_mle_data_sums_" + wtype_var + "_" + jtype_var + "_level_0.p"
    mle_data_sums_corr = pickle.load(open(mle_data_sums_corr_filename, "rb"), encoding='bytes')
    psi_hat = pickle.load(open(homedir + "/Networks/Code/june2021/MLE_estimates/panel_rio_2009_2012_psi_normalized_" + wtype_var + "_" + jtype_var + "_level_0_eta_2.p", "rb"), encoding='bytes')['psi_hat']    
    
    # Merge on mean wages by worker type and sort
    psi_hat_merged = torch.cat((torch.reshape(mle_data_sums_corr['mean_wage_i'],(mle_data_sums_corr['mean_wage_i'].shape[0],1)), psi_hat),dim=1)
    psi_hat_sorted = psi_hat_merged[psi_hat_merged[:, 0].sort()[1]][:,1:]

    corr_name_stub = 'correlograms_' + wtype_var + '_' + jtype_var
    hist_name_stub = 'correlograms_hist_' + wtype_var + '_' + jtype_var
    correlogram(psi_hat,        figuredir+corr_name_stub + '.png'                , figuredir+hist_name_stub + '.png')
    correlogram(psi_hat_sorted, figuredir+corr_name_stub + '_sorted.png'         , figuredir+hist_name_stub + '_sorted.png')
    correlogram(psi_hat_sorted, figuredir+corr_name_stub + '_sorted_weighted.png', figuredir+hist_name_stub + '_sorted_weighted.png', p_ig = mle_data_sums_corr['p_ig_actual'])
  
    
  
    
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
correlogram(psi_2_clusters,        figuredir+corr_name_stub + '.png'                , figuredir+hist_name_stub + '.png')

# Highly correlated
psi_1_cluster =  torch.tensor(np.transpose(np.random.multivariate_normal(np.ones(50), np.random.uniform(low=10, high=10.5, size=(50,50)), (200))) )
corr_name_stub = 'correlograms_benchmark_1_cluster'
hist_name_stub = 'correlograms_hist_benchmark_1_cluster'
correlogram(psi_1_cluster,        figuredir+corr_name_stub + '.png'                , figuredir+hist_name_stub + '.png')

# Uncorrelated
psi_specific_skills = torch.tensor(np.diag(np.random.uniform(low=.5, high=2, size=(50))))
corr_name_stub = 'correlograms_benchmark_specific_skills'
hist_name_stub = 'correlograms_hist_benchmark_specific_skills'
correlogram(psi_specific_skills,        figuredir+corr_name_stub + '.png'                , figuredir+hist_name_stub + '.png')



####################################################################################
# Compute NMI
####################################################################################


import sklearn.metrics

data_full = pd.read_csv(homedir + "/Networks/RAIS_exports/earnings_panel/panel_rio_2009_2012_w_kmeans.csv")
data_full = data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1) & (data_full['occ4_first_recode']!=-1)]
data_full = data_full.loc[(data_full.year>=2009) & (data_full.year<=2012)]

print('Printing normalized mutual information between pairs of variables (in correlogram.py).')
with open(figuredir + "nmi.tex", "w") as f:  
    for idx in [('iota','gamma'), ('iota','cbo2002_first'), ('iota','clas_cnae20_first'), ('gamma','cbo2002_first'), ('gamma','clas_cnae20_first'), ('cbo2002_first','clas_cnae20_first')]:
        nmi = sklearn.metrics.normalized_mutual_info_score(data_full[idx[0]], data_full[idx[1]], average_method='arithmetic')
        f.write(idx[0].replace('_','\_') + ' & ' + idx[1].replace('_','\_')  + ' & ' + str(round(nmi,3)) + '\n')
        print(idx[0], idx[1], round(nmi, 3))

print('Done with NMI')



####################################################################################
# Compute occupation and industry transition rates
####################################################################################

df = pd.read_csv(root + "Data/RAIS_exports/earnings_panel/panel_rio_2009_2012_w_kmeans.csv", nrows=None)

# I can't find the data set temp_educ. Need to figure this out once we get access to the server back. 
#df_educ = pd.read_csv(homedir + "/Networks/RAIS_exports/earnings_panel/temp_educ.csv", nrows=None)
#df = df.drop(columns='_merge').merge(df_educ, on=['wid_masked','jid_masked','year'], how='left', indicator=True)

df['occ4'] = pd.to_numeric(df['cbo2002'].astype(str).str.slice(0,4), errors='coerce')
df['occ2'] = pd.to_numeric(df['cbo2002'].astype(str).str.slice(0,2), errors='coerce')
df['occ1'] = pd.to_numeric(df['cbo2002'].astype(str).str.slice(0,1), errors='coerce')
df['jid_masked'].loc[df.jid_masked==-1] = np.nan

df['jid_masked_lag'] = df.groupby(['wid_masked'])['jid_masked'].shift(1)
df['occ1_lag'] = df.groupby(['wid_masked'])['occ1'].shift(1)
df['occ2_lag'] = df.groupby(['wid_masked'])['occ2'].shift(1)
df['occ4_lag'] = df.groupby(['wid_masked'])['occ4'].shift(1)
df['gamma_lag'] = df.groupby(['wid_masked'])['gamma'].shift(1)
df['cbo2002_lag'] = df.groupby(['wid_masked'])['cbo2002'].shift(1)
df['clas_cnae20_lag'] = df.groupby(['wid_masked'])['clas_cnae20'].shift(1)
df['sector_IBGE_lag'] = df.groupby(['wid_masked'])['sector_IBGE'].shift(1)
df['job_change'] = (df.jid_masked != df.jid_masked_lag) & (pd.isnull(df.jid_masked)==False) & (pd.isnull(df.jid_masked_lag)==False)
# df['educ'] = df.grau_instr.map({1:'dropout',2:'dropout',3:'dropout',4:'dropout',5:'dropout',6:'dropout',7:'hs',8:'some_college',9:'college',10:'grad',11:'grad'})

changes_df = df.loc[df.job_change==True][['cbo2002','cbo2002_lag','clas_cnae20','clas_cnae20_lag','gamma','gamma_lag','occ1','occ1_lag','occ2','occ2_lag','occ4','occ4_lag','sector_IBGE', 'sector_IBGE_lag']]
#changes_df = df.loc[df.job_change==True][['cbo2002','cbo2002_lag','clas_cnae20','clas_cnae20_lag','gamma','gamma_lag','occ1','occ1_lag','occ2','occ2_lag','occ4','occ4_lag','sector_IBGE', 'sector_IBGE_lag', 'educ']]
changes_df['change_occ6'] = changes_df.cbo2002!=changes_df.cbo2002_lag
changes_df['change_occ1']= changes_df.occ1!=changes_df.occ1_lag
changes_df['change_occ2']= changes_df.occ2!=changes_df.occ2_lag
changes_df['change_occ4']= changes_df.occ4!=changes_df.occ4_lag
changes_df['change_ind'] = changes_df.clas_cnae20!=changes_df.clas_cnae20_lag
changes_df['change_sector'] = changes_df.sector_IBGE!=changes_df.sector_IBGE_lag
changes_df['change_gamma']= changes_df.gamma!=changes_df.gamma_lag

print('Printing fractions of job changes that also change iota/gamma/occ/etc (in correlogram.py)')
print(changes_df.change_occ1.mean())
print(changes_df.change_occ2.mean())
print(changes_df.change_occ4.mean())
print(changes_df.change_occ6.mean())
print(changes_df.change_ind.mean())
print(changes_df.change_sector.mean())
print(changes_df.change_gamma.mean())

output_df = pd.DataFrame(columns=['Variable','Fraction'])
vars = ('change_occ1', 'change_occ2', 'change_occ4', 'change_occ6', 'change_ind', 'change_sector', 'change_gamma')
varnames = ('1-digit Occupation', '2-digit Occupation', '4-digit Occupation', '6-digit Occupation', 'Industry', 'Sector (IBGE)', 'Gamma')

idx = 0
for v in vars:
    newrow = {'Variable':varnames[idx],'Fraction':round(changes_df[v].mean(),3)}
    output_df = output_df.append(newrow, ignore_index=True)
    idx = idx+1

print(output_df)

# It would be good to do this for employer changes to kind of get at the job ladder thing but we'll need to do that on the IPEA server since we haven't brought over employer ID


print(df.occ1.value_counts().shape)
print(df.occ2.value_counts().shape)
print(df.occ4.value_counts().shape)
print(df.cbo2002.value_counts().shape)
print(df.clas_cnae20.value_counts().shape)
print(df.gamma.value_counts().shape)

d = {'occ1':1, 'occ2':2, 'occ4':4}


# Commented out because can't find temp_educ.csv
# print(changes_df.groupby(['educ'])['change_occ1'].mean())
# print(changes_df.groupby(['educ'])['change_occ2'].mean())
# print(changes_df.groupby(['educ'])['change_occ4'].mean())
# print(changes_df.groupby(['educ'])['change_occ6'].mean())
# print(changes_df.groupby(['educ'])['change_ind'].mean())
# print(changes_df.groupby(['educ'])['change_gamma'].mean())




# Gamma transition matrix
#data_full = data_full.pivot(index='wid_masked', columns='year', values=['employed','real_hrly_wage_dec','ln_real_hrly_wage_dec','iota','occ4_first_recode','sector_IBGE','gamma'])
    





'''
1	Analfabeto, inclusive o que, embora tenha recebido instrução, não se alfabetizou.
2	Até o 5º ano incompleto do Ensino Fundamental (antiga 4ª série) que se tenha alfabetizado sem ter frequentado escola regular.
3	5º ano completo do Ensino Fundamental.
4	Do 6º ao 9º ano do Ensino Fundamental incompleto (antiga 5ª à 8ª série).
5	Ensino Fundamental completo.
6	Ensino Médio incompleto.
7	Ensino Médio completo.
8	Educação Superior incompleta.
9	Educação Superior completa.
10	Mestrado completo.
11	Doutorado completo.
'''


# From page 9 of David Arnold's JMP: "Figure A1 computes the probability a job transition is within a given occupation or industry cell using Brazilian matched employer-employee data. As can be seen in the figure, at the 1-digit level, about 60 percent of transitions are within the same occupation, while about 50 percent are within the same industry. At the 4-digit level, about 22 percent of job transitions are within the same occupation, while about 19 percent are within the same industry." So we're in the same ballpark even if we're using somewhat different samples.


# If two rows of a matrix are identical, then the matrix is singular (https://math.libretexts.org/Bookshelves/Linear_Algebra/Book%3A_A_First_Course_in_Linear_Algebra_(Kuttler)/03%3A_Determinants/3.02%3A_Properties_of_Determinants). Is there a continuous metric of matrix singularity (like if it's equal to 1 the matrix is singular and if it's equal to 0 then the matrix is as orthogonal as possible)? If so, this could complement the correlograms. 



#correlogram(psi_hat, figuredir+'correlograms_' + worker_type_var + '_' + job_type_var + '.png' , figuredir+'c' + worker_type_var + '_' + job_type_var + '.png' ,sorted=False)

####################################################################################
# Variance decomposition with earnings that didn't work
####################################################################################

data_full = pd.read_csv(mle_data_filename)
data_full = data_full.loc[(data_full['gamma']>0) & (data_full['iota']!=-1)]

df = data_full[['iota','gamma','occ4_first_recode','sector_IBGE','ln_real_hrly_wage_dec']]
df['iota_mean_wage'] = df.groupby('iota'             )['ln_real_hrly_wage_dec'].transform('mean') 
df['occ4_mean_wage'] = df.groupby('occ4_first_recode')['ln_real_hrly_wage_dec'].transform('mean')
df['demeaned_ln_wage_iota'] = df.ln_real_hrly_wage_dec - df.iota_mean_wage
df['demeaned_ln_wage_occ4'] = df.ln_real_hrly_wage_dec - df.occ4_mean_wage

df.ln_real_hrly_wage_dec.var()
df.demeaned_ln_wage_iota.var()
df.iota_mean_wage.var()

df.ln_real_hrly_wage_dec.var()
df.demeaned_ln_wage_occ4.var()
df.occ4_mean_wage.var()





####################################################################################
# iota-occ4 crosstab
# - If all we did was replicate occ4 this would be diagonal. Clearly it is not. 
####################################################################################



data_full = pd.read_csv(mle_data_filename)
crosstab_iota_occ4 = pd.crosstab(index = data_full.iota.loc[data_full.iota!=-1], columns = data_full.occ4_first_recode.loc[data_full.occ4_first_recode!=-1])

from matplotlib.colors import LogNorm, Normalize
fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
sns.heatmap(
     crosstab_iota_occ4+.000001, # Add epsilon to avoid log(0)
     norm=LogNorm(),
     #vmin=-1, vmax=1, center=0,
     cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
     #cmap=sns.color_palette("coolwarm", n_colors=20),
     #cmap=sns.color_palette("RdBu", 100),
     square=False,
     xticklabels=False,
     yticklabels=False,
     ax=ax
 )
#ax.set_aspect(1.2)
ax.tick_params(axis='both', which='major', labelsize=18)
ax.figure.savefig(figuredir + 'iota_occ4_crosstab_heatmap.png', dpi=300, bbox_inches="tight")



# Same thing but rescaling it so it's the share of the iota not the raw count
crosstab_iota_occ4_scale = crosstab_iota_occ4.div(crosstab_iota_occ4.sum(axis=1), axis=0)

from matplotlib.colors import LogNorm, Normalize
fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
sns.heatmap(
     crosstab_iota_occ4_scale+.0000000001, # Add epsilon to avoid log(0)
     norm=LogNorm(),
     #vmin=-1, vmax=1, center=0,
     cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
     #cmap=sns.color_palette("coolwarm", n_colors=20),
     #cmap=sns.color_palette("RdBu", 100),
     square=False,
     xticklabels=False,
     yticklabels=False,
     ax=ax
 )
#ax.set_aspect(1.2)
ax.tick_params(axis='both', which='major', labelsize=18)
ax.figure.savefig(figuredir + 'iota_occ4_crosstab_heatmap_share.png', dpi=300, bbox_inches="tight")



############3
# Occ6 
crosstab_iota_occ6 = pd.crosstab(index = data_full.iota.loc[data_full.iota!=-1], columns = data_full.cbo2002_first.loc[data_full.cbo2002_first!=-1])

from matplotlib.colors import LogNorm, Normalize
fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
sns.heatmap(
     crosstab_iota_occ6+.000001, # Add epsilon to avoid log(0)
     norm=LogNorm(),
     #vmin=-1, vmax=1, center=0,
     cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
     #cmap=sns.color_palette("coolwarm", n_colors=20),
     #cmap=sns.color_palette("RdBu", 100),
     square=False,
     xticklabels=False,
     yticklabels=False,
     ax=ax
 )
#ax.set_aspect(1.2)
ax.tick_params(axis='both', which='major', labelsize=18)
ax.figure.savefig(figuredir + 'iota_occ6_crosstab_heatmap.png', dpi=300, bbox_inches="tight")

# Same thing but rescaling it so it's the share of the iota not the raw count
crosstab_iota_occ6_scale = crosstab_iota_occ6.div(crosstab_iota_occ6.sum(axis=1), axis=0)

fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
sns.heatmap(
     crosstab_iota_occ6_scale+.0000000001, # Add epsilon to avoid log(0)
     norm=LogNorm(),
     #vmin=-1, vmax=1, center=0,
     cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
     #cmap=sns.color_palette("coolwarm", n_colors=20),
     #cmap=sns.color_palette("RdBu", 100),
     square=False,
     xticklabels=False,
     yticklabels=False,
     ax=ax
 )
#ax.set_aspect(1.2)
ax.tick_params(axis='both', which='major', labelsize=18)
ax.figure.savefig(figuredir + 'iota_occ6_crosstab_heatmap_share.png', dpi=300, bbox_inches="tight")



#############################################
# Degree distribution histograms
#############################################

[worker_degs, job_degs] = pickle.load(open( homedir + '/Networks/RAIS_exports/other/degree_distribution.p', 'rb'))

fig, ax = plt.subplots(figsize=(5.76,4.8))
ax.hist(worker_degs, density=False, bins=20)
plt.yscale('log')
plt.show()    
ax.figure.savefig(figuredir + 'worker_degree_distribution_hist.png', dpi=300, bbox_inches="tight")


print('Average worker degree: ', worker_degs.mean())
print('Average job degree: ', job_degs.mean())

print('Fraction of workers with more than one job: ', (worker_degs>1).mean())


def plot_loghist(x, bins, savefile):
  hist, bins = np.histogram(x, bins=bins)
  logbins = np.logspace(np.log10(bins[0]),np.log10(bins[-1]),len(bins))
  fig, ax = plt.subplots(figsize=(5.76,4.8))
  ax.hist(x, bins=logbins)
  plt.xscale('log')
  plt.yscale('log')
  plt.show()    
  ax.figure.savefig(savefile , dpi=300, bbox_inches="tight")


#plot_loghist(worker_degs, 20, figuredir +'worker_degree_distribution_hist.png')
plot_loghist(job_degs[job_degs>=5], 50, figuredir +'job_degree_distribution_hist.png')





########################################################
# Distribution  of iota and gamma sizes histograms
########################################################

# XX Not sure where I created this file
[iota_sizes, gamma_sizes] = pickle.load(open( homedir + '/Networks/RAIS_exports/other/iota_gamma_sizes.p', 'rb'))

fig, ax = plt.subplots(figsize=(5.76,4.8))
ax.hist(iota_sizes, density=False, bins=50)
#plt.yscale('log')
plt.show()    
ax.figure.savefig(figuredir + 'iota_size_distribution.png', dpi=300, bbox_inches="tight")


fig, ax = plt.subplots(figsize=(5.76,4.8))
ax.hist(gamma_sizes, density=False, bins=50)
#plt.yscale('log')
plt.show()    
ax.figure.savefig(figuredir + 'gamma_size_distribution.png', dpi=300, bbox_inches="tight")


        
iota_sizes_by_worker = iota_sizes.loc[iota_sizes.index.repeat(iota_sizes)]
iota_sizes_by_worker.mean()
iota_sizes_by_worker.median()

        
gamma_sizes_by_worker = gamma_sizes.loc[gamma_sizes.index.repeat(gamma_sizes)]
gamma_sizes_by_worker.mean()
gamma_sizes_by_worker.median()
