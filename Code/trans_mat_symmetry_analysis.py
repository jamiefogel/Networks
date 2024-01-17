#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This was previously called misc_analysis.py

Created on Tue Oct  5 14:21:15 2021

@author: jfogel
"""

import seaborn as sns

mle_data_filename      = root + 'Data/RAIS_exports/earnings_panel/panel_rio_2009_2012_w_kmeans.csv'
data_full = pd.read_csv(mle_data_filename)

##############################################################################
# To what extent are moves between gammas symmetric? Our model predicts that they are
gamma_trans_mat = pd.read_pickle('~/Downloads/gamma_trans_mat.p')

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



