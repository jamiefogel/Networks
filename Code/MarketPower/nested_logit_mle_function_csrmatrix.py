
import numpy as np
from scipy.sparse import csr_matrix, vstack
import os
import pandas as pd

os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/_git/Networks/Code/MarketPower/')

# LOADING ONE EXAMPLE
data = pd.read_csv('nested_logit_example.csv')
#data = pd.read_csv('nested_logit_example_zeros.csv')
data
theta = 1
eta = 0
x = csr_matrix(data[['i1','i2','i3']])
g = csr_matrix(data['g'])

# USING OUR REAL DATA
#x = mean_wage_matrix
#x.shape
#g = csr_matrix(jid_gamma_cw.astype(int))
#g.shape

def collapse_rows(z, g, G, G_min):
    for i in range(G_min,G+1):
        if i == G_min:
            zgi = csr_matrix(z[g.data == i].sum(axis=0))
        else:
            zgi = vstack([zgi, csr_matrix(z[g.data == i].sum(axis=0))])
    return zgi

def log_or_zero(matrix):
    return csr_matrix(np.where(matrix.toarray() != 0, np.log(matrix.toarray()), 0))

# DATA PREP FOR EFICIENCY
I = x.shape[1]
J = x.shape[0]
G = np.max(g)
G_min = np.min(g)
g_card = csr_matrix(np.unique(g.toarray(), return_counts=True)[1])

# NESTED LOGIT LOG LIKELIHOOD
# I am following our overleaf document, nested logit MLE subsection:
# https://www.overleaf.com/project/63852b08ac01347091649216
def nested_logit_log_likelihood(x,g,theta,eta,I,G,J,G_min,g_card):
    ###########################
    # Quick data prep
    z = x.power(1+eta)    
    zgi = collapse_rows(z, g, G, G_min)

    ###########################
    # TERM 3
    term3 = np.sum(log_or_zero(z))
    
    ###########################
    # TERM 1
    log_zgi = log_or_zero(zgi)
    term1 = ((theta - eta) / (eta+1)) * np.sum(g_card * log_zgi)
    
    ###########################
    # TERM 2
    term2 = np.sum(np.log((g_card * zgi.power((theta+1)/(eta+1))).toarray()))*J

    return term1 + term2 + term3

nested_logit_log_likelihood(x,g,theta,eta,I,G,J,G_min,g_card)











