
import numpy as np
from scipy.sparse import csr_matrix, vstack
import os
import pandas as pd
import torch
import pickle

os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/_git/Networks/Code/MarketPower/')

# LOADING ONE EXAMPLE
data = pd.read_csv('nested_logit_example.csv')
#data = pd.read_csv('nested_logit_example_zeros.csv')
data
theta = 1
eta = 0
x = data[['i1','i2','i3']]
g = data['g'].astype(int)

# Load your data
x = torch.tensor(x.values)
g = torch.tensor(g.values, dtype=torch.int32)  # Convert g to a torch tensor

theta = torch.tensor(1.0, requires_grad=True)
eta = torch.tensor(0.0, requires_grad=True)

def collapse_rows(z, g, G, G_min):
    for i in range(G_min, G+1):
        z_sum = torch.sum(z[g == i], dim=0)
        if i == G_min:
            zgi = z_sum
        elif i == 2:
            zgi = torch.cat((zgi.unsqueeze(0), z_sum.unsqueeze(0)), dim=0)        
        else:
            zgi = torch.cat((zgi, z_sum.unsqueeze(0)), dim=0)
    return zgi

def log_or_zero(matrix):
    return torch.where(matrix != 0, torch.log(matrix), torch.zeros_like(matrix))

# DATA PREP FOR EFFICIENCY
I = x.shape[1]
J = x.shape[0]
G = torch.max(g)
G_min = torch.min(g)
unique_g, g_counts = torch.unique(g, return_counts=True)
g_card = torch.tensor(g_counts, dtype=torch.float32).view(1,G)

# NESTED LOGIT LOG LIKELIHOOD
def nested_logit_log_likelihood(x, g, theta, eta, I, G, J, G_min, g_card):
    ###########################
    # Quick data prep
    z = x.pow(1 + eta)
    zgi = collapse_rows(z, g, G, G_min)
    ###########################
    # TERM 3
    term3 = torch.sum(log_or_zero(z))
    ###########################
    # TERM 1
    log_zgi = log_or_zero(zgi)
    term1 = ((theta - eta) / (eta + 1)) * torch.sum(torch.mm(g_card,log_zgi))
    ###########################
    # TERM 2
    term2 = J * torch.sum(log_or_zero(torch.mm(g_card,zgi.pow((theta + 1) / (eta + 1)))))
    return term1 + term2 + term3

# Calculate the nested logit log likelihood
likelihood = nested_logit_log_likelihood(x, g, theta, eta, I, G, J, G_min, g_card)
print("Nested Logit Log Likelihood:", likelihood.item())

