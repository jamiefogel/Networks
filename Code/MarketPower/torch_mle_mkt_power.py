import pandas as pd
import numpy as np
import pickle
import os
import sys
import torch
from datetime import datetime
from scipy.sparse import csr_matrix


homedir = os.path.expanduser('~')
data_dir = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/Data/derived'

def torch_mle(data_dir):
    '''
    - move run_query_sums/mle_load_fulldata_vs outside torch_mle
    - Make torch_mle a function
    - Take arguments  saved_mle_sums, estimates_savefile
    '''    
    print("Now running")
    start = datetime.now()
    print(start)
    os.chdir(data_dir)    
    ################################
    # LOAD AND PREP DATA
    ################################
    # Load data
    x, g = pickle.load(open('nested_logit.p', "rb" ))
    x = torch.tensor(x.toarray(), requires_grad=False)
    g = torch.tensor(g.values, dtype=torch.int32, requires_grad=False)
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
    g_card = g_counts.view(1,G).to(torch.float64)
    ################################
    # LOG-LIKELIHOOD FUNCTION
    ################################
    # NESTED LOGIT LOG LIKELIHOOD
    # I am following our overleaf document, nested logit MLE subsection:
    # https://www.overleaf.com/project/63852b08ac01347091649216
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
    ################################
    # OPTIMIZATION
    ################################
    # SGD
    inv_eta_mayara   = 0.985
    inv_theta_mayara = 1.257
    theta    = torch.tensor(1/inv_theta_mayara,    requires_grad=True)
    eta    = torch.tensor(1/inv_eta_mayara,    requires_grad=True)
    optimizer = torch.optim.Adam([theta, eta], lr=2e-3)
    tol = .01
    maxiter = 10000
    iter = 1
    converged = 0
    theta_prev = theta.detach().clone() + 1
    eta_prev    = eta.detach().clone() + 1
    negloglike_vec = list()
    while converged == 0 and iter<maxiter :
        optimizer.zero_grad() # discards previous computations of gradients                                                                                                   
        negloglike = -nested_logit_log_likelihood(x,g,theta,eta,I,G,J,G_min,g_card)
        negloglike.backward() # computes the gradient of the function at the current value of the argument                                                                    
        optimizer.step() # updates the argument using gradient descent                                                                                                        
        #print(negloglike)
        end = datetime.now()
        print('Negative log-likelihood = ', negloglike, 'step = ', iter, ' t =',end - start)
        print('theta = ', theta, 'eta = ', eta)
        if iter % 10:
            negloglike_vec.append(negloglike)
        if torch.max(torch.abs(theta-theta_prev)) <tol and  torch.max(torch.abs(eta-eta_prev)) <tol:
            converged = 1
            print("MLE converged in " + str(iter) + " iterations")
        if torch.isnan(negloglike):
            converged = -1
            raise ValueError('Log-likelihood is NAN on iteration ' + str(maxiter))
        theta_prev = theta.detach().clone()
        eta_prev    = eta.detach().clone()
        iter = iter + 1
        #print(iter)
    if converged!=1:
        raise RuntimeError('Failed to converge after ' + str(maxiter) + ' iterations')
    estimates = {}
    estimates['theta']       = theta
    estimates['eta'] = eta 
    pickle.dump(estimates, open('nested_logit_estimates.p', "wb"))
    

torch_mle(data_dir)