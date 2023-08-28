# srun --pty --account=lsa1 /bin/bash  
# module load python3.6-anaconda

import pandas as pd
import numpy as np
import pickle
import os
import sys
import torch
from scipy.sparse import csr_matrix, vstack
from datetime import datetime

homedir = os.path.expanduser('~')
data_dir = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/Data/derived'


def torch_mle(data_dir):
    '''
    - move run_query_sums/mle_load_fulldata_vs outside torch_mle
    - Make torch_mle a function
    - Take arguments  saved_mle_sums, estimates_savefile
    '''    
    print("Now running")
    os.chdir(data_dir)    
    ################################
    # LOAD AND PREP DATA
    ################################
    # Load data
    x = pickle.load(open('nested_logit.p', "rb" ))[0]
    g = csr_matrix(pickle.load(open('nested_logit.p', "rb" ))[1].astype(int))
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
    ################################
    # LOG-LIKELIHOOD FUNCTION
    ################################
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
        #term2 = np.sum(np.log((g_card * zgi.power((theta+1)/(eta+1))).toarray()))*J # old code
        term2 = np.sum(log_or_zero((g_card * zgi.power((theta+1)/(eta+1)))))*J
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
        print('Negative log-likelihood = ', negloglike, 'step = ', iter)
        print('theta = ', theta)
        print('eta = ', eta)
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