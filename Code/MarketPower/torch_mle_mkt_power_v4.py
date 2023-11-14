import pandas as pd
import numpy as np
import pickle
import os
import torch
from datetime import datetime
from scipy.sparse import csr_matrix


homedir = os.path.expanduser('~')
data_dir = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/Data/derived/MarketPower'

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
    # FUNCTIONS FOR THE MLE
    ################################
    def collapse_rows(z, g, G, G_min):
        for i in g.unique().tolist():
            z_sum = torch.sum(z[g == i], dim=0)
            if i == G_min:
                zgi = z_sum
            elif i == G_min+1:
                zgi = torch.cat((zgi.unsqueeze(0), z_sum.unsqueeze(0)), dim=0)
            else:
                zgi = torch.cat((zgi, z_sum.unsqueeze(0)), dim=0)
        return zgi
    def nested_logit_likelihood_perj(G, G_min, g, j, z, zgi, theta, eta, baseline_mkt_choice_nolog):
        num2 = z[j, :]
        denom2 = zgi[g[j], :]
        num1 = zgi[g[j], :].pow((1 + theta) / (1 + eta))
        denom1 = baseline_mkt_choice_nolog
        return (num1/denom1)*(num2/denom2)
    def nested_logit_likelihood_probs(G, I, J, G_min, g, x, theta, eta):
        z = x.pow(1 + eta)
        zgi = collapse_rows(z, g, G, G_min)
        baseline_mkt_choice_nolog = torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0)
        probs = torch.empty(J, I)
        for j in range(J):
            probs[j, :] = nested_logit_likelihood_perj(
                G, G_min, g, j, z, zgi, theta, eta, baseline_mkt_choice_nolog)
        return torch.where(probs > 0, probs, torch.zeros_like(probs))
    def reindex_g(g_index):    
        unique_values = torch.unique(g_index) # Get unique values from the original tensor
        value_to_label = {value.item(): label for label, value in enumerate(unique_values)} # Create a mapping from unique values to labels
        return torch.tensor([value_to_label[value.item()] for value in g_index])
    ################################
    # LOAD AND PREP DATA
    ################################
    # load data
    probs, x, g_index = pickle.load(open('nested_logit.p', "rb" ))
    x = torch.tensor(x.toarray(), requires_grad=False)
    probs = torch.tensor(probs.toarray(), requires_grad=False)
    g_index = torch.tensor(g_index['gamma'].values, dtype=torch.int32, requires_grad=False)
    # creating important objectives for the mle
    I = x.shape[1]
    J = x.shape[0]
    g = reindex_g(g_index)
    G = torch.max(g)
    G_min = torch.min(g)
    ################################
    # OPTIMIZATION
    ################################
    # SGD
    inv_eta_mayara   = 0.985
    inv_theta_mayara = 1.257
    theta    = torch.tensor(1/inv_theta_mayara,    requires_grad=True)
    eta    = torch.tensor(1/inv_eta_mayara,    requires_grad=True)
    optimizer = torch.optim.Adam([theta, eta], lr=2e-3)
    tol = .0001
    maxiter = 10000
    iter = 1
    converged = 0
    theta_prev = theta.detach().clone() + 1
    eta_prev    = eta.detach().clone() + 1
    obj_function_to_minimize_vec = list()
    while converged == 0 and iter<maxiter :
        optimizer.zero_grad() # discards previous computations of gradients                                                                                                   
        obj_function_to_minimize = torch.log(1 + torch.sum(torch.pow(probs - nested_logit_likelihood_probs(G, I, J, G_min, g, x, theta, eta), 2)))        
        obj_function_to_minimize.backward(retain_graph=True) # computes the gradient of the function at the current value of the argument                                                                    
        optimizer.step() # updates the argument using gradient descent                                                                                                        
        end = datetime.now()
        print('Obj func = ', round(obj_function_to_minimize.item(), 5),', step = ', iter, ' t =', end - start)
        print('theta = ', round(theta.item(), 5), ', eta = ', round(eta.item(), 5))
        if iter % 10:
            obj_function_to_minimize_vec.append(obj_function_to_minimize)
        if torch.max(torch.abs(theta-theta_prev)) <tol and  torch.max(torch.abs(eta-eta_prev)) <tol:
            converged = 1
            print("MLE converged in " + str(iter) + " iterations")
        if torch.isnan(obj_function_to_minimize):
            converged = -1
            raise ValueError('Likelihood is NAN on iteration ' + str(maxiter))
        theta_prev = theta.detach().clone()
        eta_prev    = eta.detach().clone()
        iter = iter + 1
        if converged!=1:
            raise RuntimeError('Failed to converge after ' + str(maxiter) + ' iterations')
    estimates = {}
    estimates['theta']       = theta
    estimates['eta'] = eta 
    pickle.dump(estimates, open('nested_logit_estimates.p', "wb"))


torch_mle(data_dir)