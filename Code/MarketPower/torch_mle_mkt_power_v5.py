import pandas as pd
import numpy as np
import pickle
import os
import torch
from datetime import datetime
homedir = os.path.expanduser('~')
data_dir = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/Data/derived/MarketPower'
#def torch_mle(data_dir):
'''
- move run_query_sums/mle_load_fulldata_vs outside torch_mle
- Make torch_mle a function
- Take arguments  saved_mle_sums, estimates_savefile
'''    
print("Now running")
start = datetime.now()
print(start)
os.chdir(data_dir)
read_fewer_rows = True  
fewer_rows = 50000  
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
def reindex_g(g):    
    unique_values = torch.unique(g) # Get unique values from the original tensor
    value_to_label = {value.item(): label for label, value in enumerate(unique_values)} # Create a mapping from unique values to labels
    return torch.tensor([value_to_label[value.item()] for value in g])
#def nested_logit_likelihood_probs_full_shortcut(G, I, J, G_min, g, x, theta, eta):
#    z = x.pow(1 + eta)
#    zgi = collapse_rows(z, g, G, G_min)
#    baseline_mkt_choice_nolog = torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0)
#    probs = (zgi[g, :].pow((1 + theta) / (1 + eta) - 1) * z) / baseline_mkt_choice_nolog
#    return probs
#    #return torch.where(probs > 0, probs, torch.zeros_like(probs))
def nested_logit_likelihood_probs_full_shortcut(G, I, J, G_min, g, x, theta, eta):
    z = x.pow(1 + eta)
    zgi = collapse_rows(z, g, G, G_min)
    baseline_mkt_choice_nolog = torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0)
    # Create a mask for non-zero values in x
    non_zero_mask = x != 0.0
    # Compute probs only for non-zero values in x
    probs = torch.zeros_like(x)  # Initialize with zeros
    probs[non_zero_mask] = (zgi[g, :][non_zero_mask].pow((1 + theta) / (1 + eta) - 1) * z[non_zero_mask])
    probs = probs / baseline_mkt_choice_nolog
    return probs
def torch_check(w):
    print('Number of zeros = ', torch.count_nonzero(w==0).sum())
    print('Number of NaNs = ',torch.isnan(w).sum())
    print('Number of Infs = ',torch.isinf(w).sum())
################################
# LOAD AND PREP DATA
################################
# load data
probs, x, g_index = pickle.load(open('nested_logit.p', "rb" ))
x = torch.tensor(x.toarray(), requires_grad=False)
probs = torch.tensor(probs.toarray(), requires_grad=False)
g_index = torch.tensor(g_index['gamma'].values, dtype=torch.int32, requires_grad=False)
if read_fewer_rows:
    x = x[:fewer_rows,:]
    probs = probs[:fewer_rows,:]
    g_index = g_index[:fewer_rows]
# creating important objectives for the mle
I = x.shape[1]
J = x.shape[0]
g = reindex_g(g_index)
G = torch.max(g)
G_min = torch.min(g)
##### WE NEED TO RESCALE X BECAUSE IT'S CAUSING THE PROBS TO BE INFINITY
torch_check(x)
print(torch.mean(x))
print(torch.std(x))
print(torch.min(x))
print(torch.median(x))
print(torch.max(x))
#print(torch.quantile(x, .25))
#print(torch.quantile(x, .75))
x = x / torch.max(x)
################################
# OPTIMIZATION
################################
# SGD
inv_eta_mayara   = 0.985
inv_theta_mayara = 1.257
theta    = torch.tensor(1/inv_theta_mayara,    requires_grad=True)
eta    = torch.tensor(1/inv_eta_mayara,    requires_grad=True)
adam_lr =  2e-3
optimizer = torch.optim.Adam([theta, eta]) #, lr=adam_lr)
#optimizer = torch.optim.SGD([theta, eta], lr=0.01, momentum=0.9, nesterov=True)
#optimizer = torch.optim.LBFGS([theta, eta], lr=0.01)
tol = .0001
maxiter = 100000
i = 1
converged = 0
theta_prev = theta.detach().clone() + 1
eta_prev    = eta.detach().clone() + 1
obj_function_to_minimize_vec = list()
print("Starting the torch loop")
print("t = ", datetime.now())
while converged == 0 and i<maxiter :
    optimizer.zero_grad() # discards previous computations of gradients                                                                                                   
    obj_function_to_minimize = torch.log(1 + torch.sum(torch.pow(probs - nested_logit_likelihood_probs_full_shortcut(G, I, J, G_min, g, x, theta, eta), 2)))        
    print('Obj func = ', round(obj_function_to_minimize.item(), 5),', step = ', i, ' t =', datetime.now())
    obj_function_to_minimize.backward() # computes the gradient of the function at the current value of the argument                                                                    
    optimizer.step() # updates the argument using gradient descent                                                                                                        
    print('theta = ', round(theta.item(), 5), ', eta = ', round(eta.item(), 5), ", t = ", datetime.now())
    if i % 1:
        obj_function_to_minimize_vec.append(obj_function_to_minimize)
    if torch.max(torch.abs(theta-theta_prev)) <tol and  torch.max(torch.abs(eta-eta_prev)) <tol:
        converged = 1
        print("MLE converged in " + str(i) + " iterations", ', t= ', datetime.now())
    if torch.isnan(obj_function_to_minimize) or torch.isnan(eta) or torch.isnan(theta):
        converged = -1
        print('Obj func = ', round(obj_function_to_minimize.item(), 5),', step = ', i, 'theta = ', round(theta.item(), 5), ', eta = ', round(eta.item(), 5), ", t = ", datetime.now())
        raise ValueError('Likelihood or parameters having NaN on iteration ' + str(i) + ', t= ' + str(datetime.now()))
    theta_prev = theta.detach().clone()
    eta_prev    = eta.detach().clone()
    i += 1
if converged!=1:
    raise RuntimeError('Failed to converge after ' + str(i) + ' iterations', ", t = ", datetime.now())
# end of while
estimates = {}
estimates['theta']       = theta
estimates['eta'] = eta 
estimates['tol'] = tol
estimates['iter'] = i
estimates['adam_lr'] = adam_lr
estimates['obj_func'] = obj_function_to_minimize_vec
pickle.dump(estimates, open('nested_logit_estimates.p', "wb"))
#torch_mle(data_dir)