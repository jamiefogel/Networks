import pandas as pd
import numpy as np
import torch
from datetime import datetime

###############################################################################
# SIMULATION
torch.manual_seed(734)

def collapse_rows(z, g, G, G_min):
    for i in range(G_min, G):
        z_sum = torch.sum(z[g == i], dim=0)
        if i == G_min:
            zgi = z_sum
        elif i == G_min+1:
            zgi = torch.cat((zgi.unsqueeze(0), z_sum.unsqueeze(0)), dim=0)        
        else:
            zgi = torch.cat((zgi, z_sum.unsqueeze(0)), dim=0)
    return zgi

# Define the dimensions of the matrix
J = 10 # number of jobs, identified from 0 to J-1
I = 2 # number of iotas, identified from 0 to I-1
G = 3 # number of gammas, identified from 0 to G-1
N = J*1000

# Generate a random matrix with values between 0 and 1
x = torch.rand(J, I, requires_grad=False)
g = torch.randint(0, G, (J,), requires_grad=False)
g[:G] = torch.arange(G)
G_min = torch.min(g)
unique_g, g_counts = torch.unique(g, return_counts=True)
g_card = torch.tensor(g_counts.clone().detach(), dtype=torch.float32, requires_grad=False).view(1,G)

theta = torch.tensor(.5, requires_grad=True)
eta = torch.tensor(1.5, requires_grad=True)

z = x.pow(1 + eta)
zgi = collapse_rows(z, g, G, G_min)
baseline_mkt_choice_nolog = torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0)

def nested_logit_likelihood_perj_ratios(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog):
    num2 = z[j,:]
    denom2 = zgi[g[j]-1,:]
    num1 = zgi[g[j]-1,:].pow((1 + theta) / (1 + eta))
    denom1 = baseline_mkt_choice_nolog
    return [num1/denom1, num2/denom2]

def nested_logit_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog):
    num2 = z[j,:]
    denom2 = zgi[g[j]-1,:]
    num1 = zgi[g[j]-1,:].pow((1 + theta) / (1 + eta))
    denom1 = baseline_mkt_choice_nolog
    return (num1/denom1)*(num2/denom2)

def nested_logit_likelihood_minus_probs_squared(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog, J, probs_obs):
    for j in range(J):
        if j == 0:
            total = torch.sum(torch.pow(probs_obs[j,:] - nested_logit_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog),2))
        else:
            total += total
    return total

def nested_logit_log_likelihood(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog, J, probs_obs):
    for j in range(J):
        if j == 0:
            total = torch.sum(torch.log(nested_logit_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog)))
        else:
            total += total
    return total

ratio1 = torch.empty(J, I, requires_grad=False)
ratio2 = torch.empty(J, I, requires_grad=False)
for j in range(J):
    temp = nested_logit_likelihood_perj_ratios(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog)
    ratio1[j,:] = temp[0]
    ratio2[j,:] = temp[1]

ratio2 = torch.where(ratio2 >0, ratio2, torch.zeros_like(ratio2))
ratio1
ratio2
torch.sum(ratio1, axis=0)
torch.sum(ratio2, axis=0)

probs_unadj = ratio1 * ratio2
probs = probs_unadj / torch.sum(probs_unadj, axis=0)

torch.sum(probs, axis=0)

nested_logit_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog)

### TO DO XXBM:
    # check why this below isn't zero. it has to be zero, otherwise the torch won't work indeed. should start the torch without simulating 1st
    # check if the function I have is working with the spreadsheet example. for that, construct the function that will give the probs vectors like above, to compare them with the spreadsheet
    # why this line? ratio2 = torch.where(ratio2 >0, ratio2, torch.zeros_like(ratio2))
    # 
nested_logit_likelihood_minus_probs_squared(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog, J, probs)



choices = torch.empty(J, I, requires_grad=False, dtype=torch.int)
for i in range(I):
    # Sample N times from the multinomial distribution
    samples = torch.multinomial(probs[:,i], N, replacement=True)
    # Compute the counts for each category
    choices[:,i] = torch.bincount(samples, minlength=len(probs[:,i]))

x_obs = x * (choices > 0)
probs_obs = choices / torch.sum(choices, axis=0)
probs_obs.requires_grad
x_obs.requires_grad

##############################################
# FINAL DATA
probs_obs
x_obs
I
G
g
J
choices

# true values for the parameters
eta0 = eta.detach().clone()
theta0 = theta.detach().clone()

# SGD
inv_eta_mayara   = 0.985
inv_theta_mayara = 1.257
theta    = torch.tensor(1/1.257,    requires_grad=True)
eta    = torch.tensor(1/0.985,    requires_grad=True)
            
#############################################
# TORCH IMPLEMENTATION

# CHECKS: 
    # (1) does g have all integers between G_min and G, including these values?
    # (2) 

print("Now running")
start = datetime.now()
print(start)
################################
# LOAD AND PREP DATA
################################
unique_g, g_counts = torch.unique(g, return_counts=True)
g_card = g_counts.view(1,G).to(torch.float64)
G = torch.max(g)
G_min = torch.min(g)
################################
# OPTIMIZATION
################################

optimizer = torch.optim.Adam([theta, eta], lr=2e-3)
tol = .0001
maxiter = 10000
iter = 1
converged = 0
theta_prev = theta.detach().clone() + 1
eta_prev    = eta.detach().clone() + 1
obj_function_to_minimize_vec = list()
while converged == 0 and iter<maxiter :
    z = x_obs.pow(1 + eta)
    z = x.pow(1 + eta)
    zgi = collapse_rows(z, g, G, G_min)
    baseline_mkt_choice_nolog = torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0)

    optimizer.zero_grad() # discards previous computations of gradients                                                                                                   
    obj_function_to_minimize = torch.log(nested_logit_likelihood_minus_probs_squared(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog, J, probs_obs))
    #obj_function_to_minimize = nested_logit_log_likelihood(G, G_min, g_card, g, j, z, theta, eta, baseline_mkt_choice_nolog, J, probs_obs)
    obj_function_to_minimize.backward(retain_graph=True) # computes the gradient of the function at the current value of the argument                                                                    
    optimizer.step() # updates the argument using gradient descent                                                                                                        
    #print(negloglike)
    end = datetime.now()
    print('Obj func = ', round(obj_function_to_minimize.item(),5), ', step = ', iter, ' t =',end - start)
    print('theta = ', round(theta.item(),5), ', eta = ', round(eta.item(),5))
    if theta < 0:
        theta    = torch.tensor(1/1.257,    requires_grad=True)
    if eta < 0:
        eta    = torch.tensor(1/0.985,    requires_grad=True)
    if iter % 10:
        obj_function_to_minimize_vec.append(obj_function_to_minimize)
    if torch.max(torch.abs(theta-theta_prev)) <tol and  torch.max(torch.abs(eta-eta_prev)) <tol:
        converged = 1
        print("MLE converged in " + str(iter) + " iterations")
    if torch.isnan(obj_function_to_minimize):
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
    






