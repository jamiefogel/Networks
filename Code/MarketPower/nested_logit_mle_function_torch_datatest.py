import pandas as pd
import numpy as np
import pickle
import os
import sys
import torch
from datetime import datetime
from scipy.sparse import csr_matrix

#os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/mkt_power/')
os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/_git/Networks/Code/MarketPower/')

# LOADING ONE EXAMPLE
#x, g = pickle.load(open('nested_logit.p', "rb" ))
#n = 10000
#x, g = pd.read_pickle("nested_logit.p")
#g = torch.tensor(g[:n].values, dtype=torch.int32, requires_grad=False)
#x = torch.tensor(x[:n,:].toarray(), requires_grad=False)
#pickle.dump(obj, open('obj_name.p', 'wb'))
#pickle.load(open('obj_name.p', 'rb'))

data = pd.read_csv('nested_logit_example_zeros.csv')
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
        elif i == G_min+1:
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
    term2 = - J * torch.sum(log_or_zero(torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0)))
    return term1 + term2 + term3

# Calculate the nested logit log likelihood
likelihood = nested_logit_log_likelihood(x, g, theta, eta, I, G, J, G_min, g_card)
#print("Nested Logit Log Likelihood:", likelihood.item())


# NESTED LOGIT LOG LIKELIHOOD SUMMING OVER J (this is to include another approach to the MLE problem, which would include the probabilities on the LHS of the logit)
def nested_logit_log_likelihood_overj(x, g, theta, eta, I, G, J, G_min, g_card):
    ###########################
    # Quick data prep
    z = x.pow(1 + eta)
    zgi = collapse_rows(z, g, G, G_min)
    log_zgi = log_or_zero(zgi)
    baseline_mkt_choice = log_or_zero(torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0))

    
    ###########################
    term1 = torch.tensor(0)
    term2 = torch.tensor(0)
    term3 = torch.tensor(0)
    for j in range(z.shape[0]):
        term3 = term3 + torch.sum(log_or_zero(z[j,:]))
        term1 = term1 + ((theta - eta) / (eta + 1)) * torch.sum(log_zgi[g[j]-1,:])
        term2 = term2 - torch.sum(baseline_mkt_choice)
    return term1 + term2 + term3


# Calculate the nested logit log likelihood
likelihood_overj = nested_logit_log_likelihood_overj(x, g, theta, eta, I, G, J, G_min, g_card)
print("Comparison of the two functions:", likelihood.item(), likelihood_overj.item())


def nested_logit_log_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice):
    term3 = log_or_zero(z[j,:])
    term1 = ((theta - eta) / (eta + 1)) * log_zgi[g[j]-1,:]
    term2 = - baseline_mkt_choice
    return term1 + term2 + term3

# NESTED LOGIT LOG LIKELIHOOD SUMMING OVER J (this is to include another approach to the MLE problem, which would include the probabilities on the LHS of the logit)
def nested_logit_log_likelihood_overj2(x, g, theta, eta, I, G, J, G_min, g_card):
    ###########################
    # Quick data prep
    z = x.pow(1 + eta)
    zgi = collapse_rows(z, g, G, G_min)
    log_zgi = log_or_zero(zgi)
    baseline_mkt_choice = log_or_zero(torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0))
    ###########################
    logsum = torch.tensor(0)
    for j in range(z.shape[0]):
        logsum = logsum + torch.sum(nested_logit_log_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice))
    return logsum

nested_logit_log_likelihood_overj2(x, g, theta, eta, I, G, J, G_min, g_card)


z = x.pow(1 + eta)
zgi = collapse_rows(z, g, G, G_min)
log_zgi = log_or_zero(zgi)
baseline_mkt_choice = log_or_zero(torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0))

probs = torch.rand(J, I, requires_grad=False)
for j in range(J):
    probs[j,:] = torch.exp(nested_logit_log_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice))

torch.sum(probs, axis=0)



###############################################################################
###############################################################################
# SIMULATION

#import random
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
J = 20 # number of jobs, identified from 0 to J-1
I = 2 # number of iotas, identified from 0 to I-1
G = 3 # number of gammas, identified from 0 to G-1
N = J*10

# Generate a random matrix with values between 0 and 1
x = torch.rand(J, I, requires_grad=False)
g = torch.randint(0, G, (J,), requires_grad=False)
g[:G] = torch.arange(G)
G_min = torch.min(g)
#unique_g, g_card = torch.unique(g, return_counts=True)
unique_g, g_counts = torch.unique(g, return_counts=True)
g_card = torch.tensor(g_counts.clone().detach(), dtype=torch.float32, requires_grad=False).view(1,G)

theta = torch.tensor(.5, requires_grad=True)
eta = torch.tensor(1.5, requires_grad=True)

z = x.pow(1 + eta)
zgi = collapse_rows(z, g, G, G_min)
log_zgi = log_or_zero(zgi)
baseline_mkt_choice = log_or_zero(torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0))
baseline_mkt_choice_nolog = torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0)

### for the data on the spreadsheet
J = x.shape[0]
I = x.shape[1]
G
G_min = 0


def log_or_zero(matrix):
    return torch.where(matrix != 0, torch.log(matrix), torch.zeros_like(matrix))


def nested_logit_likelihood_perj_ratios(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice):
    num2 = z[j,:]
    denom2 = zgi[g[j]-1,:]
    num1 = zgi[g[j]-1,:].pow((1 + theta) / (1 + eta))
    denom1 = baseline_mkt_choice_nolog
    return [num1/denom1, num2/denom2]


ratio1 = torch.empty(J, I, requires_grad=False)
ratio2 = torch.empty(J, I, requires_grad=False)
for j in range(J):
    temp = nested_logit_likelihood_perj_ratios(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice)
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

choices = torch.empty(J, I, requires_grad=False, dtype=torch.int)
for i in range(I):
    # Sample N times from the multinomial distribution
    samples = torch.multinomial(probs[:,i], N, replacement=True)
    # Compute the counts for each category
    choices[:,i] = torch.bincount(samples, minlength=len(probs[:,i]))

x_obs = x * (choices > 0)
probs_obs = choices / torch.sum(choices, axis=0)

# DATA READY FOR MLE. WE NOW KNOW THETA AND ETA
# X_OBS IS THE AVG WAGES FOR EACH J X I PAIR
# PROBS_OBS IS OBSERVED CHOICES 
probs_obs.requires_grad
x_obs.requires_grad


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
theta    = torch.tensor(1/inv_theta_mayara,    requires_grad=True)
eta    = torch.tensor(1/inv_eta_mayara,    requires_grad=True)

# preprocessing the data
z = x_obs.pow(1 + eta)
zgi = collapse_rows(z, g, G, G_min)
baseline_mkt_choice_nolog = torch.sum(zgi.pow((theta + 1) / (eta + 1)), axis=0)


## adjusted functions for the torch mle
def nested_logit_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice):
    num2 = z[j,:]
    denom2 = zgi[g[j]-1,:]
    num1 = zgi[g[j]-1,:].pow((1 + theta) / (1 + eta))
    denom1 = baseline_mkt_choice_nolog
    return (num1/denom1)*(num2/denom2)

def nested_logit_likelihood(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice, J, probs_obs):
    for j in range(J):
        if j == 0:
            total = torch.sum(nested_logit_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice))
        else:
            total += total
    return total


def nested_logit_likelihood_minus_probs_squared(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice, J, probs_obs):
    for j in range(J):
        if j == 0:
            total = torch.sum(torch.pow(probs_obs[j,:] - nested_logit_likelihood_perj(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice),2))
        else:
            total += total
    return total
            
            
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
tol = .01
maxiter = 10000
iter = 1
converged = 0
theta_prev = theta.detach().clone() + 1
eta_prev    = eta.detach().clone() + 1
obj_function_to_minimize_vec = list()
while converged == 0 and iter<maxiter :
    optimizer.zero_grad() # discards previous computations of gradients                                                                                                   
    obj_function_to_minimize = nested_logit_likelihood_minus_probs_squared(G, G_min, g_card, g, j, z, theta, eta, log_zgi, baseline_mkt_choice, J, probs_obs)
    obj_function_to_minimize.backward(retain_graph=True) # computes the gradient of the function at the current value of the argument                                                                    
    optimizer.step() # updates the argument using gradient descent                                                                                                        
    #print(negloglike)
    end = datetime.now()
    print('Negative log-likelihood = ', obj_function_to_minimize, 'step = ', iter, ' t =',end - start)
    print('theta = ', theta, 'eta = ', eta)
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
    






