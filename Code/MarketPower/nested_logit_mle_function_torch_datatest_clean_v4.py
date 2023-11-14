import pandas as pd
import numpy as np
import torch
from datetime import datetime
import os

load_excel = 0
load_excel_with_zeros = 0

simulation = 1 - load_excel
simulation_with_zeros = 1

#########################################################################
# SIMULATION - SETTING THE GROUND TRUTH

if simulation:
    #torch.manual_seed(734)

    theta = torch.tensor(.5, requires_grad=True)
    eta = torch.tensor(1.5, requires_grad=True)

    # Define the dimensions of the matrix
    J = 120  # number of jobs, identified from 0 to J-1
    I = 4  # number of iotas, identified from 0 to I-1
    G = 3  # number of gammas, identified from 0 to G-1
    N = J*1000

    # Generate a random matrix with values between 0 and 1
    x = torch.rand(J, I, requires_grad=False)
    g_index = torch.randint(0, G, (J,), requires_grad=False)
    g_index[:G] = torch.arange(G)


#########################################################################
# LOADING EXCEL DATA
# IMPORTING OUR OWN DATA TO CHECK IF LIKELIHOOD FUNCTIONS ARE CORRECT / IN AGREEMENT WITH THE SPREADSHEET COMPUTATIONS

if load_excel:
    os.chdir(
        '/home/bm/Dropbox (University of Michigan)/_papers/_git/Networks/Code/MarketPower/')

    # LOADING ONE EXAMPLE
    if load_excel_with_zeros:
        data = pd.read_csv('nested_logit_example_zeros.csv')
    else:
        data = pd.read_csv('nested_logit_example.csv')

    theta = torch.tensor(1., requires_grad=True)
    eta = torch.tensor(0., requires_grad=True)

    x = data[['i1', 'i2', 'i3']]
    g_index = data['g'].astype(int)

    # Load your data
    x = torch.tensor(x.values)
    # Convert g to a torch tensor
    g_index = torch.tensor(g_index.values, dtype=torch.int64)


#########################################################################
# FUNCTIONS NEEDED


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

def reindex_g(g):    
    unique_values = torch.unique(g) # Get unique values from the original tensor
    value_to_label = {value.item(): label for label, value in enumerate(unique_values)} # Create a mapping from unique values to labels
    return torch.tensor([value_to_label[value.item()] for value in g])

#########################################################################
# QUICK CHECK DATA PREP

# DATA PREP FOR EFFICIENCY
I = x.shape[1]
J = x.shape[0]

g = reindex_g(g_index)

G = torch.max(g)
G_min = torch.min(g)


#########################################################################
# QUICK CHECK & FINISHING SETTING THE UP THE GROUND TRUTH FOR THE SIMULATION
probs = nested_logit_likelihood_probs(G, I, J, G_min, g, x, theta, eta)
probs
torch.sum(probs, axis=0)


#########################################################################
# SIMULATION: sampling from ground truth

# true values for the parameters
eta0 = eta.detach().clone()
theta0 = theta.detach().clone()

# CHANGING PARAMETERS FROM THE GROUND TRUTH JUST TO SEE IF TORCH WOULD FIND IT
inv_eta_mayara = 0.985
inv_theta_mayara = 1.257
theta = torch.tensor(1/1.257,    requires_grad=True)
eta = torch.tensor(1/0.985,    requires_grad=True)


if simulation:
    # sapmling choices
    choices = torch.empty(J, I, requires_grad=False, dtype=torch.int)
    for i in range(I):
        # Sample N times from the multinomial distribution
        samples = torch.multinomial(probs[:, i], N, replacement=True)
        # Compute the counts for each category
        choices[:, i] = torch.bincount(samples, minlength=len(probs[:, i]))
    
    # observed probs
    x_obs = x * (choices > 0)
    probs_obs = choices / torch.sum(choices, axis=0)
    torch.sum(probs, axis=0)



#########################################################################
# ANOTHER QUICK CHECK

# another quick check
torch.sum(torch.pow(probs - nested_logit_likelihood_probs(G, I, J, G_min, g, x, theta, eta), 2))
torch.sum(torch.pow(probs - nested_logit_likelihood_probs(G, I, J, G_min, g, x, theta0, eta0), 2))

if simulation:
    torch.sum(torch.pow(probs_obs - nested_logit_likelihood_probs(G, I, J, G_min, g, x_obs, theta, eta), 2))
    torch.sum(torch.pow(probs_obs - nested_logit_likelihood_probs(G, I, J, G_min, g, x_obs, theta0, eta0), 2))


#########################################################################
# TORCH IMPLEMENTATION

print("Now running")
start = datetime.now()
print(start)

optimizer = torch.optim.Adam([theta, eta], lr=2e-3)
tol = .0001
maxiter = 10000
iter = 1
converged = 0
theta_prev = theta.detach().clone() + 1
eta_prev = eta.detach().clone() + 1
obj_function_to_minimize_vec = list()
while converged == 0 and iter < maxiter:
    optimizer.zero_grad()  # discards previous computations of gradients

    if simulation:
        if simulation_with_zeros:
            obj_function_to_minimize = torch.log(1 + torch.sum(torch.pow(
                probs_obs - nested_logit_likelihood_probs(G, I, J, G_min, g, x_obs, theta, eta), 2)))
        else:
            obj_function_to_minimize = torch.log(1 + torch.sum(torch.pow(
                probs - nested_logit_likelihood_probs(G, I, J, G_min, g, x, theta, eta), 2)))
    else:
        obj_function_to_minimize = torch.log(1 + torch.sum(torch.pow(probs - nested_logit_likelihood_probs(G, I, J, G_min, g, x, theta, eta), 2)))        
    # computes the gradient of the function at the current value of the argument
    obj_function_to_minimize.backward(retain_graph=True)
    optimizer.step()  # updates the argument using gradient descent
    end = datetime.now()
    print('Obj func = ', round(obj_function_to_minimize.item(), 5),', step = ', iter, ' t =', end - start)
    print('theta = ', round(theta.item(), 5), ', eta = ', round(eta.item(), 5))
    if theta < 0:
        theta = torch.tensor(1/1.257,    requires_grad=True)
    if eta < 0:
        eta = torch.tensor(1/0.985,    requires_grad=True)
    if iter % 10:
        obj_function_to_minimize_vec.append(obj_function_to_minimize)
    if torch.max(torch.abs(theta-theta_prev)) < tol and torch.max(torch.abs(eta-eta_prev)) < tol:
        converged = 1
        print("MLE converged in " + str(iter) + " iterations")
    if torch.isnan(obj_function_to_minimize):
        converged = -1
        raise ValueError('Log-likelihood is NAN on iteration ' + str(maxiter))
    theta_prev = theta.detach().clone()
    eta_prev = eta.detach().clone()
    iter = iter + 1
    # print(iter)
if converged != 1:
    raise RuntimeError('Failed to converge after ' +
                       str(maxiter) + ' iterations')
estimates = {}
estimates['theta'] = theta
estimates['eta'] = eta



final = datetime.now()
print(' ')
print(' ---  ')
print('total time =', final - start)
 