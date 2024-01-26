# srun --pty --account=lsa1 /bin/bash  
# module load python3.6-anaconda

import pandas as pd
import numpy as np
import pickle
import os
import sys
import torch


def torch_mle(mle_sums_data, estimates_file, worker_type_var='', job_type_var='', level='', c_bump=.2, count_bump=.2):
    '''
    - move run_query_sums/mle_load_fulldata_vs outside torch_mle
    - Make torch_mle a function
    - Take arguments  saved_mle_sums, estimates_savefile
    '''
    # We thought that what matters is c_bump*count_bump, not each value individually. But when we changed this to c_bump=1 and count_bump=.01 torch couldn't solve the model. I don't understand what's going on but haven't dug very deep. 
    #omega_bump = .1
    
    
    # Somehow Torch hasnt defined pi
    torch.pi = torch.tensor(np.pi)
    
    
    print("Now running")
    print("Level:", level)
    print("Worker type variable:", worker_type_var)
    print("Job type variable:", job_type_var)
    
    # Define outside options
    phi_outopt_scalar = torch.zeros(1, requires_grad=False)
    xi_outopt =  torch.zeros(1, requires_grad=False)
    
    
    # Load data
    mle_data = pickle.load(open(mle_sums_data, "rb"), encoding='bytes')
    print(mle_data.keys())
    
    # Define a couple necessary objects
    phi_outopt = np.reshape(np.repeat(phi_outopt_scalar, mle_data['I']), (-1,1))
    
    # INITIAL VALUES FOR PARAMETERS
    
    nu0 = 1.
    sigma0 = np.maximum(np.sqrt(mle_data['sum_logomega_sq'] / mle_data['obs_employ'] - np.power(mle_data['sum_logomega'] / mle_data['obs_employ'],2)),1e-2)
    xi0 = np.repeat(-1., mle_data['G'])
    phi0 = np.maximum((mle_data['sum_omega_ig'][:,1:] / mle_data['obs_employ']),1e-5)
    
    
    
    # Adjustments for sparsity
    # I checked that at level 0 there are 37,581 non-zero cells and 5,043 zero cells
    # Whenever there are zero matches, sum_logomega_ig=0 as well. This is because we convert nans to 0 in mle_load_fulldata.py
    
    mle_data['sum_c_ig'][mle_data['sum_c_ig']==0]            = c_bump
    mle_data['sum_count_ig'][mle_data['sum_count_ig']==0]    = count_bump
    #mle_data['sum_logomega_ig'][mle_data['sum_logomega_ig']==0] = omega_bump
    
    
    
    ################################
    # LOG-LIKELIHOOD FUNCTION
    ################################
    
    def loglike_sums(sigma_par,nu_par,xi_par,phi_par, mle_data, phi_outopt, xi_outopt):
        phi_norm = torch.cat((phi_outopt,phi_par),axis=1)
        xi_norm = torch.cat((xi_outopt, xi_par), axis=0)
        avgutilitystd = (phi_norm + xi_norm) / nu_par 
        pgi = torch.transpose(torch.div(torch.transpose(torch.exp(avgutilitystd),0,1), torch.sum(torch.exp(avgutilitystd), dim=1)),0,1)
        term_choice = (torch.log(pgi) * mle_data['sum_c_ig']).sum()
        term_earnings = -( mle_data['obs_employ'] * torch.log(sigma_par * torch.sqrt(2*torch.pi)) + mle_data['sum_logomega'] + (1/(2*torch.pow(sigma_par,2))) * \
        (mle_data['sum_logomega_sq'] - 2*(torch.log(phi_par) * mle_data['sum_logomega_ig']).sum() + ( torch.pow(torch.log(phi_par),2) * mle_data['sum_count_ig'][:,1:]).sum()))
        return term_choice + term_earnings

    # This is the problem we're having with ('occ2Xmeso_first_recode', 'gamma')
    #>>> mle_data['sum_count_ig'][:,1:].shape
    #torch.Size([1287, 1371])
    #>>> mle_data['sum_logomega_ig'].shape
    #torch.Size([1285, 1371])
    
    
    # SGD
    
    nu_hat    = torch.tensor(nu0,    requires_grad=True)
    sigma_hat = sigma0.clone().detach().requires_grad_(True)
    xi_hat    = torch.tensor(xi0,    requires_grad=True)
    phi_hat   = phi0.clone().detach().requires_grad_(True)  
    
    optimizer = torch.optim.Adam([sigma_hat, nu_hat, xi_hat, phi_hat], lr=2e-3)
    
    converged = 0
    if level==3:
        tol = .00001
    if level<3:
        tol = .0001
    maxiter = 100000
    iter = 1
    
    sigma_hat_prev = sigma_hat.detach().clone() + 100
    nu_hat_prev    = nu_hat.detach().clone() + 100
    xi_hat_prev    = xi_hat.detach().clone() + 100
    phi_hat_prev   = phi_hat.detach().clone() + 100
    negloglike_vec = list()
    
    while converged == 0 and iter<maxiter :
        optimizer.zero_grad() # discards previous computations of gradients                                                                                                   
        negloglike = -loglike_sums(sigma_hat,nu_hat,xi_hat,phi_hat, mle_data, phi_outopt, xi_outopt)
        negloglike.backward() # computes the gradient of the function at the current value of the argument                                                                    
        optimizer.step() # updates the argument using gradient descent                                                                                                        
        #print(negloglike)
        if iter % 5000:
            negloglike_vec.append(negloglike)
        if torch.max(torch.abs(nu_hat-nu_hat_prev)) <tol and  torch.max(torch.abs(sigma_hat-sigma_hat_prev)) <tol and torch.max(torch.abs(xi_hat-xi_hat_prev)) <tol and torch.max(torch.abs(phi_hat-phi_hat_prev)) <tol:
            converged = 1
            print("MLE converged in " + str(iter) + " iterations")
        if torch.isnan(negloglike):
            converged = -1
            raise ValueError('Log-likelihood is NAN on iteration ' + str(maxiter))
        sigma_hat_prev = sigma_hat.detach().clone()
        nu_hat_prev    = nu_hat.detach().clone()
        xi_hat_prev    = xi_hat.detach().clone()
        phi_hat_prev   = phi_hat.detach().clone()
        iter = iter + 1
        #print(iter)
    
    if converged!=1:
        raise RuntimeError('Failed to converge after ' + str(maxiter) + ' iterations')
        
    
    estimates = {}
    estimates['level']       = level
    estimates['worker_type'] = worker_type_var
    estimates['job_type']    = job_type_var
    estimates['xi_hat']      = xi_hat.clone().detach().requires_grad_(False)
    estimates['phi_hat']     = phi_hat.clone().detach().requires_grad_(False)
    estimates['nu_hat']      = nu_hat.clone().detach().requires_grad_(False)
    estimates['sigma_hat']   = sigma_hat.clone().detach().requires_grad_(False)
    estimates['count_bump']  = count_bump
    estimates['c_bump']      = c_bump
    
    pickle.dump(estimates, open(estimates_file, "wb"))
    

    
    
    
    







# Old style for outputting results. Replaced with the pickle above
#for e in estimates:
#    print(e)
#    print(estimates[e])
#    outfile_p   = homedir + "/Networks/Code/mar2021/mle_estimates/" + e + "_level_" + str(level) + "_" + label + ".p"
#    outfile_csv = homedir + "/Networks/Code/mar2021/mle_estimates/" + e + "_level_" + str(level) + "_" + label + ".csv"
#    if estimates[e].detach().numpy().shape == ():
#        pickle.dump(estimates[e].detach().numpy().reshape((1,)), open(outfile_p, "wb"))
#        np.savetxt(outfile_csv, estimates[e].detach().numpy().reshape((1,)), delimiter=',')
#    else:
#        pickle.dump(estimates[e].detach().numpy(), open(outfile_p, "wb"))
#        np.savetxt(outfile_csv, estimates[e].detach().numpy(), delimiter=',')
#


# This was our attempt to do gradient descent manually. It didn't work.
# Gradient Descent
#
#steps = 500
#step_size = 0.02
#
#for _ in range(steps):
#    negloglike = -loglike_sums(sigma_hat,nu_hat,xi_hat,phi_hat, mle_data, phi_outopt, xi_outopt)
#    negloglike.backward()
#    with torch.no_grad():
#        nu_hat -= step_size * nu_hat.grad
#        sigma_hat -= step_size * sigma_hat.grad
#        xi_hat -= step_size * xi_hat.grad
#        phi_hat -= step_size * phi_hat.grad
#        nu_hat.grad.zero_()
#        sigma_hat.grad.zero_()
#        xi_hat.grad.zero_()
#        phi_hat.grad.zero_()
#    print(negloglike)




