#--------------------------------------
# GENERALIZATION OF GRIGSBY'S NORMALIZATION
#--------------------------------------
import pickle
import torch
import pandas as pd
import numpy as np

from solve_model_functions import *


# Generalized Grigsby's normalization - V1
def generalized_grigsby_norm(m_i, phi, k, I):
    print(phi.shape)
    w = torch.sum(torch.reshape(m_i, (I, 1)) * phi, dim = 0) * (1/k)
    psi = phi / w
    return (psi, w)

def normalization_k(savefile,  worker_type_var, job_type_var, mle_estimates, mle_data_sums, S, a_s, b_gs, eta, phi_outopt_scalar, xi_outopt_scalar, level, year, raw_data_file = "~/Networks/RAIS_exports/earnings_panel/mle_data_long_level_0.csv"):
    
    a_s_variation = True #I'm just ignoring the case where we use p_s for now. Should be easy to change the function arguments if we want it.
    
    phi_outopt = torch.reshape(torch.tensor([phi_outopt_scalar]*mle_data_sums['I'], requires_grad=False), (mle_data_sums['I'],1))
    xi_outopt = torch.tensor([xi_outopt_scalar], requires_grad=False)

    # This section is just computing actual nonemployment rate 
    data_full = pd.read_csv(raw_data_file)    
    
    if worker_type_var=='1':
        data_full['worker_type'] = 1
        data_full['worker_type'].loc[data_full['iota']==-1] = -1
    else: 
        data_full['worker_type'] = data_full[worker_type_var]
        
    if job_type_var=='1':
        data_full['job_type'] = 1
        data_full['job_type'].loc[data_full['gamma']== 0] = 0
        data_full['job_type'].loc[data_full['gamma']==-1] = -1
    else:
        data_full['job_type']    = data_full[job_type_var]
        
    data_full.loc[data_full['gamma']==0, 'job_type'] = 0 #Make sure the job type is set to 0 for non-employed
    data_full_levels = data_full[(data_full['gamma']!=-1) & (data_full['iota']!=-1)]
    data_full_levels['count']    = 1
    data_levels = data_full_levels.groupby(['worker_type','job_type','year'])['count'].count().reset_index()
    del data_full
    temp = data_levels.pivot_table(index=["worker_type", "year"], columns='job_type', values='count').reset_index(level='year')
    pickle.dump(temp, open('pivot_pig_by_year', 'wb'))
    sum_count_ig    = temp[temp['year'] == year].drop(columns='year').values
    sum_count_ig    = np.nan_to_num(sum_count_ig)
    p_ig_actual     = sum_count_ig / sum_count_ig.sum()
    actual_outopt_avg_prop = p_ig_actual[:,0].sum()

    
    # Setting a start value for the check variable
    outopt_avg_prop = max(actual_outopt_avg_prop, .99)
    
    # Set optimization parameters
    #k_init = 1000000    # Initial choice of k
    k_init = 7    # Initial choice of k
    tol_k = .001
    maxiter_k = 1e3
    count_k = 1
    factor_k = .5 #I changed this from 1e0 because it seemed to be stuck in a loop and not converging
    #factor_k=5
    
    k = k_init
    
    while abs(actual_outopt_avg_prop - outopt_avg_prop) > tol_k and count_k < maxiter_k:
        
        count_k += 1
        print('k = ' + str(k))
        # Find psi given current k
        psi_hat = generalized_grigsby_norm(mle_data_sums['m_i'], mle_estimates['phi_hat'], k, mle_data_sums['I'])[0]
        # Solve model using psi_hat corresponding to current k
        if a_s_variation == True:
            print('Solving model for k= ' + str(k))
            equi = solve_model(eta,
                        mle_data_sums['I'],
                        mle_data_sums['G'],
                        S,
                        a_s,
                        b_gs,
                        mle_data_sums['m_i'],
                        nu_hat = mle_estimates['nu_hat'],
                        sigma_hat = mle_estimates['sigma_hat'],
                        xi_hat = mle_estimates['xi_hat'],
                        xi_outopt = xi_outopt,
                        phi_outopt = phi_outopt,
                        psi_hat = psi_hat,
                        maxiter = 1e6,  # maximum number of iterations
                        factor = 1e-3,  # dampening factor
                        tol = 1e-4,     # precision level in the model solution
                        decimals = 4,   # printed output rounding decimals
                        silent = 1
                        )
        else:
            p_s = torch.tensor(np.array(p_ts.iloc[p_ts.index == year,:]))
            equi = solve_model(p_s,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                b_gs,
                mle_data_sums['m_i'],
                nu_hat = mle_estimates['nu_hat'],
                sigma_hat = mle_estimates['sigma_hat'],
                xi_hat = mle_estimates['xi_hat'],
                xi_outopt = xi_outopt,
                phi_outopt = phi_outopt,
                psi_hat = psi_hat,
                maxiter = 1e6,  # maximum number of iterations
                factor = 1e-3,  # dampening factor
                tol = 1e-4,     # precision level in the model solution
                decimals = 4,   # printed output rounding decimals
                silent = solve_GE_silently
                )
            
            # The idea here is that we pick a k, use it to extract a psi, solve the model and compare the model-implied overall (averaged over all types) nonemployment rate to the actual nonemployment rate. We then keep choosing different values of k until we can match the overall nonemployment rate. We do this because the choice of k is not WLOG because the value of k has implications for the total supply of skills in the economy, which affects wages and thus employment (see MiscellaneousNotes 1/14/2021).
                
        outopt_avg_prop = torch.sum(equi['worker_job_type_allocation'], dim=0)
        outopt_avg_prop = outopt_avg_prop[0]
        k = k * (outopt_avg_prop / actual_outopt_avg_prop)**factor_k
        print(' ')
        print(outopt_avg_prop)
        print(actual_outopt_avg_prop)
    
    print('Final value for k = ' + str(k))
    psi_hat = generalized_grigsby_norm(mle_data_sums['m_i'], mle_estimates['phi_hat'], k, mle_data_sums['I'])[0]
    psi_and_k = {
            'psi_hat': psi_hat,
            'k': k,
            'k_init': k_init,
            'tol_k': tol_k,
            'count_k': count_k,
            'factor_k': factor_k
            }
    pickle.dump(psi_and_k, open(savefile, "wb"))
    

 