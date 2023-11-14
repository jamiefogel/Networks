###############################################################
# LIBRARIES
###############################################################
import numpy as np
import torch

###############################################################
# EQUILIBRIUM FUNCTIONS
###############################################################

def y_s_supply_fun(l_gs_demand, b_gs): # This is the old version of eq 3 with the representative firm
    return torch.prod(l_gs_demand ** b_gs, dim=0)

def y_s_demand_fun(a_s, p_s, eta, Y):
    num = a_s * Y
    denom = p_s ** eta * torch.sum(a_s * p_s ** (1-eta))
    return num / denom

def Y_fun(p_s, y_s_supply):
    return torch.sum(p_s * y_s_supply)

def l_gs_demand_fun(p_s, w_g, b_gs, S): #This is equation 21 
    sumbgs = torch.sum(b_gs, dim=0)
    ratio = torch.transpose(torch.transpose(b_gs,0,1) / w_g, 0, 1)
    return (p_s * (ratio ** (1 - sumbgs)) * torch.reshape(torch.prod(ratio ** b_gs, dim=0),(1,S))) ** (1 / (1 - sumbgs)) #Shouldn't the torch.prod() be length S, not length Gamma?

def l_g_supply_workers_fun(psi, m_i, w_g, xi, nu, xi_outopt, phi_outopt): #Unnumbered equation between eq'ns 6 and 7
    phi = w_g * psi
    phi_full = torch.cat((phi_outopt, phi), 1)
    xi_full = torch.cat((xi_outopt, xi), 0)
    avg_utility = (phi_full + xi_full) / nu
    p_ig = torch.exp(avg_utility) / torch.reshape(torch.sum(torch.exp(avg_utility), dim=1),(m_i.shape[0],1))
    workers_allocation = p_ig * m_i
    return workers_allocation

def l_g_supply_fun(psi, m_i, w_g, xi, nu, xi_outopt, phi_outopt, G): # Equation 7
    workers_allocation = l_g_supply_workers_fun(psi, m_i, w_g, xi, nu, xi_outopt, phi_outopt)
    return torch.sum(workers_allocation[:, 1:G+1] * psi, dim=0)

def obj_fun(w_g, p_s, m_i, a_s, eta, b_gs, G, S, nu, psi, xi, xi_outopt, phi_outopt):
    
    l_g_supply = l_g_supply_fun(psi, m_i, w_g, xi, nu, xi_outopt, phi_outopt, G)
    l_gs_demand = l_gs_demand_fun(p_s, w_g, b_gs, S)
    y_s_supply = y_s_supply_fun(l_gs_demand, b_gs)
    Y = Y_fun(p_s, y_s_supply)
    y_s_demand = y_s_demand_fun(a_s, p_s, eta, Y)    
    prod_eq = torch.sum(abs(y_s_supply - y_s_demand))
    labor_eq = torch.sum(abs(l_g_supply - torch.sum(l_gs_demand, dim=1)))
    return prod_eq + labor_eq



###############################################################
# FIXED POINT SOLUTION FUNCTION
###############################################################


def solve_model(eta,
                I,
                G,
                S,
                a_s,
                b_gs,
                m_i,
                nu_hat,
                sigma_hat,
                xi_hat,
                xi_outopt,
                phi_outopt,
                psi_hat,
                maxiter = 1e6,  # maximum number of iterations
                factor = 1e-1,  # dampening factor
                tol = 1e-4,     # precision level in the model solution
                decimals = 4,   # printed output rounding decimals
                silent = 1,
                w0 = 0         # initial vector of wages (If we want to specify a specific vector for initial wages, set it equal to something different than zero)
                ):
    
    # initial values
    #w_g = torch.ones(G, requires_grad=False) # these high wages to start with were breaking the labor suply in the exponentials, when psi is high due to the outside option k normalization. This would blow up: exp(wages*psi)
    
    if  isinstance(w0, int) and w0 ==0:
        w_g = torch.tensor([.01]*G, requires_grad=False)    
    else:
        w_g = w0
    
    p_s = torch.ones(S, requires_grad=False)

    
    converged = 0
    count = 1
    equi = 1.

    
    #while converged == 0 and count < maxiter and equi > 0.0091 :
    while converged == 0 and count < maxiter:
        count += 1
        w_g0 = w_g.clone().detach()
        p_s0 = p_s.clone().detach()
        l_g_supply = l_g_supply_fun(psi_hat, m_i, w_g, xi_hat, nu_hat, xi_outopt, phi_outopt, G)
        l_gs_demand = l_gs_demand_fun(p_s, w_g, b_gs, S)
        l_g_demand = torch.sum(l_gs_demand, dim=1)
        y_s_supply = y_s_supply_fun(l_gs_demand, b_gs)
        Y = Y_fun(p_s, y_s_supply)
        y_s_demand = y_s_demand_fun(a_s, p_s, eta, Y)    
        p_s = p_s0 * (y_s_demand / y_s_supply)**factor
        w_g = w_g0 * (l_g_demand / l_g_supply)**factor
        equi = obj_fun(w_g, p_s, m_i, a_s, eta, b_gs, G, S, nu_hat, psi_hat, xi_hat, xi_outopt, phi_outopt)
        
        if silent == 0:
            if count % 10 == 0:
                print(' ')
                print('Iteration = ' + str(count))
                print('Total absolute supply x demand mismatch is ' + str(np.round(equi.data.numpy(), decimals)))
                print(np.round(w_g.data.numpy(), decimals))
                print(np.round(p_s.data.numpy(), decimals))
            if count % 1e2 == 0:
                print('iteration = ' + str(count))
        if equi < tol:
            converged = 1
            print(' ')
            print('CONVERGED!')
            if silent == 0:
                print('Total absolute supply x demand mismatch is ' + str(equi))
                print('It took ' + str(count) + ' iterations.')
    
    l_g_supply = l_g_supply_fun(psi_hat, m_i, w_g, xi_hat, nu_hat, xi_outopt, phi_outopt, G)
    l_gs_demand = l_gs_demand_fun(p_s, w_g, b_gs, S)
    l_g_demand = torch.sum(l_gs_demand, dim=1)
    y_s_supply = y_s_supply_fun(l_gs_demand, b_gs)
    Y = Y_fun(p_s, y_s_supply)
    y_s_demand = y_s_demand_fun(a_s, p_s, eta, Y)    
    
    LS = l_g_supply.data.numpy()
    LD = l_g_demand.data.numpy()
    YS = y_s_supply.data.numpy()
    YD = y_s_demand.data.numpy()
    
    if silent == 0:
        print(' ')
        print('Labor supply, labor demand, Excess Labor supply')
        print(np.round(LS,decimals))
        print(np.round(LD,decimals))
        print(np.round(LS-LD,decimals))
    
        print(' ')
        print('Prodcut supply, product demand, Excess Product supply')    
        print(np.round(YS,decimals))
        print(np.round(YD,decimals))
        print(np.round(YS-YD,decimals))
    
    worker_job_type_allocation = l_g_supply_workers_fun(psi_hat, m_i, w_g, xi_hat, nu_hat, xi_outopt, phi_outopt)
    
    if silent == 0:
        print(' ')
        print('Worker-Job Type Allocation (includes the outside option)')    
        print(np.round(worker_job_type_allocation,decimals))
    
    # Compute probability of matching with type gamma conditional on being type iota
    p_ig = worker_job_type_allocation/torch.reshape(torch.sum(worker_job_type_allocation,dim=1),(I,1))
    # Restrict to employed workers and re-scale so that probabilities are conditional on employment. 
    p_ig_employed = p_ig[:,1:] / torch.reshape(1-p_ig[:,0],(I,1))

    
    equilibrium = {
            'p_s': p_s,
            'w_g': w_g,
            'l_g_supply': l_g_supply,
            'l_gs_demand': l_gs_demand,
            'l_g_demand': l_g_demand,
            'y_s_supply': y_s_supply,
            'y_s_demand': y_s_demand,
            'y_s': (y_s_supply + y_s_demand) / 2,
            'l_g': (l_g_supply + l_g_demand) / 2,
            'worker_job_type_allocation': worker_job_type_allocation,
            'p_ig': p_ig,
            'p_ig_employed': p_ig_employed,
            'Y' : Y
            }
    
    return equilibrium
