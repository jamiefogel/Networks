"""
Simplified General Equilibrium Model Solver for Table 5 Replication
Contains only the essential functions needed to solve the model for wage predictions
"""

import numpy as np
import torch

###############################################################
# EQUILIBRIUM FUNCTIONS
###############################################################

def y_s_supply_fun(l_gs_demand, b_gs):
    """Sector output supply function (Equation 3)"""
    return torch.prod(l_gs_demand ** b_gs, dim=0)

def y_s_demand_fun(a_s, p_s, eta, Y):
    """Sector output demand function"""
    num = a_s * Y
    denom = p_s ** eta * torch.sum(a_s * p_s ** (1-eta))
    return num / denom

def Y_fun(p_s, y_s_supply):
    """Total output function"""
    return torch.sum(p_s * y_s_supply)

def l_gs_demand_fun(p_s, w_g, b_gs, S):
    """Labor demand by (market, sector) - Equation 21"""
    sumbgs = torch.sum(b_gs, dim=0)
    ratio = torch.transpose(torch.transpose(b_gs,0,1) / w_g, 0, 1)
    return (p_s * (ratio ** (1 - sumbgs)) * torch.reshape(torch.prod(ratio ** b_gs, dim=0),(1,S))) ** (1 / (1 - sumbgs))

def l_g_supply_workers_fun(psi, m_i, w_g, xi, nu, xi_outopt, phi_outopt):
    """Worker allocation across markets (between equations 6 and 7)"""
    phi = w_g * psi
    phi_full = torch.cat((phi_outopt, phi), 1)
    xi_full = torch.cat((xi_outopt, xi), 0)
    avg_utility = (phi_full + xi_full) / nu
    p_ig = torch.exp(avg_utility) / torch.reshape(torch.sum(torch.exp(avg_utility), dim=1),(m_i.shape[0],1))
    workers_allocation = p_ig * m_i
    return workers_allocation

def l_g_supply_fun(psi, m_i, w_g, xi, nu, xi_outopt, phi_outopt, G):
    """Labor supply by market (Equation 7)"""
    workers_allocation = l_g_supply_workers_fun(psi, m_i, w_g, xi, nu, xi_outopt, phi_outopt)
    return torch.sum(workers_allocation[:, 1:G+1] * psi, dim=0)

def obj_fun(w_g, p_s, m_i, a_s, eta, b_gs, G, S, nu, psi, xi, xi_outopt, phi_outopt):
    """Objective function measuring equilibrium distance"""
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
                silent = True,  # Default to silent for Table 5 replication
                w0 = 0         # initial vector of wages
                ):
    """
    Solve the general equilibrium model

    Returns equilibrium wages, prices, and allocations needed for Table 5
    """

    # Initial values
    if isinstance(w0, int) and w0 == 0:
        w_g = torch.tensor([.01]*G, requires_grad=False)
    else:
        w_g = w0

    p_s = torch.ones(S, requires_grad=False)

    converged = 0
    count = 1
    equi = 1.

    # Fixed point iteration
    while converged == 0 and count < maxiter:
        count += 1
        w_g0 = w_g.clone().detach()
        p_s0 = p_s.clone().detach()

        # Compute supplies and demands
        l_g_supply = l_g_supply_fun(psi_hat, m_i, w_g, xi_hat, nu_hat, xi_outopt, phi_outopt, G)
        l_gs_demand = l_gs_demand_fun(p_s, w_g, b_gs, S)
        l_g_demand = torch.sum(l_gs_demand, dim=1)
        y_s_supply = y_s_supply_fun(l_gs_demand, b_gs)
        Y = Y_fun(p_s, y_s_supply)
        y_s_demand = y_s_demand_fun(a_s, p_s, eta, Y)

        # Update prices and wages
        p_s = p_s0 * (y_s_demand / y_s_supply)**factor
        w_g = w_g0 * (l_g_demand / l_g_supply)**factor

        # Check convergence
        equi = obj_fun(w_g, p_s, m_i, a_s, eta, b_gs, G, S, nu_hat, psi_hat, xi_hat, xi_outopt, phi_outopt)

        if not silent:
            if count % 10 == 0:
                print(f'Iteration = {count}')
                print(f'Total absolute supply x demand mismatch = {np.round(equi.data.numpy(), decimals)}')
                print(f'Wages: {np.round(w_g.data.numpy(), decimals)}')
                print(f'Prices: {np.round(p_s.data.numpy(), decimals)}')

        if equi < tol:
            converged = 1
            if not silent:
                print('CONVERGED!')
                print(f'Total absolute supply x demand mismatch = {equi}')
                print(f'Iterations: {count}')

    if count >= maxiter and not silent:
        print(f'Warning: Maximum iterations ({maxiter}) reached without convergence')

    # Final equilibrium values
    l_g_supply = l_g_supply_fun(psi_hat, m_i, w_g, xi_hat, nu_hat, xi_outopt, phi_outopt, G)
    l_gs_demand = l_gs_demand_fun(p_s, w_g, b_gs, S)
    l_g_demand = torch.sum(l_gs_demand, dim=1)
    y_s_supply = y_s_supply_fun(l_gs_demand, b_gs)
    Y = Y_fun(p_s, y_s_supply)
    y_s_demand = y_s_demand_fun(a_s, p_s, eta, Y)

    # Worker allocation and choice probabilities
    worker_job_type_allocation = l_g_supply_workers_fun(psi_hat, m_i, w_g, xi_hat, nu_hat, xi_outopt, phi_outopt)

    # Probability of matching with market gamma conditional on being worker type iota
    p_ig = worker_job_type_allocation/torch.reshape(torch.sum(worker_job_type_allocation,dim=1),(I,1))

    # Restrict to employed workers and re-scale conditional on employment
    p_ig_employed = p_ig[:,1:] / torch.reshape(1-p_ig[:,0],(I,1))

    equilibrium = {
        'p_s': p_s,                                    # Sector prices
        'w_g': w_g,                                    # Market wages
        'l_g_supply': l_g_supply,                      # Labor supply by market
        'l_gs_demand': l_gs_demand,                    # Labor demand by (market, sector)
        'l_g_demand': l_g_demand,                      # Total labor demand by market
        'y_s_supply': y_s_supply,                      # Sector output supply
        'y_s_demand': y_s_demand,                      # Sector output demand
        'y_s': (y_s_supply + y_s_demand) / 2,         # Average sector output
        'l_g': (l_g_supply + l_g_demand) / 2,         # Average labor by market
        'worker_job_type_allocation': worker_job_type_allocation,  # Worker allocation including outside option
        'p_ig': p_ig,                                  # Choice probabilities (including outside option)
        'p_ig_employed': p_ig_employed,                # Choice probabilities conditional on employment
        'Y': Y,                                        # Total output
        'converged': converged,                        # Convergence flag
        'iterations': count                            # Number of iterations
    }

    return equilibrium