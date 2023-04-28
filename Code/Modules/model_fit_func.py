#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 12:26:39 2021

@author: jfogel
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch
import statsmodels.api as sm
from statsmodels.formula.api import ols
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import shutil


from solve_model_functions import *


plt.rcParams.update({
    "text.usetex": True,
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica"]})

# Imoved these two functions out of model_fit() on 8/23/2021. Need to test that this doesn't break anything. 

def compute_expected_Phi(p_ig, psi, w_g, sigma_hat):
    return torch.sum( p_ig * (psi * w_g * torch.exp(sigma_hat**2/2)), dim=1) 

def compute_expected_U(p_ig, psi, xi, nu, w_g):
    phi_plus_xi = psi * w_g + xi
    E_phi_xi =  torch.sum( p_ig * phi_plus_xi, dim=1)
    entropy  = -torch.sum( p_ig * torch.log(p_ig),dim=1)
    E_U      = E_phi_xi + nu * entropy
    return E_U
    
def model_fit(mle_data_filename, mle_data_sums, mle_estimates, psi_hat, S, eta, level, b_gs, worker_type_var, job_type_var, a_ts, p_ts, figuredir, phi_outopt_scalar=0, xi_outopt_scalar=0):
    
    

    
    def make_scatter(x, y, x_str, y_str, xlabel, ylabel, xlim, ylim, title='', filename='', printvars=True, cmap=None, c=None):

        wgt = mle_data_sums['m_i'].flatten()
        y = torch.tensor(y.values)
        if worker_type_var=='occ2_first_recode':  #These occ types collectively have 6 obs in 2014 and 0 in 2009. The 0 in 2009 creates problems
            x = x[np.isnan(y)!=True]
            wgt = wgt[np.isnan(y)!=True]
            y = y[np.isnan(y)!=True]
        
        MSE = (torch.sum( wgt * (torch.tensor(x-y)**2))/torch.sum(wgt)).item()
        print('MSE', MSE)
        
        fig, ax1 = plt.subplots()
        if (cmap!=None) & (c!=None):
            ax1.scatter(x, y, c=c, cmap=cmap, s=wgt*100)
        else:
            ax1.scatter(x, y, s=wgt*100)
        if xlabel!='':
            ax1.set_xlabel(xlabel)
        if ylabel!='':
            ax1.set_ylabel(ylabel)
        if xlim!=():
            ax1.set_xlim(xlim[0], xlim[1])
        if ylim!=():
            ax1.set_ylim(ylim[0], ylim[1])
        ax1.set_title(title)
        axes = plt.gca()
        X = sm.add_constant(x)
        #regr = LinearRegression(fit_intercept=False)
        #b, m = regr.fit(X, y, wgt).coef_
        #r2 = regr.score(X, y, wgt)
        result = sm.WLS(np.array(y), X, weights=np.array(wgt) ).fit()
        b = result.params[0]
        m = result.params[1]
        b_se = result.bse[0]
        m_se = result.bse[1]
        X_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)
        ax1.plot(X_plot, m*X_plot + b, '-', label='Linear fit')
        #cov = np.cov(x, y, aweights=np.array(wgt) )
        #wgt_corr = cov[1,0]/np.sqrt(cov[0,0] * cov[1,1])
        ax1.text(0.73, 0.15, 'Slope = ' + str(round(m,3)) + ' \nIntercept = ' + str(round(b,3)) + ' \nMSE = ' + str(round(MSE,3)), verticalalignment='top', transform=ax1.transAxes) 
        xx = np.linspace(*ax1.get_xlim())
        ax1.plot(xx, xx, '--', label ='45 degree line')    
        ax1.legend()       
        if printvars==True:
            plt.figtext(0.1, -0.05, "xvar: " + x_str.replace('_', '\_') + ",    yvar: " + y_str.replace('_', '\_'), ha="left")
        if filename!='':
            fig.savefig(filename, dpi=300, bbox_inches="tight")
        
        return {'slope':m, 'intercept':b, 'slope_se':m_se, 'intercept_se':b_se, 'mse':MSE, 'n':int(result.nobs), 'reg_result':result}


    pre=2009
    post=2014    
    
    # Pull data
    data_full = pd.read_csv(mle_data_filename)
    data_full['ln_real_hrly_wage_dec'] = data_full['ln_real_hrly_wage_dec'].fillna(0)
    #data_full['worker_type'] = data_full[worker_type_var]
    #data_full['job_type']    = data_full[job_type_var]
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
    
    data_full.loc[data_full['gamma']==0, 'job_type'] = 0 
    
    temp = data_full[(data_full['gamma']>=0) & (data_full['iota']!=-1) & (data_full['job_type']!=-1) & (data_full['worker_type']!=-1) & (data_full['year']==pre)][['job_type','worker_type']]
    temp['employed'] = np.where(temp['job_type'] > 0, 1, 0)
    emprate_actual_pre = temp.groupby(['worker_type'])['employed'].mean() 
    emprate_actual_norm_pre = emprate_actual_pre / temp['employed'].mean()
    
    temp = data_full[(data_full['gamma']>=0) & (data_full['iota']!=-1) & (data_full['job_type']!=-1) & (data_full['worker_type']!=-1) & (data_full['year']==post)][['job_type','worker_type']]
    temp['employed'] = np.where(temp['job_type'] > 0, 1, 0)
    emprate_actual_post = temp.groupby(['worker_type'])['employed'].mean() 
    emprate_actual_norm_post = emprate_actual_post / temp['employed'].mean()
    
    #######################################
    # Actual wage changes, excluding non-employment
    if 1==1:
        data_full_levels_no_N_pre  = data_full[(data_full['gamma']>0) & (data_full['iota']!=-1) & (data_full['job_type']!=-1) & (data_full['worker_type']!=-1) & (data_full['year']==pre) ]
        data_full_levels_no_N_post = data_full[(data_full['gamma']>0) & (data_full['iota']!=-1) & (data_full['job_type']!=-1) & (data_full['worker_type']!=-1) & (data_full['year']==post) ]
        ln_wage_no_N_iota_pre  = data_full_levels_no_N_pre.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean() 
        ln_wage_no_N_iota_post = data_full_levels_no_N_post.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean()
        # Normalize by that year's mean wage
        wage_level_no_N_pre  = data_full_levels_no_N_pre['ln_real_hrly_wage_dec'].mean()
        wage_level_no_N_post = data_full_levels_no_N_post['ln_real_hrly_wage_dec'].mean()
        ln_wage_no_N_norm_iota_pre  = data_full_levels_no_N_pre.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean() /wage_level_no_N_pre 
        ln_wage_no_N_norm_iota_post = data_full_levels_no_N_post.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean()/wage_level_no_N_post
        # Compute changes
        actual_ln_wage_no_N_delta            = ln_wage_no_N_iota_post - ln_wage_no_N_iota_pre
        actual_ln_wage_no_N_delta_pct        = ln_wage_no_N_iota_post / ln_wage_no_N_iota_pre - 1
        actual_ln_wage_no_N_norm_delta       = ln_wage_no_N_norm_iota_post - ln_wage_no_N_norm_iota_pre
        actual_ln_wage_no_N_norm_delta_pct   = ln_wage_no_N_norm_iota_post / ln_wage_no_N_norm_iota_pre - 1
    
    
    
    #######################################
    # Actual wage changes, including non-employment
    if 1==1:
        data_full_levels_N_pre  = data_full[(data_full['gamma']>=0) & (data_full['iota']!=-1) & (data_full['job_type']!=-1) & (data_full['worker_type']!=-1) & (data_full['year']==pre) ]
        data_full_levels_N_post = data_full[(data_full['gamma']>=0) & (data_full['iota']!=-1) & (data_full['job_type']!=-1) & (data_full['worker_type']!=-1) & (data_full['year']==post) ]
        ln_wage_N_iota_pre  = data_full_levels_N_pre.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean() 
        ln_wage_N_iota_post = data_full_levels_N_post.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean()
        # Normalize by that year's mean wage
        wage_level_N_pre  = data_full_levels_N_pre['ln_real_hrly_wage_dec'].mean()
        wage_level_N_post = data_full_levels_N_post['ln_real_hrly_wage_dec'].mean()
        ln_wage_N_norm_iota_pre  = data_full_levels_N_pre.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean() /wage_level_N_pre 
        ln_wage_N_norm_iota_post = data_full_levels_N_post.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean()/wage_level_N_post
        # Compute changes
        actual_ln_wage_N_delta            = ln_wage_N_iota_post - ln_wage_N_iota_pre
        actual_ln_wage_N_delta_pct        = ln_wage_N_iota_post / ln_wage_N_iota_pre - 1
        actual_ln_wage_N_norm_delta       = ln_wage_N_norm_iota_post - ln_wage_N_norm_iota_pre
        actual_ln_wage_N_norm_delta_pct   = ln_wage_N_norm_iota_post / ln_wage_N_norm_iota_pre - 1
    
    
    
    
    ##########################################################
    # Solve model pre- and post-shock
    a_s_pre  = torch.tensor(a_ts[p_ts.index == pre,])
    a_s_post = torch.tensor(a_ts[p_ts.index == post,])  
    
    phi_outopt = torch.reshape(torch.tensor([phi_outopt_scalar]*mle_data_sums['I'], requires_grad=False), (mle_data_sums['I'],1))
    xi_outopt = torch.tensor([xi_outopt_scalar], requires_grad=False)
    
    
    equi_pre = solve_model(eta,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                a_s_pre,
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
                silent = True
                )
    
    equi_post = solve_model(eta,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                a_s_post,
                b_gs,
                mle_data_sums['m_i'],
                nu_hat = mle_estimates['nu_hat'],
                sigma_hat = mle_estimates['sigma_hat'],
                xi_hat = mle_estimates['xi_hat'],
                xi_outopt = xi_outopt,
                phi_outopt = phi_outopt,
                psi_hat = psi_hat,
                factor = 1e-3,  # dampening factor
                tol = 1e-4,     # precision level in the model solution
                decimals = 4,   # printed output rounding decimals
                silent = True
                )
    
    
    
    # Compute model-implied change in Phi (Also U^* and Phi+Xi)
    # This is all excluding non-employment
    
    # This is no longer Phi, it's expected wages. We need to revisit whether or not we want to do the sigma_hat piece

    
    
    # Compute levels and changes in Phi and U
    if 1==1:
        E_phi_N_pre  = compute_expected_Phi(equi_pre['p_ig'][:,1:] , psi_hat, equi_pre['w_g'], mle_estimates['sigma_hat'])
        E_phi_N_post = compute_expected_Phi(equi_post['p_ig'][:,1:], psi_hat, equi_post['w_g'], mle_estimates['sigma_hat'])
        
        
        p_ig_pre_no_N  = equi_pre['p_ig'][:,1:]  / torch.reshape( (1-equi_pre['p_ig'][:,0]),  (mle_data_sums['I'],1))
        p_ig_post_no_N = equi_post['p_ig'][:,1:] / torch.reshape( (1-equi_post['p_ig'][:,0]), (mle_data_sums['I'],1))
        E_phi_no_N_pre  = compute_expected_Phi(p_ig_pre_no_N , psi_hat, equi_pre['w_g'], mle_estimates['sigma_hat'])
        E_phi_no_N_post = compute_expected_Phi(p_ig_post_no_N, psi_hat, equi_post['w_g'], mle_estimates['sigma_hat'])
        
        
        E_U_pre  = compute_expected_U(equi_pre['p_ig'][:,1:] , psi_hat, mle_estimates['xi_hat'], mle_estimates['nu_hat'], equi_pre['w_g'])
        E_U_post = compute_expected_U(equi_post['p_ig'][:,1:], psi_hat, mle_estimates['xi_hat'], mle_estimates['nu_hat'], equi_post['w_g'])
        
        
        E_phi_N_delta     = E_phi_N_post - E_phi_N_pre
        E_phi_N_delta_pct = E_phi_N_post / E_phi_N_pre - 1
        E_phi_no_N_delta     = E_phi_no_N_post - E_phi_no_N_pre
        E_phi_no_N_delta_pct = E_phi_no_N_post / E_phi_no_N_pre - 1
        
        E_U_delta       = E_U_post - E_U_pre
        E_U_delta_pct   = E_U_post / E_U_pre - 1
    
    
    ######################################
    # Wages 
    
    if 1==1:
    
    
        ######################
        # Phi, normalized actual wages
        # 
        '''
        x = E_phi_N_delta
        y = actual_ln_wage_N_norm_delta
        x_str = 'E_phi_N_delta'
        y_str = 'actual_ln_wage_N_norm_delta'
        '''
        
        returnvals = make_scatter(E_phi_N_delta, actual_ln_wage_N_norm_delta, 'E_phi_N_delta', 'actual_ln_wage_N_norm_delta', xlabel='Model', ylabel='Actual', xlim=(-.3,.3), ylim=(-.5,.5), title=r'$\Delta$ log wages, $\eta=$'+str(eta)+ ', level ' + str(level) + ', N', filename=figuredir + '/' + 'model_fit_shock_E_Phi_norm_' + worker_type_var + "_" + job_type_var + "_" + "level_" + str(level) + '_eta_' + str(eta) + '_N.png', printvars=True)
                    
        cmap = matplotlib.colors.ListedColormap(['C0', 'red'])
        make_scatter(E_phi_N_delta, actual_ln_wage_N_norm_delta, 'E_phi_N_delta', 'actual_ln_wage_N_norm_delta', xlabel='Model', ylabel='Actual', xlim=(-.3,.3), ylim=(-.5,.5), title=r'$\Delta$ log wages, $\eta=$'+str(eta)+ ', level ' + str(level) + ', N', filename=figuredir + '/' + 'model_fit_shock_E_Phi_norm_' + worker_type_var + "_" + job_type_var + "_" + "level_" + str(level) + '_eta_' + str(eta) + '_N_highlight_1.png', printvars=True, cmap=cmap, c=mle_data_sums['m_i'].flatten()>.0045)
        
        cmap = matplotlib.colors.ListedColormap(['white', 'red'])
        make_scatter(E_phi_N_delta, actual_ln_wage_N_norm_delta, 'E_phi_N_delta', 'actual_ln_wage_N_norm_delta', xlabel='Model', ylabel='Actual', xlim=(-.3,.3), ylim=(-.5,.5), title=r'$\Delta$ log wages, $\eta=$'+str(eta)+ ', level ' + str(level) + ', N', filename=figuredir + '/' + 'model_fit_shock_E_Phi_norm_' + worker_type_var + "_" + job_type_var + "_" + "level_" + str(level) + '_eta_' + str(eta) + '_N_highlight_2.png', printvars=True, cmap=cmap, c=mle_data_sums['m_i'].flatten()>.0045)
    
    
        model_elasticities = (equi_post['l_g_demand']/equi_pre['l_g_demand']-1) / (equi_post['w_g']/equi_pre['w_g']-1)
        
        fig, ax2 = plt.subplots()
        ax2.hist(model_elasticities, facecolor='blue', alpha=0.5, range=(-100,400), bins=50)
        plt.figtext(0.7, 0.8, 'Median: '+ str(round(model_elasticities.median().item(),2)) + '\n' + r'$\hat \nu=$' + str(round(mle_estimates['nu_hat'].item(),4)), ha='left')
        if (level==0) | (level==1):
            plt.figtext(0, 0, 'Note: some positive and negative outliers trimmed', ha='left')
        fig.savefig(figuredir + '/' + 'model_fit_shock_elasticities_histogram_' + worker_type_var + "_" + job_type_var + "_" + "level_" + str(level) + '_eta_' + str(eta) + '_N.png', dpi=300, bbox_inches="tight")
    
    
        
        ###################################
        # Cross-sectional MLE fit
    
        if worker_type_var!='occ2_first_recode':
            # Pre, rescaling the P_ig
            make_scatter(E_phi_no_N_pre, ln_wage_no_N_iota_pre, 'E_phi_no_N_pre', 'ln_wage_no_N_iota_pre', xlabel='Model', ylabel='Actual', xlim=(1.5,4.5), ylim=(1.5,4.5), title=r'$\Phi$ versus actual log wages; 2009, level ' + str(level), filename=figuredir + '/' + 'model_fit_cross_section_pre_level_' + worker_type_var + "_" + job_type_var + "_" + "level_" + str(level) + '_eta_' + str(eta) + '.png', printvars=False)
         
            # Post, rescaling the P_ig
            make_scatter(E_phi_no_N_post, ln_wage_no_N_iota_post, 'E_phi_no_N_post', 'ln_wage_no_N_iota_post', xlabel='Model', ylabel='Actual', xlim=(1.5,4.5), ylim=(1.5,4.5), title=r'$\Phi$ versus actual log wages; 2014, level ' + str(level), filename=figuredir + '/' + 'model_fit_cross_section_post_level_' + worker_type_var + "_" + job_type_var + "_" + "level_" + str(level) + '_eta_' + str(eta) + '.png', printvars=False)
            
    return returnvals