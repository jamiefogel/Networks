#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 12:26:39 2021

@author: jfogel
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import shutil
from stargazer.stargazer import Stargazer, LineLocation


from model_fit_func import model_fit

plt.rcParams.update({
    "text.usetex": True,
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica"]})


fit_returnvals = []
for idx in classification_list: #
    # using wtype_var and jtype_Var instead of worker_type_var and job_type_var so we don't reset the master versions set in do_all.py
    wtype_var = idx[0]
    jtype_var = idx[1]
    print('---------------------------------------------')
    print(wtype_var, jtype_var)
    print('---------------------------------------------')

    suffix = wtype_var + "_" + jtype_var + "_" "level_" + str(level)

    suffix = wtype_var + "_" + jtype_var + "_" "level_" + str(level)
    fit_mle_data_filename      = root + "Data/derived/earnings_panel/" + filename_stub + "_level_0.csv"
    fit_mle_data_sums_filename = root + "Data/derived/mle_data_sums/" + filename_stub + "_mle_data_sums_" + suffix + ".p"
    fit_mle_estimates_filename = root + "Data/derived/MLE_estimates/" + filename_stub + "_mle_estimates_"  + suffix + ".p"
    fit_psi_and_k_file         = root + "Data/derived/MLE_estimates/" + filename_stub + "_psi_normalized_" + suffix + "_eta_" + str(eta) + ".p"
    fit_alphas_file            = root + "Data/derived/MLE_estimates/" + filename_stub + "_alphas_" + suffix + "_eta_" + str(eta) + ".p"
    
    fit_mle_data_sums = pickle.load(open(fit_mle_data_sums_filename, "rb"), encoding='bytes')
    fit_mle_estimates = pickle.load(open(fit_mle_estimates_filename, "rb"), encoding='bytes')
    fit_psi_hat = pickle.load(open(fit_psi_and_k_file, "rb"), encoding='bytes')['psi_hat']

    fit_alphags=load_alphas(fit_alphas_file)
    if jtype_var == 'sector_IBGE':
        fit_b_gs = torch.diag(x_s * torch.ones(S))
    else:
        fit_b_gs = fit_alphags * x_s


    rvals =  model_fit(fit_mle_data_filename, fit_mle_data_sums, fit_mle_estimates, fit_psi_hat, S, eta, level, fit_b_gs, wtype_var, jtype_var, a_ts, p_ts, 2013, 2018 figuredir)
    fit_returnvals.append(rvals)



i=0
for idx in classification_list: 
    wtype_var = idx[0]
    jtype_var = idx[1]
    print(wtype_var,jtype_var,round(fit_returnvals[i]['intercept'],3),round(fit_returnvals[i]['slope'],3),round(fit_returnvals[i]['mse'],3))
    i+=1
    
    
    
###################
# Make table 

# No S.E.s  (bad formatting in this version. The one with standard errors improves formatting significantly.)
result_table = open(figuredir + '/model_fit.tex', "w+")
result_table.write(r'\begin{table}[h!] \centering' +' \n')
result_table.write(r'\caption{Model fit}' +'\n')
result_table.write(r'\begin{tabular}{llccc}' +'\n')
result_table.write(r'\toprule' +'\n')   
i=0
for idx in classification_list:
    wtype_var = idx[0]
    jtype_var = idx[1]
    result_table.write(wtype_var.replace('_','\_') + '\t& ' + jtype_var.replace('_','\_') + '\t& ' + str(round(fit_returnvals[i]['intercept'],3)) + '\t& ' + str(round(fit_returnvals[i]['slope'],3)) + '\t& ' + str(round(fit_returnvals[i]['mse'],3)) + '\t \\\\ \n' )
    i+=1 

result_table.write(r'\bottomrule ' +' \n')
result_table.write(r'\end{tabular}' +' \n')
result_table.write(r'\label{table:model_fit}' +' \n')
result_table.write(r'\end{table}' +' \n')
result_table.close()  






regs_ig  = fit_returnvals[0]['reg_result']
regs_o2s = fit_returnvals[1]['reg_result']
regs_o4s = fit_returnvals[2]['reg_result']
regs_o4g = fit_returnvals[4]['reg_result']
regs_ks  = fit_returnvals[3]['reg_result']
regs_kg  = fit_returnvals[5]['reg_result']


mse_ig  = fit_returnvals[0]['mse']
mse_o2s = fit_returnvals[1]['mse']
mse_o4s = fit_returnvals[2]['mse']
mse_o4g = fit_returnvals[4]['mse']
mse_ks  = fit_returnvals[3]['mse']
mse_kg  = fit_returnvals[5]['mse']


n_ig  = fit_returnvals[0]['n']
n_o2s = fit_returnvals[1]['n']
n_o4s = fit_returnvals[2]['n']
n_o4g = fit_returnvals[4]['n']
n_ks  = fit_returnvals[3]['n']
n_kg  = fit_returnvals[5]['n']

# Ensure that the I/O is closed
try: 
    table.close()
except:
    pass
    
std_errors=True
if std_errors==True: 
    table = open(figuredir + "model_fit_se.tex", "w+")
else: 
    table = open(figuredir + "model_fit.tex", "w+")
    
#table.write('\\begin{table}[h!] \\centering\n')
#table.write('\\caption{Model fit}\n')
table.write('\\begin{tabular}{lccccc}\n')
table.write('\\toprule \n')
table.write('Worker classification \t & $\iota$ \t & Occ4 \t & Occ4 \t & k-means  \t & k-means  \\\\ \n')
table.write('Market classification \t & $\gamma$ \t & Sector \t & $\gamma$ \t & Sector  \t & $\gamma$  \\\\ \n')
table.write('\\midrule \n')
table.write('Intercept')
for model in [regs_ig,regs_o4s,regs_o4g,regs_ks,regs_kg]:
    table.write(' & {:.3f}'.format(np.round(model.params[0],3)))
table.write(' \\\\ \n')
if std_errors==True:
    for model in [regs_ig,regs_o4s,regs_o4g,regs_ks,regs_kg]:
        table.write(' & ( {:.3f} )'.format(np.round(model.bse[0],3)))
    table.write(' \\\\ \n')
table.write('Model-implied $\Delta$ log earnings')
for model in [regs_ig,regs_o4s,regs_o4g,regs_ks,regs_kg]:
    table.write(' & {:.3f}'.format(np.round(model.params[1],3)))
table.write(' \\\\ \n')
if std_errors==True:
    for model in [regs_ig,regs_o4s,regs_o4g,regs_ks,regs_kg]:
        table.write(' & ({:.3f})'.format(np.round(model.bse[1],3)))
    table.write(' \\\\ \n')
table.write('\\midrule \n')
table.write('MSE')
for model in [mse_ig,mse_o4s,mse_o4g,mse_ks,mse_kg]:
    table.write(' & {:.3f}'.format(np.round(model,3)))
table.write(' \\\\ \n')
table.write('Observations')
for model in [n_ig,n_o4s,n_o4g,n_ks,n_kg]:
    table.write(' & ' + str(model))
table.write(' \\\\ \n')
table.write('\\bottomrule \n')
table.write('\\end{tabular}\n')

#table.write('\\label{table:model_fit}\n')
#table.write('\\end{table}\n')

table.close()



