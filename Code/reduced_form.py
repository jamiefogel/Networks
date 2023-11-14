#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 17:03:46 2021

In our 7/28/2021 meeting Matthew suggested I do the reduced form exercise for a bunch of different simulated shocks. I implement that here. 

@author: jfogel
"""

'''
1. Load everything: mle data, mle data sums, mle estimates, psi and k, model parameters (a_s and alphas)
2. Solve the model given demand shifters for a particular year (we have chosen 2009. Not sure why.)
3. Load the raw panel data. We do this because we're going to use the empirical distribution of iotas, gammas, and c's
4. Draw workers' choices of gamma, wages, and sectors using our model-implied DGP
'''

import matplotlib.pyplot as plt
import numpy as np
import time
from dgp_func import dgp
from bartik_analysis import bartik_analysis
from solve_model_functions import *


obs = 1e5
homedir = os.path.expanduser("~")

t0 = time.time()

np.random.seed(20210316)

# XX I think these are redundant with the new filepaths
#mle_dir = root + "MLE_estimates/"
#sums_dir= root + "Data/mle_data_sums/"

firstyear=2009
lastyear=2012



########################################################################################################################
# 1. Load everything: mle data, mle data sums, mle estimates, psi and k, model parameters (a_s and alphas)
########################################################################################################################


worker_type_var = 'iota'
job_type_var    = 'gamma'
#XX worker_type_var, job_type_var and filename_stub should probably all be arguments to a function




mle_data_filename      = root + "Data/derived/earnings_panel/" + filename_stub + "_level_0.csv"
mle_data_sums_filename = root + "Data/derived/mle_data_sums/" + filename_stub + "_mle_data_sums_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"
mle_estimates_filename = root + "Data/derived/MLE_estimates/" + filename_stub + "_mle_estimates_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"
psi_and_k_file         = root + "Data/derived/MLE_estimates/" + filename_stub + "_psi_normalized_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + "_eta_" + str(eta) + ".p"
alphas_file            = root + "Data/derived/MLE_estimates/" + filename_stub + "_alphas_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"

mle_data_sums = pickle.load(open(mle_data_sums_filename, "rb"), encoding='bytes')
mle_estimates = pickle.load(open(mle_estimates_filename, "rb"), encoding='bytes')
psi_hat = pickle.load(open(psi_and_k_file, "rb"), encoding='bytes')['psi_hat']

alphags = load_alphas(alphas_file)
b_gs = alphags * x_s

a_s_pre = torch.tensor(a_ts[p_ts.index == firstyear,])


phi_outopt = torch.reshape(torch.tensor([phi_outopt_scalar]*mle_data_sums['I'], requires_grad=False), (mle_data_sums['I'],1))
xi_outopt = torch.tensor([xi_outopt_scalar], requires_grad=False)

########################################################################################################################
# 2. Solve the model for various demand shifters
########################################################################################################################


# Pre-shock
if 1==1:
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
                silent = solve_GE_silently
                )
    
    phi_pre = equi_pre['w_g'] * psi_hat
    pickle.dump(equi_pre,  open(root + "Data/derived/dgp/dgp_equi_pre.p", "wb"))


    
# Rio Shock
if 1==1:        
    #shock = torch.ones(S)
    a_s_rio = torch.tensor(a_ts[p_ts.index == 2014,])
    equi_rio = solve_model(eta,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                a_s_rio,
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
    
    phi_rio = equi_rio['w_g'] * psi_hat
    pickle.dump(equi_rio,  open(root + "Data/derived/dgp/dgp_equi_rio.p", "wb"))
 

# XXX Shock (arbitrarily defined shock that we used in dgp_adh)
if 1==1:
    shock = np.ones(15)
    shock[2] = .5
    shock[7] = .2
    shock[6] = 3
    shock[10]= 2
    a_s_xxx = a_s_pre * shock
 
    equi_xxx = solve_model(eta,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                a_s_xxx,
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
    
    phi_xxx = equi_xxx['w_g'] * psi_hat
    pickle.dump(equi_xxx, open(root + "Data/derived/dgp/dgp_equi_xxx.p", "wb"))



# "China" shock
if 1==1:
        
    shock = torch.ones(S)
    shock[2] = .5 # Manufacturing industries
    a_s_china = a_s_pre * shock

    equi_china = solve_model(eta,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                a_s_china,
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
    
    phi_china = equi_china['w_g'] * psi_hat
    pickle.dump(equi_china,  open(root + "Data/derived/dgp/dgp_equi_china.p", "wb"))
  
    
print('Location '+str(1))

fake_data_pre       = dgp(mle_data_filename, mle_data_sums, phi_pre,       mle_estimates['sigma_hat'], equi_pre,       2009, 2009)
# The version we used for the current figures didn't get saved. This is a new version of fake_data_free
fake_data_pre_filename = root + "Data/derived/dgp/fake_data_pre_rio_2009_2009_level_" + str(level) + ".csv"
fake_data_pre.to_csv(fake_data_pre_filename, index=False)
fake_data_pre = pd.read_csv(fake_data_pre_filename)

    
print('Location '+str(2))

fake_data_pre_rio   = dgp(mle_data_filename, mle_data_sums, phi_pre,       mle_estimates['sigma_hat'], equi_pre,     2009, 2012)
fake_data_rio       = dgp(mle_data_filename, mle_data_sums, phi_rio,       mle_estimates['sigma_hat'], equi_rio,     2014, 2014)
fake_data_xxx       = dgp(mle_data_filename, mle_data_sums, phi_xxx,       mle_estimates['sigma_hat'], equi_xxx,     2014, 2014)


print('Location '+str(3))

equi_pre = pickle.load(open(root + "Data/derived/dgp/dgp_equi_pre.p", "rb"))
equi_rio = pickle.load(open(root + "Data/derived/dgp/dgp_equi_rio.p", "rb"))


    
print('Location '+str(4))

fake_data_rio_filename = root + "Data/derived/dgp/fake_data_rio_rio_2009_2012_level_" + str(level) + ".csv"
fake_data_rio = fake_data_rio.append(fake_data_pre_rio)
fake_data_rio.sort_values(by=['wid_masked','year'], inplace=True)
fake_data_rio.to_csv(fake_data_rio_filename, index=False)
fake_data_rio = pd.read_csv(fake_data_rio_filename)

    
print('Location '+str(5))

# We don't actually use the xxx version in the paper. Keeping it for now because it simulates a shock that leads to Bartik regressions with much large R2's, which may help satisfy the concerns John raised in his emails the weekend of 7/23/2022
fake_data_xxx_filename = root + "Data/derived/dgp/fake_data_xxx_rio_2009_2012_level_" + str(level) + ".csv"
fake_data_xxx = fake_data_xxx.append(fake_data_pre_rio)
fake_data_xxx.sort_values(by=['wid_masked','year'], inplace=True)
fake_data_xxx.to_csv(fake_data_xxx_filename, index=False)
fake_data_xxx = pd.read_csv(fake_data_xxx_filename)


    
print('Location '+str(6))


########################################################################################################################
# 3. Load the fake data and run the analyses.
########################################################################################################################


# This function creates iota_occ4_exposure_regs_ln_wage_N.tex as well as a bunch of other files we didn't end up using
bartik_analysis(fake_data_rio_filename,         equi_shock=equi_rio,       equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_rio') # Produces fake_data_rio_iota_occ4_exposure_regs_ln_wage_N.tex
bartik_analysis(fake_data_xxx_filename,         equi_shock=equi_xxx,       equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_xxx') # This doesn't perfectly match the old version because we generated new data which introduces some random variation. 

    
print('Location '+str(7))

# Actual data
bartik_analysis(mle_data_filename, 'iota', 'gamma', 2009, 2014, y_ts=y_ts, shock_source='data', figuredir=figuredir, savefile_stub='real_data') # Produces real_data_iota_occ4_exposure_regs_ln_wage_N.tex

reg_dict = {}
classification_list2 = [('iota','gamma'), ('iota','occ2Xmeso_recode'), ('occ2Xmeso_first_recode','gamma'), ('occ4_first_recode','gamma'), ('iota','occ4_recode'), ('occ4_first_recode', 'gamma'), ('occ4_first_recode', 'occ4_recode'), ('occ2Xmeso_first_recode', 'occ2Xmeso_recode')]
for idx in classification_list2: #
    print(idx[0])
    print(idx[1])
    (res1, res2, data) = bartik_analysis(mle_data_filename, idx[0], idx[1], 2009, 2014, y_ts=y_ts, shock_source='data', figuredir=figuredir, savefile_stub='real_data')
    dict = {'wtype':idx[0], 'jtype':idx[1], 'reg_output_nojtype': res1, 'reg_output_jtype': res2} 
    reg_dict[idx] = dict

pickle.dump(reg_dict, open(root + '/Data/derived/'+ filename_stub + 'reduced_form_reg_results.p', 'wb'))



regs_list = []
exposure_list = []
wtype_list = []
jtype_list = []
for idx in [('iota','gamma'), ('iota','occ2Xmeso_recode'), ('iota','occ4_recode'), ('occ2Xmeso_first_recode','gamma'), ('occ4_first_recode','gamma')]:
    regs_list = regs_list + [reg_dict[idx]['reg_output_jtype'], reg_dict[idx]['reg_output_nojtype']]
    exposure_list = exposure_list + ['Market', 'Sector']
    wtype_str = idx[0].replace('gamma','$\gamma$').replace('iota','$\iota$').replace('occ2Xmeso','Occ2$\\times$Meso Region').replace('occ4_recode','Occ4').replace('occ4_first_recode','Occ4')
    jtype_str = idx[0].replace('gamma','$\gamma$').replace('iota','$\iota$').replace('occ2Xmeso','Occ2$\\times$Meso Region').replace('occ4_recode','Occ4').replace('occ4_first_recode','Occ4')    
    wtype_list = wtype_list + [wtype_str, wtype_str]
    jtype_list = jtype_list + [jtype_str, 'N/A']
    print(idx)
    print(reg_dict[idx]['reg_output_nojtype'].params)
    print(reg_dict[idx]['reg_output_nojtype'].rsquared)
    print(reg_dict[idx]['reg_output_jtype'].params)
    print(reg_dict[idx]['reg_output_jtype'].rsquared)


    
# Regress changes in earnings on various exposure measures
# This is the output we actually use

                                         

stargazer = Stargazer(regs_list)
stargazer.rename_covariates({'wtype_exposure_jtype_std':'Exposure (market)','wtype_exposure_std':'Exposure (sector)'})
stargazer.covariate_order(['Intercept','wtype_exposure_jtype_std','wtype_exposure_std'])
stargazer.show_model_numbers(False)
stargazer.dep_var_name = None
stargazer.show_degrees_of_freedom(False)
stargazer.show_f_statistic = False
stargazer.show_residual_std_err=False
stargazer.show_adj_r2 = False
stargazer.significance_levels([10e-100, 10e-101, 10e-102])  # Suppress significance stars
stargazer.append_notes(False)
stargazer.add_line('Exposure:', exposure_list, LineLocation.BODY_TOP)
stargazer.add_line('Worker classification:', wtype_list, LineLocation.BODY_TOP)
stargazer.add_line('Job classification:', jtype_list, LineLocation.BODY_TOP)
#stargazer.add_line('\hline', ['', '', '', ''], LineLocation.BODY_TOP)
#stargazer.add_line('Preferred Specification', ['No', 'Yes', 'No', 'No'], LineLocation.FOOTER_TOP)
print(stargazer.render_latex())
with open(figuredir + savefile_stub + "_rio_shock.tex", "w") as f:
    f.write(stargazer.render_latex(only_tabular=True ))
    
    

    
print('Location '+str(8))


##############################################################################################################################
##############################################################################################################################
# Let's loop over sectors and do a positive and a negative shock for each
##############################################################################################################################
##############################################################################################################################

r2_df   = pd.DataFrame(columns=['Shock','IotaSector','IotaMarket','Occ4Sector','Occ4Market'])
coef_df = pd.DataFrame(columns=['Shock','IotaSector','IotaMarket','Occ4Sector','Occ4Market'])

for s in range(S):
    print(s)
        
    shock = torch.ones(S)
    shock[s] = .5 # Accomodations and food
    a_s_pandemic = a_s_pre * shock

    equi_neg = solve_model(eta,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                a_s_pandemic,
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
    
    phi_shock_loop = equi_neg['w_g'] * psi_hat    
    fake_data = dgp(mle_data_filename, mle_data_sums, phi_shock_loop,  mle_estimates['sigma_hat'], equi_neg,  2013, 2013, replaceyear='2016')
    fake_data_filename = root + "Data/derived/dgp/fake_data_temp.csv"
    fake_data = fake_data.append(fake_data_pre)
    fake_data.sort_values(by=['wid_masked','year'], inplace=True)
    fake_data.to_csv(fake_data_filename, index=False)
    (dump, r2_vec, coef_vec, se_vec) = bartik_analysis(fake_data_filename,    equi_shock=equi_neg,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_sector' +str(s+1) + 'neg')
    
    #r2_append_df = pd.DataFrame({'Shock':str(s+1)+', negative','IotaSector':r2_vec[0],'IotaMarket':r2_vec[1],'Occ4Sector':r2_vec[2],'Occ4Market':r2_vec[3]}, index=np.array(0))
    
    r2_append_df = pd.DataFrame(data=[[str(s+1)+', negative'] + r2_vec  ], columns = ['Shock','IotaSector','IotaMarket','Occ4Sector','Occ4Market'], index=[0])
    r2_df = r2_df.append(r2_append_df) 
    
    coef_append_df = pd.DataFrame(data=[[str(s+1)+', negative'] + coef_vec  ], columns = ['Shock','IotaSector','IotaMarket','Occ4Sector','Occ4Market'], index=[0])
    coef_df = coef_df.append(coef_append_df) 
    
    del fake_data
    del dump

        
    shock = torch.ones(S)
    shock[s] = 2 # Accomodations and food
    a_s_pandemic = a_s_pre * shock

    equi_pos = solve_model(eta,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                a_s_pandemic,
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
    
    phi_shock_loop = equi_pos['w_g'] * psi_hat    
    fake_data = dgp(mle_data_filename, mle_data_sums, phi_shock_loop,  mle_estimates['sigma_hat'], equi_pos,  2013, 2013, replaceyear='2016')
    fake_data_filename = root + "Data/derived/dgp/fake_data_temp.csv"
    fake_data = fake_data.append(fake_data_pre)
    fake_data.sort_values(by=['wid_masked','year'], inplace=True)
    fake_data.to_csv(fake_data_filename, index=False)
    (dump, r2_vec, coef_vec, se_vec) = bartik_analysis(fake_data_filename,    equi_shock=equi_pos,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_sector' +str(s+1) + 'pos')

    r2_append_df = pd.DataFrame(data=[[str(s+1)+', positive'] + r2_vec  ], columns = ['Shock','IotaSector','IotaMarket','Occ4Sector','Occ4Market'], index=[0])
    r2_df = r2_df.append(r2_append_df) 
    
    coef_append_df = pd.DataFrame(data=[[str(s+1)+', positive'] + coef_vec  ], columns = ['Shock','IotaSector','IotaMarket','Occ4Sector','Occ4Market'], index=[0])
    coef_df = coef_df.append(coef_append_df) 
    
    del fake_data
    del dump




    
print('Location '+str(9))
    
pickle.dump(r2_df,    open(root + "Results/r2_df_30_shocks.p",   "wb"))
pickle.dump(coef_df,  open(root + "Results/coef_df_30_shocks.p", "wb"))

r2_df   = pickle.load(open(root + "Results/r2_df_30_shocks.p",   "rb"), encoding='bytes')
coef_df = pickle.load(open(root + "Results/coef_df_30_shocks.p", "rb"), encoding='bytes')


fig, ax = plt.subplots()
ax.plot(range(r2_df.shape[0]), r2_df.IotaMarket, label="Worker type, Market", linestyle='--')
ax.plot(range(r2_df.shape[0]), r2_df.IotaSector, label="Worker type, Sector", linestyle='-')
ax.plot(range(r2_df.shape[0]), r2_df.Occ4Market, label="Occ4, Market", linestyle=':')
ax.plot(range(r2_df.shape[0]), r2_df.Occ4Sector, label="Occ4, Sector", linestyle='-.')
ax.set_xlabel('Simulation')
ax.set_ylabel('$R^2$')
ax.legend()
ax.figure.savefig(figuredir + 'fake_data_all_sector_shocks_r2.png',bbox_inches='tight') # Used in paper and slides


    
print('Location '+str(10))

# Used in paper and slides
fig, ax = plt.subplots()
ax.plot(range(coef_df.shape[0]), coef_df.IotaMarket, label="Worker type, Market", linestyle='--')
ax.plot(range(coef_df.shape[0]), coef_df.IotaSector, label="Worker type, Sector", linestyle='-')
ax.plot(range(coef_df.shape[0]), coef_df.Occ4Market, label="Occ4, Market", linestyle=':')
ax.plot(range(coef_df.shape[0]), coef_df.Occ4Sector, label="Occ4, Sector", linestyle='-.')
ax.set_xlabel('Simulation')
ax.set_ylabel(r'$\beta_{Exposure}$')
ax.legend()
ax.figure.savefig(figuredir + 'fake_data_all_sector_shocks_coef.png',bbox_inches='tight')

df_means = pd.concat([ \
    pd.DataFrame({'Worker Classification':['Worker type','Worker type','Occ4','Occ4']}, index=r2_df[['IotaMarket','IotaSector','Occ4Market','Occ4Sector']].mean().index), \
    pd.DataFrame({'Job Classification':['Market','Sector','Market','Sector']}, index=r2_df[['IotaMarket','IotaSector','Occ4Market','Occ4Sector']].mean().index), \
    pd.DataFrame({'Coefficient':np.round(coef_df[['IotaMarket','IotaSector','Occ4Market','Occ4Sector']].mean(),3)}), \
    pd.DataFrame({'Coefficient':np.round(coef_df[['IotaMarket','IotaSector','Occ4Market','Occ4Sector']].std(),3)}), \
    pd.DataFrame({r'$R^2$':np.round(r2_df[['IotaMarket','IotaSector','Occ4Market','Occ4Sector']].mean(),3)}), \
    pd.DataFrame({r'$R^2$':np.round(r2_df[['IotaMarket','IotaSector','Occ4Market','Occ4Sector']].std(),3)}) \
    ], axis=1)
    
df_means.columns = pd.MultiIndex.from_arrays([df_means.columns, ['\hfill','\hfill','Mean','Std Dev','Mean','Std Dev']])


    
print('Location '+str(11))

# Used in paper and slides. I create the table initially as temp.tex and then parse it and edit it and save as fake_data_all_sector_shocks_means.tex
df_means.to_latex(index=False, buf=figuredir + "temp.tex", caption='Means across all simulated shocks', multicolumn=True, multicolumn_format='c', label='table:all_sector_shocks_means', escape=False)
fin = open(figuredir + "temp.tex", "rt")
#output file to write the result to
fout = open(figuredir + "fake_data_all_sector_shocks_means.tex", "wt")
#for each line in the input file
for line in fin:
	line = line.replace(r'\multicolumn{2}{c}{$R^2$} \\', r'\multicolumn{2}{c}{$R^2$} \\ \cline{3-4}\cline{5-6}')
	line = line.replace(r'\begin{tabular}{llrrrr}',r'\begin{tabular}{@{\extracolsep{10pt}}llrrrr}')
	fout.write(line)
    #print(line.replace(r'Worker Classification & Job Classification & \multicolumn{2}{c}{Coefficient} & \multicolumn{2}{c}{$R^2$} \\', r'Worker Classification & Job Classification & \multicolumn{2}{c}{Coefficient} & \multicolumn{2}{c}{$R^2$} \\\cline{3-4}\cline{5-6}'))

fin.close()
fout.close()



    
print('Location '+str(12))
