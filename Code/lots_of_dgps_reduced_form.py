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


obs = 1e5
homedir = os.path.expanduser("~")

t0 = time.time()

np.random.seed(20210316)

mle_dir = homedir + "/Networks/Code/jmp_version/mle_estimates/"
sums_dir= homedir + "/Networks/RAIS_exports/earnings_panel/"

firstyear=2009
lastyear=2012


exec(open('solve_model_functions.py').read())


########################################################################################################################
# 1. Load everything: mle data, mle data sums, mle estimates, psi and k, model parameters (a_s and alphas)
########################################################################################################################

#mle_data_filename      = homedir + "/Networks/RAIS_exports/earnings_panel/panel_rio_2009_2012_level_" + str(level) + ".csv"
mle_data_filename      = homedir + "/Networks/RAIS_exports/earnings_panel/panel_rio_2009_2012_w_kmeans.csv"
mle_data_sums_filename = sums_dir + "panel_rio_2009_2012_mle_data_sums_iota_gamma_level_" + str(level) + ".p"
mle_estimates_filename = mle_dir  + "panel_rio_2009_2012_mle_estimates_iota_gamma_level_" + str(level) + ".p"
psi_and_k_file         = mle_dir  + "panel_rio_2009_2012_psi_normalized_iota_gamma_level_" + str(level) + "_eta_" + str(eta) + ".p"
alphas_file            = homedir + "/Networks/Code/jmp_version/MLE_estimates/panel_rio_2009_2012_alphas_iota_gamma_level_" + str(level) + "_eta_" + str(eta) + ".p"



mle_data_sums = pickle.load(open(mle_data_sums_filename, "rb"), encoding='bytes')
mle_estimates = pickle.load(open(mle_estimates_filename, "rb"), encoding='bytes')
psi_hat = pickle.load(open(psi_and_k_file, "rb"), encoding='bytes')['psi_hat']

alphags = load_alphas(alphas_file)
b_gs = alphags * x_s

a_s_pre = torch.tensor(a_ts[p_ts.index == 2009,])


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
    pickle.dump(equi_pre,  open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_pre.p", "wb"))


# Pandemic shock
if 1==1:
        
    shock = torch.ones(S)
    shock[7] = .5 # Accomodations and food
    a_s_pandemic = a_s_pre * shock

    equi_pandemic = solve_model(eta,
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
    
    phi_pandemic = equi_pandemic['w_g'] * psi_hat
    pickle.dump(equi_pandemic,  open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_pandemic.p", "wb"))


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
    pickle.dump(equi_china,  open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_china.p", "wb"))
  
    
# Rio Shock
if 1==1:
        
    shock = torch.ones(S)
    shock[2] = .5 # Manufacturing industries
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
    pickle.dump(equi_rio,  open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_rio.p", "wb"))
 

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
    pickle.dump(equi_xxx, open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_xxx.p", "wb"))




fake_data_pre       = dgp(mle_data_filename, mle_data_sums, phi_pre,       mle_estimates['sigma_hat'], equi_pre,       2009, 2009)
# The version we used for the current figures didn't get saved. This is a new version of fake_data_free
fake_data_pre_filename = homedir + "/Networks/Code/jmp_version/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
fake_data_pre.to_csv(fake_data_pre_filename)
fake_data_pre = pd.read_csv(fake_data_pre_filename)


fake_data_pandemic  = dgp(mle_data_filename, mle_data_sums, phi_pandemic,  mle_estimates['sigma_hat'], equi_pandemic,  2009, 2009, replaceyear='2014')
fake_data_china     = dgp(mle_data_filename, mle_data_sums, phi_china,     mle_estimates['sigma_hat'], equi_china,     2009, 2009, replaceyear='2014')


fake_data_pre_rio   = dgp(mle_data_filename, mle_data_sums, phi_pre,       mle_estimates['sigma_hat'], equi_pre,     2009, 2012)
fake_data_rio       = dgp(mle_data_filename, mle_data_sums, phi_rio,       mle_estimates['sigma_hat'], equi_rio,     2014, 2014)
fake_data_xxx       = dgp(mle_data_filename, mle_data_sums, phi_xxx,       mle_estimates['sigma_hat'], equi_xxx,     2014, 2014)



equi_pre = pickle.load(open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_pre.p", "rb"))
equi_pandemic = pickle.load(open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_pandemic.p", "rb"))
equi_china = pickle.load(open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_china.p", "rb"))
equi_rio = pickle.load(open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_rio.p", "rb"))



fake_data_pandemic_filename = homedir + "/Networks/Code/jmp_version/dgp/fake_data_pandemic_rio_2009_2012_level_" + str(level) + ".csv"
fake_data_pandemic = fake_data_pandemic.append(fake_data_pre)
fake_data_pandemic.sort_values(by=['wid_masked','year'], inplace=True)
fake_data_pandemic.to_csv(fake_data_pandemic_filename)
fake_data_pandemic = pd.read_csv(fake_data_pandemic_filename)

fake_data_china_filename = homedir + "/Networks/Code/jmp_version/dgp/fake_data_china_rio_2009_2012_level_" + str(level) + ".csv"
fake_data_china = fake_data_china.append(fake_data_pre)
fake_data_china.sort_values(by=['wid_masked','year'], inplace=True)
fake_data_china.to_csv(fake_data_china_filename)
fake_data_china = pd.read_csv(fake_data_china_filename)

fake_data_rio_filename = homedir + "/Networks/Code/jmp_version/dgp/fake_data_rio_rio_2009_2012_level_" + str(level) + ".csv"
fake_data_rio = fake_data_rio.append(fake_data_pre_rio)
fake_data_rio.sort_values(by=['wid_masked','year'], inplace=True)
fake_data_rio.to_csv(fake_data_rio_filename)
fake_data_rio = pd.read_csv(fake_data_rio_filename)

fake_data_xxx_filename = homedir + "/Networks/Code/jmp_version/dgp/fake_data_xxx_rio_2009_2012_level_" + str(level) + ".csv"
fake_data_xxx = fake_data_xxx.append(fake_data_pre_rio)
fake_data_xxx.sort_values(by=['wid_masked','year'], inplace=True)
fake_data_xxx.to_csv(fake_data_xxx_filename)
fake_data_xxx = pd.read_csv(fake_data_xxx_filename)




########################################################################################################################
# 3. Load the fake data and run the analyses.
########################################################################################################################



bartik_analysis(fake_data_pandemic_filename,    equi_shock=equi_pandemic,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_pandemic')
(china_data_full, a, b, c) = bartik_analysis(fake_data_china_filename,       equi_shock=equi_china,     equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_china')
bartik_analysis(fake_data_rio_filename,         equi_shock=equi_rio,       equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_rio')
bartik_analysis(fake_data_xxx_filename,         equi_shock=equi_xxx,       equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_xxx') #This doesn't perfectly match the old version because we generated new data which introduces some random variation. 


# Actual data
mle_data_filename      = homedir + "/Networks/RAIS_exports/earnings_panel/panel_rio_2009_2012_w_kmeans.csv"
bartik_analysis(mle_data_filename,  y_ts=y_ts, shock_source='data', figuredir=figuredir, savefile_stub='real_data')






# This is temporary. Replicates the old version using the new bartik_analysis() function
dgp_mle_data_filename      = homedir + "/Networks/Code/jmp_version/dgp/dgp_panel_rio_2009_2012_level_" + str(level) + ".csv"
dgp_equi_2009 = pickle.load( open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_2009.p", "rb"))
dgp_equi_2014 = pickle.load( open(homedir + "/Networks/Code/jmp_version/dgp/dgp_equi_2014.p", "rb"))
bartik_analysis(dgp_mle_data_filename, equi_shock=dgp_equi_2014, equi_pre=dgp_equi_2009, figuredir=figuredir, savefile_stub='test', print_regs=True)




########################################################################################################################
# 4. Explore the simulated China shock
########################################################################################################################


# Where do workers initially in manufacturing go?



sector_transition_counts = pd.crosstab(index = china_data_full.sector_IBGE_2009, columns = data_full.sector_IBGE_2014)
rowsums = sector_transition_counts.sum(axis=1)
sector_transition_rates = sector_transition_counts.divide(rowsums, axis=0)


fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
#sns.heatmap(df1.iloc[:, 1:6:], annot=True, linewidths=.5, ax=ax)
hmap = sns.heatmap(
    sector_transition_rates.values, 
    vmin=-1, vmax=1, center=0,
    cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
    #cmap=sns.color_palette("coolwarm", n_colors=20),
    #cmap=sns.color_palette("RdBu", 100),
    square=False,
    xticklabels=sector_labels_abbr,
    yticklabels=sector_labels_abbr,
    ax=ax
)
ax.set_ylabel('Origin Sector', fontsize=18)
ax.set_xlabel('Destination Sector', fontsize=18)
hmap.set_xticklabels(hmap.get_xticklabels(), rotation = 40, ha='right')
ax.figure.savefig(figuredir + savefile_stub + "_sector_transition_rates.png", dpi=300, bbox_inches="tight")





sector_emp_counts_2009 = pd.DataFrame(china_data_full.groupby(['sector_IBGE_2009'])['sector_IBGE_2009'].count()).rename(columns={'sector_IBGE_2009':'count'})
sector_emp_counts_2009['label'] = np.array(sector_labels_abbr)
sector_emp_counts_2009.index.name=None
sector_emp_counts_2009.to_latex(index=False, buf=figuredir + savefile_stub + "_sector_counts.tex", caption='Sector counts (2009)', columns=['label','count'], header=['Sector','Employment'], multicolumn=False)

entropy  = -torch.sum( equi_china['l_gs_demand'] * torch.log(equi_china['l_gs_demand']),dim=0)



round(pd.DataFrame(equi_china['l_gs_demand'][:,1]).sort_values(by=0),4)


for s in range(15):
    fig, ax = plt.subplots()
    ax.hist(equi_china['l_gs_demand'][:,s], bins=50, range=[0,1.2], density=True)
    ax.set_title(str(s))
    
    
plt.hist(equi_china['l_gs_demand'][:,0], bins=50, range=[0,1])
plt.hist(equi_china['l_gs_demand'][:,1], bins=50, range=[0,1])
plt.hist(equi_china['l_gs_demand'][:,2], bins=50, range=[0,1])
plt.hist(equi_china['l_gs_demand'][:,3], bins=50, range=[0,1])
plt.hist(equi_china['l_gs_demand'][:,4], bins=50, range=[0,1])



# Gamma=189 (Gamma is the index+1) is the most-used gamma in this sector. Unsurprisingly it is the largest beta too. 
round(pd.DataFrame(equi_china['l_gs_demand'][:,1]).sort_values(by=0),4)
round(pd.DataFrame(b_gs[:,1]).sort_values(by=0),4)


# Most w_g's increased due to the shock
plt.hist(equi_china['w_g']/equi_pre['w_g'])


# What w_g's changed most due to the China shock?
round(pd.DataFrame(equi_china['w_g']/equi_pre['w_g']).sort_values(by=0),4)

#Which gammas does manufacturing use most intensely?
round(pd.DataFrame(b_gs[:,2]).sort_values(by=0),4)




# Post-shock non-employment decreased for most iotas
plt.hist(equi_china['p_ig'][:,0]/equi_pre['p_ig'][:,0])



# Output declined for manufacturing but increased for every other sector. Presumably because the decline in manufacturing freed up workers for every other sector. 
equi_china['y_s']/equi_pre['y_s']




# Compute wage changes by gamma
df = pd.DataFrame({'delta_w_g':equi_china['w_g']/equi_pre['w_g'], 'gamma':np.arange(1,428), 'beta_manuf':b_gs[:,2],'beta_extract':b_gs[:,1],'beta_agric':b_gs[:,0]})
                   
df = pd.DataFrame(np.array(b_gs.data))
df['gamma'] = np.arange(1,428)
df['delta_w_g'] = equi_china['w_g']/equi_pre['w_g']
df.corr()[['delta_w_g',2]]
# The shock-induced wage changes are somewhat strongly negatively correlated with manufacturing betas. The correlations with all other sectors' betas are smaller. 


# Which iotas were most affected by the shock?
E_phi_N_pre  = compute_expected_Phi(equi_pre['p_ig'][:,1:] ,  psi_hat, equi_pre['w_g'],   mle_estimates['sigma_hat'])
E_phi_N_post = compute_expected_Phi(equi_china['p_ig'][:,1:], psi_hat, equi_china['w_g'], mle_estimates['sigma_hat'])

delta_earn     = E_phi_N_post - E_phi_N_pre
delta_earn_pct = E_phi_N_post / E_phi_N_pre - 1
   

df = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=0),axis=1)
df['delta_earn'] = delta_earn
df.corr()[['delta_earn',3.0]]

# Iotas with bigger earnings losses are more likely to work in manufacturing (Sector 3) or extraction (sector 2)
# THe probability that an iota works in manufacturing is highly correlated with the probability that they work in almost every other sector, with the largest exceptions being agriculture (sector 1), extraction (sector 2), and utilities (sector 4).  


# My tentative story is that the shock hits manufacturing and spills over especially strongly to extraction because the betas for manufacturing and extraction are somewhat highly correlated (0.34), which means that manufacturing and extraction require similar tasks, so the drop in manufacturing reduces demand for tasks that extraction is intensive in. Manufacturing has even more highly correlated betas with finance (Sector 10) and prof/science/tech svcs (sector 12) however we do not see much of an earnings effect for workers who tend to be employed in these sectors. Professional/science/tech svcs has highly correlated iota hiring patterns with lots of other sectors so maybe that means those workers are more insulated. But I don't think this story works so well for finance. 






#############
# I deleted a bunch of code at 4:30pm on 8/23/2021 that was here that did an ad hoc study of the China shock. I think it was all just a rough draft of what is now shock_case_study_china.py and therefore not worth saving, but I'm leaving this note in case I need to go back to Dropbox and recover it. 







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
    fake_data = dgp(mle_data_filename, mle_data_sums, phi_shock_loop,  mle_estimates['sigma_hat'], equi_neg,  2009, 2009, replaceyear='2014')
    fake_data_filename = homedir + "/Networks/Code/jmp_version/dgp/fake_data_temp.csv"
    fake_data = fake_data.append(fake_data_pre)
    fake_data.sort_values(by=['wid_masked','year'], inplace=True)
    fake_data.to_csv(fake_data_filename)
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
    fake_data = dgp(mle_data_filename, mle_data_sums, phi_shock_loop,  mle_estimates['sigma_hat'], equi_pos,  2009, 2009, replaceyear='2014')
    fake_data_filename = homedir + "/Networks/Code/jmp_version/dgp/fake_data_temp.csv"
    fake_data = fake_data.append(fake_data_pre)
    fake_data.sort_values(by=['wid_masked','year'], inplace=True)
    fake_data.to_csv(fake_data_filename)
    (dump, r2_vec, coef_vec, se_vec) = bartik_analysis(fake_data_filename,    equi_shock=equi_pos,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_sector' +str(s+1) + 'pos')

    r2_append_df = pd.DataFrame(data=[[str(s+1)+', positive'] + r2_vec  ], columns = ['Shock','IotaSector','IotaMarket','Occ4Sector','Occ4Market'], index=[0])
    r2_df = r2_df.append(r2_append_df) 
    
    coef_append_df = pd.DataFrame(data=[[str(s+1)+', positive'] + coef_vec  ], columns = ['Shock','IotaSector','IotaMarket','Occ4Sector','Occ4Market'], index=[0])
    coef_df = coef_df.append(coef_append_df) 
    
    del fake_data
    del dump

pickle.dump(r2_df,    open(homedir + "/Networks/Code/jmp_version/results/r2_df_30_shocks.p",   "wb"))
pickle.dump(coef_df,  open(homedir + "/Networks/Code/jmp_version/results/coef_df_30_shocks.p", "wb"))

r2_df   = pickle.load(open(homedir + "/Networks/Code/jmp_version/results/r2_df_30_shocks.p",   "rb"), encoding='bytes')
coef_df = pickle.load(open(homedir + "/Networks/Code/jmp_version/results/coef_df_30_shocks.p", "rb"), encoding='bytes')

fig, ax = plt.subplots()
ax.plot(range(r2_df.shape[0]), r2_df.IotaMarket, label="Worker type, Market", linestyle='--')
ax.plot(range(r2_df.shape[0]), r2_df.IotaSector, label="Worker type, Sector", linestyle='-')
ax.plot(range(r2_df.shape[0]), r2_df.Occ4Market, label="Occ4, Market", linestyle=':')
ax.plot(range(r2_df.shape[0]), r2_df.Occ4Sector, label="Occ4, Sector", linestyle='-.')
ax.set_xlabel('Simulation')
ax.set_ylabel('$R^2$')
ax.legend()
ax.figure.savefig(figuredir + 'fake_data_all_sector_shocks_r2.png',bbox_inches='tight')



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


