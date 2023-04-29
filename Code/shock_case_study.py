#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 28 14:19:08 2021

@author: jfogel
"""

create_data = False

# Create fake data for construction shock
# if 1==1:
#     shock = torch.ones(S)
#     shock[4] = .5 # Construction
#     a_s_const = a_s_pre * shock
    
#     equi_const = solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_const,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_const = equi_const['w_g'] * psi_hat
#     pickle.dump(equi_const,  open(root + "Data/dgp/dgp_equi_const.p", "wb"))
      
    
#     fake_data_const     = dgp(mle_data_filename, mle_data_sums, phi_const,     mle_estimates['sigma_hat'], equi_const,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_const_filename = root + "Data/dgp/fake_data_const_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_const = fake_data_const.append(fake_data_pre)
#     fake_data_const.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_const.to_csv(fake_data_const_filename)
#     fake_data_const = pd.read_csv(fake_data_const_filename)
    
# Create fake data for accomodations and food shock
if 1==1:
    shock = torch.ones(S)
    shock[7] = .5 # Accomodations and food
    a_s_AccomFood = a_s_pre * shock
    
    equi_AccomFood= solve_model(eta,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                a_s_AccomFood,
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
    
    phi_AccomFood = equi_AccomFood['w_g'] * psi_hat
    pickle.dump(equi_AccomFood,  open(root + "Data/dgp/dgp_equi_AccomFood.p", "wb"))
      
    
    fake_data_AccomFood     = dgp(mle_data_filename, mle_data_sums, phi_AccomFood,     mle_estimates['sigma_hat'], equi_AccomFood,     2009, 2009, replaceyear='2014')
    
    fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
    fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    fake_data_AccomFood_filename = root + "Data/dgp/fake_data_AccomFood_rio_2009_2012_level_" + str(level) + ".csv"
    fake_data_AccomFood = fake_data_AccomFood.append(fake_data_pre)
    fake_data_AccomFood.sort_values(by=['wid_masked','year'], inplace=True)
    fake_data_AccomFood.to_csv(fake_data_AccomFood_filename)
    fake_data_AccomFood = pd.read_csv(fake_data_AccomFood_filename)

# # Create fake data for Extractive industries
# if 1==1:
#     shock = torch.ones(S)
#     shock[7] = .5 # Accomodations and food
#     a_s_AccomFood = a_s_pre * shock
    
#     equi_AccomFood= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_AccomFood,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_AccomFood = equi_AccomFood['w_g'] * psi_hat
#     pickle.dump(equi_AccomFood,  open(root + "Data/dgp/dgp_equi_AccomFood.p", "wb"))
      
    
#     fake_data_AccomFood     = dgp(mle_data_filename, mle_data_sums, phi_AccomFood,     mle_estimates['sigma_hat'], equi_AccomFood,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
#     fake_data_AccomFood_filename = root + "Data/dgp/fake_data_AccomFood_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_AccomFood = fake_data_AccomFood.append(fake_data_pre)
#     fake_data_AccomFood.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_AccomFood.to_csv(fake_data_AccomFood_filename)
#     fake_data_AccomFood = pd.read_csv(fake_data_AccomFood_filename)

# # Create fake data for Manufacturing
# if 1==1:
#     shock = torch.ones(S)
#     shock[2] = .5 # 
#     a_s_Manuf = a_s_pre * shock
    
#     equi_Manuf= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_Manuf,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_Manuf = equi_Manuf['w_g'] * psi_hat
#     pickle.dump(equi_Manuf,  open(root + "Data/dgp/dgp_equi_Manuf.p", "wb"))
      
    
#     fake_data_Manuf     = dgp(mle_data_filename, mle_data_sums, phi_Manuf,     mle_estimates['sigma_hat'], equi_Manuf,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_Manuf_filename = root + "Data/dgp/fake_data_Manuf_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_Manuf = fake_data_Manuf.append(fake_data_pre)
#     fake_data_Manuf.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_Manuf.to_csv(fake_data_Manuf_filename)
#     fake_data_Manuf = pd.read_csv(fake_data_Manuf_filename)

# # Create fake data for Electricity and gas, water, sewage, waste mgmt and decontaminations
# if 1==1:
#     shock = torch.ones(S)
#     shock[3] = .5 # Accomodations and food
#     a_s_Utilities = a_s_pre * shock
    
#     equi_Utilities= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_Utilities,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_Utilities = equi_Utilities['w_g'] * psi_hat
#     pickle.dump(equi_Utilities,  open(root + "Data/dgp/dgp_equi_Utilities.p", "wb"))
      
    
#     fake_data_Utilities     = dgp(mle_data_filename, mle_data_sums, phi_Utilities,     mle_estimates['sigma_hat'], equi_Utilities,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_Utilities_filename = root + "Data/dgp/fake_data_Utilities_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_Utilities = fake_data_Utilities.append(fake_data_pre)
#     fake_data_Utilities.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_Utilities.to_csv(fake_data_Utilities_filename)
#     fake_data_Utilities = pd.read_csv(fake_data_Utilities_filename)

# # Create fake data for Trade and repair of motor vehicles and motorcycles
# if 1==1:
#     shock = torch.ones(S)
#     shock[3] = .5 # Accomodations and food
#     a_s_Utilities = a_s_pre * shock
    
#     equi_Utilities= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_Utilities,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_Utilities = equi_Utilities['w_g'] * psi_hat
#     pickle.dump(equi_Utilities,  open(root + "Data/dgp/dgp_equi_Utilities.p", "wb"))
      
    
#     fake_data_Utilities     = dgp(mle_data_filename, mle_data_sums, phi_Utilities,     mle_estimates['sigma_hat'], equi_Utilities,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_Utilities_filename = root + "Data/dgp/fake_data_Utilities_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_Utilities = fake_data_Utilities.append(fake_data_pre)
#     fake_data_Utilities.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_Utilities.to_csv(fake_data_Utilities_filename)
#     fake_data_Utilities = pd.read_csv(fake_data_Utilities_filename)

# # Create fake data for Transport, storage and mail
# if 1==1:
#     shock = torch.ones(S)
#     shock[6] = .5 # 
#     a_s_Transport = a_s_pre * shock
    
#     equi_Transport= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_Transport,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_Transport = equi_Transport['w_g'] * psi_hat
#     pickle.dump(equi_Transport,  open(root + "Data/dgp/dgp_equi_Transport.p", "wb"))
      
    
#     fake_data_Transport     = dgp(mle_data_filename, mle_data_sums, phi_Transport,     mle_estimates['sigma_hat'], equi_Transport,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_Transport_filename = root + "Data/dgp/fake_data_Transport_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_Transport = fake_data_Transport.append(fake_data_pre)
#     fake_data_Transport.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_Transport.to_csv(fake_data_Transport_filename)
#     fake_data_Transport = pd.read_csv(fake_data_Transport_filename)
    
# # Create fake data for Information and communication
# if 1==1:
#     shock = torch.ones(S)
#     shock[8] = .5 # 
#     a_s_InfoComm = a_s_pre * shock
    
#     equi_InfoComm= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_InfoComm,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_InfoComm = equi_InfoComm['w_g'] * psi_hat
#     pickle.dump(equi_InfoComm,  open(root + "Data/dgp/dgp_equi_InfoComm.p", "wb"))
      
    
#     fake_data_InfoComm     = dgp(mle_data_filename, mle_data_sums, phi_InfoComm,     mle_estimates['sigma_hat'], equi_InfoComm,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_InfoComm_filename = root + "Data/dgp/fake_data_InfoComm_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_InfoComm = fake_data_InfoComm.append(fake_data_pre)
#     fake_data_InfoComm.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_InfoComm.to_csv(fake_data_InfoComm_filename)
#     fake_data_InfoComm = pd.read_csv(fake_data_InfoComm_filename)
    
# # Create fake data for Finance
# if 1==1:
#     shock = torch.ones(S)
#     shock[9] = .5 # 
#     a_s_Finance = a_s_pre * shock
    
#     equi_Finance= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_Finance,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_Finance = equi_Finance['w_g'] * psi_hat
#     pickle.dump(equi_Finance,  open(root + "Data/dgp/dgp_equi_Finance.p", "wb"))
      
    
#     fake_data_Finance     = dgp(mle_data_filename, mle_data_sums, phi_Finance,     mle_estimates['sigma_hat'], equi_Finance,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_Finance_filename = root + "Data/dgp/fake_data_Finance_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_Finance = fake_data_Finance.append(fake_data_pre)
#     fake_data_Finance.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_Finance.to_csv(fake_data_Finance_filename)
#     fake_data_Finance = pd.read_csv(fake_data_Finance_filename)
       
# # Create fake data for Real Estate
# if 1==1:
#     shock = torch.ones(S)
#     shock[10] = .5 # 
#     a_s_RealEstate = a_s_pre * shock
    
#     equi_RealEstate= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_RealEstate,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_RealEstate = equi_RealEstate['w_g'] * psi_hat
#     pickle.dump(equi_RealEstate,  open(root + "Data/dgp/dgp_equi_RealEstate.p", "wb"))
      
    
#     fake_data_RealEstate     = dgp(mle_data_filename, mle_data_sums, phi_RealEstate,     mle_estimates['sigma_hat'], equi_RealEstate,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_RealEstate_filename = root + "Data/dgp/fake_data_RealEstate_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_RealEstate = fake_data_RealEstate.append(fake_data_pre)
#     fake_data_RealEstate.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_RealEstate.to_csv(fake_data_RealEstate_filename)
#     fake_data_RealEstate = pd.read_csv(fake_data_RealEstate_filename)
   
# # Create fake data for Professional, scientific and technical, admin and complementary svcs
# if 1==1:
#     shock = torch.ones(S)
#     shock[11] = .5 # 
#     a_s_ProfSciTech = a_s_pre * shock
    
#     equi_ProfSciTech= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_ProfSciTech,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_ProfSciTech = equi_ProfSciTech['w_g'] * psi_hat
#     pickle.dump(equi_ProfSciTech,  open(root + "Data/dgp/dgp_equi_ProfSciTech.p", "wb"))
      
    
#     fake_data_ProfSciTech     = dgp(mle_data_filename, mle_data_sums, phi_ProfSciTech,     mle_estimates['sigma_hat'], equi_ProfSciTech,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_ProfSciTech_filename = root + "Data/dgp/fake_data_ProfSciTech_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_ProfSciTech = fake_data_ProfSciTech.append(fake_data_pre)
#     fake_data_ProfSciTech.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_ProfSciTech.to_csv(fake_data_ProfSciTech_filename)
#     fake_data_ProfSciTech = pd.read_csv(fake_data_ProfSciTech_filename)
       
# # Create fake data for Public admin, defense, educ and health and soc security
# if 1==1:
#     shock = torch.ones(S)
#     shock[12] = .5 # 
#     a_s_Public = a_s_pre * shock
    
#     equi_Public= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_Public,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_Public = equi_Public['w_g'] * psi_hat
#     pickle.dump(equi_Public,  open(root + "Data/dgp/dgp_equi_Public.p", "wb"))
      
    
#     fake_data_Public     = dgp(mle_data_filename, mle_data_sums, phi_Public,     mle_estimates['sigma_hat'], equi_Public,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_Public_filename = root + "Data/dgp/fake_data_Public_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_Public = fake_data_Public.append(fake_data_pre)
#     fake_data_Public.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_Public.to_csv(fake_data_Public_filename)
#     fake_data_Public = pd.read_csv(fake_data_Public_filename)
   
    
#     shock = torch.ones(S)
#     shock[11] = .5 # 
#     a_s_ProfSciTech = a_s_pre * shock
    
#     equi_ProfSciTech= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_ProfSciTech,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_ProfSciTech = equi_ProfSciTech['w_g'] * psi_hat
#     pickle.dump(equi_ProfSciTech,  open(root + "Data/dgp/dgp_equi_ProfSciTech.p", "wb"))
      
    
#     fake_data_ProfSciTech     = dgp(mle_data_filename, mle_data_sums, phi_ProfSciTech,     mle_estimates['sigma_hat'], equi_ProfSciTech,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_ProfSciTech_filename = root + "Data/dgp/fake_data_ProfSciTech_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_ProfSciTech = fake_data_ProfSciTech.append(fake_data_pre)
#     fake_data_ProfSciTech.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_ProfSciTech.to_csv(fake_data_ProfSciTech_filename)
#     fake_data_ProfSciTech = pd.read_csv(fake_data_ProfSciTech_filename)
       
# # Create fake data for Private Health and Education
# if 1==1:
#     shock = torch.ones(S)
#     shock[13] = .5 # 
#     a_s_PrivHealthEduc = a_s_pre * shock
    
#     equi_PrivHealthEduc= solve_model(eta,
#                 mle_data_sums['I'],
#                 mle_data_sums['G'],
#                 S,
#                 a_s_PrivHealthEduc,
#                 b_gs,
#                 mle_data_sums['m_i'],
#                 nu_hat = mle_estimates['nu_hat'],
#                 sigma_hat = mle_estimates['sigma_hat'],
#                 xi_hat = mle_estimates['xi_hat'],
#                 xi_outopt = xi_outopt,
#                 phi_outopt = phi_outopt,
#                 psi_hat = psi_hat,
#                 maxiter = 1e6,  # maximum number of iterations
#                 factor = 1e-3,  # dampening factor
#                 tol = 1e-4,     # precision level in the model solution
#                 decimals = 4,   # printed output rounding decimals
#                 silent = solve_GE_silently
#                 )
    
#     phi_PrivHealthEduc = equi_PrivHealthEduc['w_g'] * psi_hat
#     pickle.dump(equi_PrivHealthEduc,  open(root + "Data/dgp/dgp_equi_PrivHealthEduc.p", "wb"))
      
    
#     fake_data_PrivHealthEduc     = dgp(mle_data_filename, mle_data_sums, phi_PrivHealthEduc,     mle_estimates['sigma_hat'], equi_PrivHealthEduc,     2009, 2009, replaceyear='2014')
    
#     fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
#     fake_data_PrivHealthEduc_filename = root + "Data/dgp/fake_data_PrivHealthEduc_rio_2009_2012_level_" + str(level) + ".csv"
#     fake_data_PrivHealthEduc = fake_data_PrivHealthEduc.append(fake_data_pre)
#     fake_data_PrivHealthEduc.sort_values(by=['wid_masked','year'], inplace=True)
#     fake_data_PrivHealthEduc.to_csv(fake_data_PrivHealthEduc_filename)
#     fake_data_PrivHealthEduc = pd.read_csv(fake_data_PrivHealthEduc_filename)
   
# # Create fake data for Arts, culture, sports and recreation and other svcs
if 1==1:
    shock = torch.ones(S)
    shock[14] = .5 # 
    a_s_ArtsCultureSportsRec = a_s_pre * shock
    
    equi_ArtsCultureSportsRec= solve_model(eta,
                mle_data_sums['I'],
                mle_data_sums['G'],
                S,
                a_s_ArtsCultureSportsRec,
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
    
    phi_ArtsCultureSportsRec = equi_ArtsCultureSportsRec['w_g'] * psi_hat
    pickle.dump(equi_ArtsCultureSportsRec,  open(root + "Data/dgp/dgp_equi_ArtsCultureSportsRec.p", "wb"))
      
    
    fake_data_ArtsCultureSportsRec     = dgp(mle_data_filename, mle_data_sums, phi_ArtsCultureSportsRec,     mle_estimates['sigma_hat'], equi_ArtsCultureSportsRec,     2009, 2009, replaceyear='2014')
    
    fake_data_pre_filename = root + "Data/dgp/fake_data_pre_rio_2009_2012_level_" + str(level) + ".csv"
    fake_data_pre = pd.read_csv(fake_data_pre_filename)
    
    
    
    fake_data_ArtsCultureSportsRec_filename = root + "Data/dgp/fake_data_ArtsCultureSportsRec_rio_2009_2012_level_" + str(level) + ".csv"
    fake_data_ArtsCultureSportsRec = fake_data_ArtsCultureSportsRec.append(fake_data_pre)
    fake_data_ArtsCultureSportsRec.sort_values(by=['wid_masked','year'], inplace=True)
    fake_data_ArtsCultureSportsRec.to_csv(fake_data_ArtsCultureSportsRec_filename)
    fake_data_ArtsCultureSportsRec = pd.read_csv(fake_data_ArtsCultureSportsRec_filename)
   
    
   
equi_pre = pickle.load(open(root + "Data/dgp/dgp_equi_pre.p", "rb"))

# equi_const			        = pickle.load(  open(root + "Data/dgp/dgp_equi_const.p", "rb"))
equi_AccomFood           	= pickle.load(  open(root + "Data/dgp/dgp_equi_AccomFood.p", "rb")) # Use August version because regenerating data creates slight differences
# equi_Extractive          	= pickle.load(  open(root + "Data/dgp/dgp_equi_Extractive.p", "rb"))
# equi_Manuf          	    = pickle.load(  open(root + "Data/dgp/dgp_equi_Manuf.p", "rb"))
# equi_Utilities           	= pickle.load(  open(root + "Data/dgp/dgp_equi_Utilities.p", "rb"))
# equi_Vehicles            	= pickle.load(  open(root + "Data/dgp/dgp_equi_Vehicles.p", "rb"))
# equi_Transport           	= pickle.load(  open(root + "Data/dgp/dgp_equi_Transport.p", "rb"))
# equi_InfoComm            	= pickle.load(  open(root + "Data/dgp/dgp_equi_InfoComm.p", "rb"))
# equi_Finance             	= pickle.load(  open(root + "Data/dgp/dgp_equi_Finance.p", "rb"))
# equi_RealEstate          	= pickle.load(  open(root + "Data/dgp/dgp_equi_RealEstate.p", "rb"))
# equi_ProfSciTech         	= pickle.load(  open(root + "Data/dgp/dgp_equi_ProfSciTech.p", "rb"))
# equi_Public              	= pickle.load(  open(root + "Data/dgp/dgp_equi_Public.p", "rb"))
# equi_PrivHealthEduc      	= pickle.load(  open(root + "Data/dgp/dgp_equi_PrivHealthEduc.p", "rb"))
# equi_ArtsCultureSportsRec	= pickle.load(  open(root + "Data/dgp/dgp_equi_ArtsCultureSportsRec.p", "rb"))

# fake_data_const_filename		= root + 'Data/dgp/fake_data_const_rio_2009_2012_level_' + str(level) + '.csv'
fake_data_AccomFood_filename		= root + 'Data/dgp/fake_data_AccomFood_rio_2009_2012_level_' + str(level) + '.csv' # Use August version because regenerating data creates slight differences
# fake_data_Extractive_filename	    = root + 'Data/dgp/fake_data_Extractive_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_Manuf_filename	       	= root + 'Data/dgp/fake_data_Manuf_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_Utilities_filename		= root + 'Data/dgp/fake_data_Utilities_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_Vehicles_filename		= root + 'Data/dgp/fake_data_Vehicles_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_Transport_filename		= root + 'Data/dgp/fake_data_Transport_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_InfoComm_filename		= root + 'Data/dgp/fake_data_InfoComm_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_Finance_filename		= root + 'Data/dgp/fake_data_Finance_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_RealEstate_filename		= root + 'Data/dgp/fake_data_RealEstate_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_ProfSciTech_filename		= root + 'Data/dgp/fake_data_ProfSciTech_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_Public_filename		= root + 'Data/dgp/fake_data_Public_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_PrivHealthEduc_filename	= root + 'Data/dgp/fake_data_PrivHealthEduc_rio_2009_2012_level_' + str(level) + '.csv'
# fake_data_ArtsCultureSportsRec_filename = root + 'Data/dgp/fake_data_ArtsCultureSportsRec_rio_2009_2012_level_' + str(level) + '.csv'
   
stubs = ['const', 'AccomFood', 'Extractive', 'Manuf', 'Utilities', 'Vehicles', 'Transport', 'InfoComm', 'Finance', 'RealEstate', 'ProfSciTech', 'Public', 'PrivHealthEduc', 'ArtsCultureSportsRec']
for s in stubs:
    print("fake_data_",s,"_filename = root + 'Data/dgp/fake_data_",s,"_rio_2009_2012_level_' + str(level) + '.csv'")

from bartik_analysis import bartik_analysis

    
# bartik_analysis(fake_data_const_filename,    equi_shock=equi_const,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_const')
bartik_analysis(fake_data_AccomFood_filename,    equi_shock=equi_AccomFood,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_AccomFood')
# bartik_analysis(fake_data_Extractive_filename,    equi_shock=equi_Extractive,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_Extractive')
# bartik_analysis(fake_data_Utilities_filename,    equi_shock=equi_Utilities,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_Utilities')
# bartik_analysis(fake_data_Vehicles_filename,    equi_shock=equi_Vehicles,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_Vehicles')
# bartik_analysis(fake_data_Transport_filename,    equi_shock=equi_Transport,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_Transport')
# bartik_analysis(fake_data_InfoComm_filename,    equi_shock=equi_InfoComm,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_InfoComm')
# bartik_analysis(fake_data_Finance_filename,    equi_shock=equi_Finance,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_Finance')
# bartik_analysis(fake_data_RealEstate_filename,    equi_shock=equi_RealEstate,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_RealEstate')
# bartik_analysis(fake_data_ProfSciTech_filename,    equi_shock=equi_ProfSciTech,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_ProfSciTech')
# bartik_analysis(fake_data_Public_filename,    equi_shock=equi_Public,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_Public')
# bartik_analysis(fake_data_PrivHealthEduc_filename,    equi_shock=equi_PrivHealthEduc,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_PrivHealthEduc')
# bartik_analysis(fake_data_ArtsCultureSportsRec_filename,    equi_shock=equi_ArtsCultureSportsRec,  equi_pre=equi_pre, figuredir=figuredir, savefile_stub='fake_data_ArtsCultureSportsRec')




# # This analysis done on the version of '/Users/jfogel../Data/dgp/fake_data_const_rio_2009_2012_level_0.csv' last modified on August 2, 2021 at 12:37pm. 
   

from case_study_func import case_study

# fake_data_china_filename = root + "Data/dgp/fake_data_china_rio_2009_2012_level_" + str(level) + ".csv"
# equi_china = pickle.load(open(root + "Data/dgp/dgp_equi_china.p", "rb"))

# fake_data_const_filename = root + "Data/dgp/fake_data_const_rio_2009_2012_level_" + str(level) + ".csv"
# equi_const = pickle.load(open(root + "Data/dgp/dgp_equi_const.p", "rb"))



# case_study(mle_data_filename, fake_data_const_filename, equi_const, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_const', 5)
# case_study(mle_data_filename, fake_data_china_filename, equi_china, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_china', 3)
# case_study(mle_data_filename, fake_data_Extractive_filename, equi_Extractive, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_Extractive', 2)
# case_study(mle_data_filename, fake_data_Utilities_filename, equi_Utilities, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_Utilities', 4)
# case_study(mle_data_filename, fake_data_Vehicles_filename, equi_Vehicles, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_Vehicles', 6)
# case_study(mle_data_filename, fake_data_Transport_filename, equi_Transport, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_Transport', 7)
case_study(mle_data_filename, fake_data_AccomFood_filename, equi_AccomFood, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_AccomFood', 8)
# case_study(mle_data_filename, fake_data_InfoComm_filename, equi_InfoComm, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_InfoComm', 9)
# case_study(mle_data_filename, fake_data_Finance_filename, equi_Finance, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_Finance', 10)
# case_study(mle_data_filename, fake_data_RealEstate_filename, equi_RealEstate, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_RealEstate', 11)
# case_study(mle_data_filename, fake_data_ProfSciTech_filename, equi_ProfSciTech, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_ProfSciTech', 12)
# case_study(mle_data_filename, fake_data_Public_filename, equi_Public, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_Public', 13)
# case_study(mle_data_filename, fake_data_PrivHealthEduc_filename, equi_PrivHealthEduc, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_PrivHealthEduc', 14)
# case_study(mle_data_filename, fake_data_ArtsCultureSportsRec_filename, equi_ArtsCultureSportsRec, equi_pre, psi_hat, mle_estimates, figuredir, 'shock_case_study_ArtsCultureSportsRec', 15)

# Utilities could work. The most shocked workers are pretty much all street cleaners which makes sense. The problem is the most shocked iota is almost exclusively within the Utilities sector, as is the most shocked gamma, which doesn't illustrate our point about how shocks should be defined at the gamma level, not sector. 

# InfoComm looks promising 

# Finance too. The most shocked iota is mostly a bunch of bank clerks but the second one is a bunch of administrative jobs and only supplies 53% of its labor to Finance. 

# Real Estate is kinda useful cuz it shows that the most shocked iotas are mostly undifferentiated service sector workers like administrative assistants, office clerks, and janitors. The problem is it's not a super clean grouping of workers which doesn't highlight our ability to identify signal. 

# A shock to Public admin, defense, educ and health and soc security hits hardest an iota that is mostly doctors. However, this iota supplies most of its labor to Private health and Education. I think this is a story that makes quite a bit of sense and illustrates our story nicely. The problem is since Brazil has a national health care system, most doctors are probably public employees and therefore excluded from our data.

# The same iota (iota=19, mostly doctors) is the most-shocked iota for a shock to Public admin, defense, educ and health and soc security and to Private health and education. I think this might be worth exploring.

