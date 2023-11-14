import os
import pandas as pd
import torch
import numpy as np

homedir = os.path.expanduser('~')



################################################################################
# Compute demand shifters a_s
################################################################################



# When we switch to the correct data for computing a_s the a_s's don't change that much:
    
#sector_data = pd.read_csv(homedir + "/Networks/Data/IBGE/Conta_da_producao_2002_2017_xls/sectors_v08172020.csv")
#a_s_new/a_s
#Out[41]: tensor([[1.0000, 0.7991, 0.9027, 1.1658, 1.1191, 0.9558, 0.9907, 1.0555, 1.0266, 1.2481, 1.1903, 1.0655, 1.0227, 1.0735, 1.1011]], dtype=torch.float64)

sector_data = pd.read_csv(root + "Data/raw/IBGE/Conta_da_producao_2002_2017_xls/sectors.csv")
sector_data = sector_data.loc[(sector_data['year']>2002)]

y_ts = sector_data.pivot_table(index=['year'], columns=['s'], values=['y_s'])
p_ts = sector_data.pivot_table(index=['year'], columns=['s'], values=['p_s'])

if S != y_ts.shape[1]:
    ValueError('Number of sectors do not agree.')

T = y_ts.shape[0]

# Choose the sector to normalize to 1
a_base = 1

# This normalizes a_1 to 1 in the pre-period but allows it to grow to reflect overall economic growth
production = p_ts.loc[pre,('p_s',  a_base)]**(1/eta) * y_ts.loc[pre,('y_s',  a_base)]

a_ts = (p_ts.values**(1/eta) * y_ts.values) / production

a_s = torch.tensor(a_ts[p_ts.index == pre,])

# 1   "Agriculture, livestock, forestry, fisheries and aquaculture"
# 2   "Extractive industries"
# 3   "Manufacturing industries"
# 4   "Electricity and gas, water, sewage, waste management and decontamination activities"
# 5   "Construction"
# 6   "Trade and repair of motor vehicles and motorcycles"
# 7   "Transport, storage and mail"
# 8   "Accommodation and food"
# 9   "Information and communication"
# 10  "Financial, insurance and related services"
# 11  "Real estate activities"
# 12  "Professional, scientific and technical, administrative activities and complementary services"
# 13  "Public administration, defense, education and health and social security"
# 14  "Private health and education"
# 15  "Arts, culture, sports and recreation and other service activities"

sector_labels = ["Agriculture, livestock, forestry, fisheries and aquaculture",
                  "Extractive industries",
                  "Manufacturing industries",
                  "Electricity and gas, water, sewage, waste mgmt and decontamination",
                  "Construction",
                  "Trade and repair of motor vehicles and motorcycles",
                  "Transport, storage and mail",
                  "Accommodation and food",
                  "Information and communication",
                  "Financial, insurance and related services",
                  "Real estate activities",
                  "Professional, scientific and technical, admin and complementary svcs",
                  "Public admin, defense, educ and health and soc security",
                  "Private health and education",
                  "Arts, culture, sports and recreation and other svcs"]


VA_df = pd.DataFrame(columns=['year', 'value_added', 'sector'])
for s in range(S):
    VA_df_temp = pd.read_excel(root + "Data/raw/IBGE/Conta_da_producao_2002_2017_xls/Tabela22.xls", sheet_name='Tabela22.2', usecols="A,F", skiprows=50, nrows=15, header=None, names=['year','value_added'])
    VA_df_temp['sector'] = s
    VA_df = VA_df.append(VA_df_temp)




# If we are doing the Olympics shock
#a_s_rio_shock = torch.tensor(a_ts[p_ts.index == post,])

covid_sectors = [8,15]

# https://fred.stlouisfed.org/series/LABSHPBRA156NRUG
#x_s = .56
x_s = .65

# normalization = .5



'''
We need:
    - Value added by sector time series  (This comes straight from IBGE)
    - Labor share by sector (Compute labor earnings from RAIS, divide by GDP from IBGE, then possiby rescale to have a mean labor share of ~.56)
    - Value of non-labor input by sector (maybe equivalent to capital stock)
    - Value of labor input. This is an output of our model (m_i*psi*w*P_ig or something like that) except we have the problem that this depends on z_s if we go with our current normalization. If we used the Grigsby normalization, we'd be fine.
'''
