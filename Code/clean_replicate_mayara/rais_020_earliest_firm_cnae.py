'''
XX Some notes: I think the idea here is that we don't have cnae before 1995, so we need to try to find a cnae for each  firm using later years. Usually that will be the cnae95 variable that we have in the 1995-2000 data, but in some cases it seems like she uses CNAE2.0 from years after 2000. I am inclined to just comment these sections out for now though because it seems like it'll be super rare that a pre-1995 firm has no cnae for 1995-2000 and then magically has a cnae after 2000. But maybe I am missing something.

'''


import pandas as pd
import os
import numpy as np
from config import root
from spec_parser import parse_spec




#chosen_spec, market_vars, file_suffix, _3states = parse_spec(root)

# Paths
base_path = root + "/Code/clean_replicate_mayara"
monopsas_dir = root + "/Code/clean_replicate_mayara/monopsonies/sas"
crosswalk_path =  root + "/Code/replicate_mayara/monopsonies/sas"


# Year range
firstyear = 1986 #XX 1985
lastyear = 2014 #XX 2015
_3states = ''

# ----- 1. Build Firm-Level CNAE95 Data (Equivalent to %cnae) -----

# Loop through years 1985-2015, reading each RAIS file and extracting key variables. The try/except allows us to loop over years that don't exist without failing
firms_list = []
for year in range(firstyear, lastyear + 1):
    print(year)
    file_path = os.path.join(monopsas_dir, f"rais_mayara_pull_{year}{_3states}.parquet")
    if year < 1995:
        # For pre-1995, no cnaeclass95 is available – keep only firm id and year.
        df_temp = pd.read_parquet(file_path, columns=["cnpj_raiz"]).drop_duplicates()
        df_temp["cnae95"] = np.nan
    else:
        # For 1995 and later, rename clas_cnae to cnae95.
        # read cnaeclass95 if it exists
        try:
             df_temp = pd.read_parquet(file_path, columns=['cnpj_raiz','clas_cnae']).drop_duplicates()
             df_temp = df_temp[['cnpj_raiz', 'clas_cnae']].drop_duplicates().copy()
             df_temp = df_temp.rename(columns={'clas_cnae': 'cnae95'})
        except Exception as e:
            print(f"Could not read clas_cnae for {year} - skipping.")  
            continue
    df_temp['year'] = year
    firms_list.append(df_temp)
    
    

# Combine all yearly data into one DataFrame.
allfirms = pd.concat(firms_list, ignore_index=True)


# Load the valid CNAE95 codes file. Mayara has a list of "valid" CNAEs but when we merge below the only CNAEs that aren't matched are missing
valid_cnae95 = pd.read_parquet(f"{crosswalk_path}/valid_cnae95.parquet")  
valid_cnae95_codes = valid_cnae95["cnae95"].unique()

# Inner join to keep only records with valid CNAE95 codes.
valid_master = pd.merge(allfirms, valid_cnae95, on='cnae95', how='inner') 
valid_master = valid_master[['cnpj_raiz', 'year', 'cnae95']].drop_duplicates().sort_values(by=['cnpj_raiz', 'year'])

# Identify firms missing valid CNAE95.
missing_firms = allfirms[~allfirms['cnpj_raiz'].isin(valid_master['cnpj_raiz'].unique())].drop_duplicates(subset=['cnpj_raiz', 'cnae95']) 

# For each firm in the valid_master, keep only the earliest (first) record.
valid_master_first = valid_master.sort_values(by=['cnpj_raiz', 'year']).drop_duplicates(subset='cnpj_raiz', keep='first')
valid_master_first = valid_master_first.drop(columns=['year'])
rais_firm_cnae95_master = valid_master_first.copy()  # This is our master set from RAIS.



# ----- 2. Create the IBGE Subactivity to CNAE95 Crosswalk (Equivalent to %ibgecross) -----
# Above we caluclated the earliest CNAE for each firm. Here we calculate the earliest subativ_ibge for each firm. Then we merge the two on firm. Then we create a crosswalk subativ_ibge cnae by taking the modal cnae within each subativ_ibge. We use this to impute missing cnae95 for the years 1985 to 1994 when we don't have cnae.


# For years 1985 to 1994, get each firm’s earliest IBGE subactivity.
ibge_list = []
for year in range(1986, 1995):
    print(year)
    file_path = os.path.join(monopsas_dir, f"rais_mayara_pull_{year}{_3states}.parquet")
    df = pd.read_parquet(file_path, columns = ['cnpj_raiz', 'subativ_ibge']).drop_duplicates()
    df['year'] = year
    ibge_list.append(df)
    
subac = pd.concat(ibge_list, ignore_index=True)
# Remove rows with missing subativ_ibge.
subac = subac.dropna(subset=['subativ_ibge'])
# For each firm, keep the earliest record.
subac = subac.sort_values(by=['cnpj_raiz', 'year']).drop_duplicates(subset='cnpj_raiz', keep='first')  
subac = subac.drop(columns=['year'])

# Join with the earliest valid CNAE master on cnpj_raiz that we computed above (only firms that appear in both).
ibge_join = pd.merge(subac, rais_firm_cnae95_master, on='cnpj_raiz', how='inner')  

# Group by subativ_ibge and cnae95 and count the occurrences.
ibge_group = ibge_join.groupby(['subativ_ibge', 'cnae95']).size().reset_index(name='obs')
# For each subativ_ibge, pick the cnae95 with the highest count.
ibge_group = ibge_group.sort_values(by=['subativ_ibge', 'obs'], ascending=[True, False]) 
crosswalk_ibge = ibge_group.drop_duplicates(subset=['subativ_ibge'], keep='first').drop(columns=['obs'])




# ----- 3. Create the CNAE20 to CNAE95 Crosswalk (Equivalent to %cnae20cross) -----
'''
# XX I don't think we actually use any data after 2000 so I'm going to ignore this section
# For years 2006 to 2015 (except 2010), extract cnaeclass20 (renamed to cnae20) and cnaeclass95 (cnae95).
cnae20_list = []
for year in range(2006, 2016):
    if year == 2010:
        continue  # Skip 2010 as noted in the SAS script.
    file_path = os.path.join(monopsas_dir, f"rais{year}.csv")
    df = pd.read_csv(file_path)
    df_temp = df[['cnpj_raiz', 'cnaeclass20', 'cnaeclass95']].drop_duplicates().copy()
    df_temp = df_temp.rename(columns={'cnaeclass20': 'cnae20', 'cnaeclass95': 'cnae95'})
    df_temp['year'] = year
    cnae20_list.append(df_temp)
tempcnae = pd.concat(cnae20_list, ignore_index=True)
# Remove rows with missing cnae20 or cnae95.
tempcnae = tempcnae.dropna(subset=['cnae20', 'cnae95']).sort_values(by=['cnpj_raiz', 'year'])
# Group by cnae20 and cnae95, count occurrences.
cnae20_group = tempcnae.groupby(['cnae20', 'cnae95']).size().reset_index(name='obs')
cnae20_group = cnae20_group.sort_values(by=['cnae20', 'obs'], ascending=[True, False])
# For each cnae20, keep the mapping with the highest frequency.
crosswalk_cnae20 = cnae20_group.drop_duplicates(subset=['cnae20'], keep='first').drop(columns=['obs'])
'''


# ----- 4. Assign Missing CNAE95 Codes (Equivalent to %assign) -----

# (a) For years 1985 to 1994, get first subativ_ibge for missing firms.
# XX If a firm has multiple subativ_ibge in the first year we observe one, then we will choose between them arbitrarily
miss_ibge_list = []
for year in range(1986, 1995):
    print(year)
    file_path = os.path.join(monopsas_dir, f"rais_mayara_pull_{year}{_3states}.parquet")
    df = pd.read_parquet(file_path, columns = ['cnpj_raiz', 'subativ_ibge']).drop_duplicates()
    # Left join missing_firms to get the IBGE subactivity for that firm-year
    df_temp = pd.merge(missing_firms[['cnpj_raiz']], df[['cnpj_raiz', 'subativ_ibge']], on='cnpj_raiz', how='left') 
    df_temp['year'] = year
    miss_ibge_list.append(df_temp)
miss_ibge = pd.concat(miss_ibge_list, ignore_index=True)
miss_ibge = miss_ibge.dropna(subset=['subativ_ibge'])
miss_ibge = miss_ibge.sort_values(by=['cnpj_raiz', 'year']).drop_duplicates(subset='cnpj_raiz', keep='first') 
miss_ibge = miss_ibge.drop(columns=['year'])

'''
# XX Again, ignore post-2000
# (b) For years 2006 to 2015 (except 2010), get cnae20 for missing firms.
miss_cnae20_list = []
for year in range(2006, 2016):
    if year == 2010:
        continue
    file_path = os.path.join(monopsas_dir, f"rais{year}.csv")
    df = pd.read_csv(file_path)
    df_temp = pd.merge(missing_firms[['cnpj_raiz']], df[['cnpj_raiz', 'cnaeclass20']], on='cnpj_raiz', how='left')
    df_temp = df_temp.rename(columns={'cnaeclass20': 'cnae20'})
    df_temp['year'] = year
    miss_cnae20_list.append(df_temp)
miss_cnae20 = pd.concat(miss_cnae20_list, ignore_index=True)
miss_cnae20 = miss_cnae20.dropna(subset=['cnae20'])
miss_cnae20 = miss_cnae20.sort_values(by=['cnpj_raiz', 'year']).drop_duplicates(subset='cnpj_raiz', keep='first')
miss_cnae20 = miss_cnae20.drop(columns=['year'])

# (c) Combine IBGE and CNAE20 alternative codes.
misslinks = pd.merge(miss_ibge, miss_cnae20, on='cnpj_raiz', how='outer')

# (d) Assign CNAE95 using the crosswalks:
# First, join with the CNAE20-to-CNAE95 crosswalk.
assign_df = pd.merge(misslinks, crosswalk_cnae20, on='cnae20', how='left', suffixes=('', '_from_cnae20'))
# Next, join with the IBGE crosswalk (cnae95 to subativ_ibge; subativ_ibge is unique).
assign_df = pd.merge(assign_df, crosswalk_ibge, on='subativ_ibge', how='left', suffixes=('', '_from_ibge'))


# Use the CNAE95 from the CNAE20 crosswalk if available; otherwise use the IBGE crosswalk.
assign_df['cnae95_assigned'] = 1

assign_df['cnae95_final'] = assign_df['cnae95'].combine_first(assign_df['cnae95_from_ibge'])
assign_missing = assign_df[['cnpj_raiz', 'cnae95_final', 'cnae95_assigned']].rename(columns={'cnae95_final': 'cnae95'})
''' 

# XX JSF Added these next lines to deal with the fact that i commented out all the merges above
misslinks = miss_ibge.copy()
assign_df = misslinks.copy()  
assign_df = pd.merge(assign_df, crosswalk_ibge, on='subativ_ibge', how='left', suffixes=('', '_from_ibge'))

assign_df['cnae95_assigned'] = 1
assign_missing = assign_df[['cnpj_raiz', 'cnae95', 'cnae95_assigned']]


# Mark the original valid assignments as not assigned (flag 0).
valid_master_assign = rais_firm_cnae95_master.copy()
valid_master_assign['cnae95_assigned'] = 0

# (e) Combine the two to form the final master dataset.
master_plus = pd.concat([valid_master_assign, assign_missing], ignore_index=True)

# ----- 5. Export the Final Dataset -----

output_path = os.path.join(monopsas_dir, "rais_firm_cnae95_master_plus.parquet")
master_plus.to_parquet(output_path, index=False)

print("Master dataset with assigned CNAE95 codes has been exported.")
