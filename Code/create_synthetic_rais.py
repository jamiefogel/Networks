# To do
# - Restrict to 3 states
# - Is there a faster way to load the first 100000 wids without loading the full data set?
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import getpass
import sys

homedir = os.path.expanduser('~')
if getpass.getuser()=='p13861161':
    print("Running on Linux")
    root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
elif getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'

sys.path.append(root + 'Code/Modules')
figuredir = root + 'Results/'

os.chdir(root)



def try_read_csv(filepath, usecols, sep=','):
    """Attempt to read a CSV file. If the CSV is not comma-delimited, it tries semicolon."""
    try:
        # Try reading just one row to detect the delimiter
        data = pd.read_csv(filepath, usecols=usecols, sep=sep, nrows=1)
        if len(data.columns) == 1:
            raise ValueError("Likely wrong delimiter")
        # Read the full file with the detected delimiter
        return pd.read_csv(filepath, usecols=usecols, sep=sep, parse_dates=['data_adm','data_deslig'])
    except:
        # If comma delimiter failed, read the full file with semicolon delimiter
        return pd.read_csv(filepath, usecols=usecols, sep=';', parse_dates=['data_adm','data_deslig'])


# Parameters
file_path_template = '~/rais/RAIS/csv/brasil{}.csv'
columns = ['id_estab', 'cbo2002', 'codemun', 'data_adm', 'pis', 
           'tipo_vinculo', 'idade', 'grau_instr', 'rem_med_r', 'clas_cnae20', 
           'data_deslig', 'tipo_salario', 'rem_dez_r', 'horas_contr', 
           'uf', 'salario']
sample_size = 1000000
years = list(range(2008, 2020))

# 1. Extract sample PIS values from the first year
first_year_data = try_read_csv(file_path_template.format(years[0]), columns)
sample_pis_values = first_year_data['pis'].sample(sample_size).unique()

# Create a dictionary to store dataframes for each year
yearly_data = {}

# 2. Extract the same PIS values for each subsequent year
for year in years:
    yearly_data[year] = try_read_csv(file_path_template.format(year), columns)
    yearly_data[year] = yearly_data[year][yearly_data[year]['pis'].isin(sample_pis_values)]


numeric_cols = ['idade', 'rem_med_r', 'rem_dez_r', 'horas_contr', 'salario']
categorical_cols = ['id_estab', 'cbo2002', 'codemun', 'tipo_vinculo', 'grau_instr', 'clas_cnae20', 'tipo_salario', 'uf']
date_cols = ['data_adm', 'data_deslig']
date_format = "%m/%d/%Y"

yearly_data_new = yearly_data    
# For each year's data
for year, data in yearly_data_new.items():
    print(year)
    print(data.head())
    # a) Numeric Data
    for col in numeric_cols:
        mean = data[col].mean()
        std = data[col].std()        
        # Check if mean or std is NaN
        if pd.notna(mean) and pd.notna(std):
            data[col] = np.random.normal(mean, std, size=len(data))
        else:
            # If mean or std is NaN, fill the column with zeros or any other default value
            data[col] = 0
    # b) Categorical Data
    for col in categorical_cols:
        data[col] = data[col].sample(frac=1).reset_index(drop=True)
    # c) Dates
    print(yearly_data[2008].data_adm)
    print(data.data_adm)
    for col in date_cols:
        # Convert 'date' column to datetime if necessary
        if not isinstance(data[col].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype):
            data[col] = pd.to_datetime(data[col])
        start_date = data[col].dropna().min()
        end_date = data[col].dropna().max()
        date_range = (end_date - start_date).days
        random_dates = [(start_date + timedelta(days=np.random.randint(0, date_range))) for _ in range(len(data))]
        data[col] = random_dates
    # d) Unique IDs
    data['pis'] = range(1, 1 + len(data))
    # Store the modified data back in the dictionary
    yearly_data_new[year] = data
    yearly_data_new[year].to_csv(root + './Data/raw/synthetic_data_' + str(year) +'.csv', index=False)    


