import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 1. Load the original data
file_path = '~/rais/RAIS/csv/brasil2015.csv'
columns = ['id_estab', 'cbo2002', 'codemun', 'data_adm', 'pis', 'tipo_vinculo', 'idade', 'grau_instr', 'rem_med_r', 'clas_cnae20', 'data_deslig', 'data_nasc', 'tipo_salario', 'rem_dez_r', 'horas_contr', 'uf', 'salario']

nrows = 1000000


original_data = pd.read_csv(file_path, usecols=columns, nrows=nrows)

# 2. Create the synthetic dataset

# a) Numeric Data
numeric_cols = ['idade', 'rem_med_r', 'rem_dez_r', 'horas_contr', 'salario']
for col in numeric_cols:
    mean = original_data[col].mean()
    std = original_data[col].std()
    original_data[col] = np.random.normal(mean, std, size=len(original_data))

# b) Categorical Data
categorical_cols = ['id_estab', 'cbo2002', 'codemun', 'tipo_vinculo', 'grau_instr', 'clas_cnae20', 'tipo_salario', 'uf']
for col in categorical_cols:
    original_data[col] = original_data[col].sample(frac=1).reset_index(drop=True)

# c) Dates
date_format = "%m/%d/%Y"
date_cols = ['data_adm', 'data_deslig', 'data_nasc']
for col in date_cols:
    start_date = datetime.strptime(original_data[col].dropna().min(), date_format)
    end_date = datetime.strptime(original_data[col].dropna().max(), date_format)
    date_range = (end_date - start_date).days
    random_dates = [(start_date + timedelta(days=np.random.randint(0, date_range))).strftime(date_format) for _ in range(len(original_data))]
    original_data[col] = random_dates

# d) Unique IDs
original_data['pis'] = range(1, 1 + len(original_data))

# 3. Save the synthetic dataset
original_data.to_csv('./Data/raw/synthetic_data.csv', index=False)
