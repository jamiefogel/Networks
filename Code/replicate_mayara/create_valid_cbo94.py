# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 12:34:56 2024

@author: p13861161
"""

import pandas as pd
from config import root, rais


# Set base directory paths
base_path =  root + "/Code/replicate_mayara"
monopsonies_path = f"{base_path}/monopsonies"
ipea_path = f"{base_path}/publicdata/IPEA/IPEA_minwage"
tariffs_path = f"{base_path}/publicdata/Tariffs"
harmonized_path = f"{base_path}/raisdictionaries/harmonized"
other_path = f"{base_path}/publicdata/other"



input_file = rais + 'parquet_novos/brasil1995.parquet'
output_file_cbo = harmonized_path + '/valid_cbo94.csv'

# Load the Parquet file and select the relevant CBO variable
df_cbo = pd.read_parquet(input_file, engine='pyarrow', columns=['cbo1994'])

# Drop rows with missing or invalid CBO codes (if any)
df_cbo = df_cbo.dropna(subset=['cbo1994'])

# Convert CBO to integer format (if appropriate)
df_cbo['cbo94'] = df_cbo['cbo1994'].astype(int)

# Create aggregated versions of CBO: 2 digits and 3 digits
df_cbo['cbo942d'] = df_cbo['cbo94'] // 1000  # First 2 digits
df_cbo['cbo943d'] = df_cbo['cbo94'] // 100   # First 3 digits

# Rename columns for clarity
df_cbo = df_cbo.drop(columns='cbo1994')

# Save the resulting DataFrame to a CSV file
df_cbo.to_csv(output_file_cbo, index=False)

print(f"CSV file saved as: {output_file_cbo}")