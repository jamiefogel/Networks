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
output_file = harmonized_path + '/valid_cnae95.csv'

# Load the Parquet file and select the relevant CNAE variable
df = pd.read_parquet(input_file, engine='pyarrow', columns=['clas_cnae'])

# Drop rows with missing or invalid CNAE codes (if any)
df = df.dropna(subset=['clas_cnae'])

# Convert CNAE to integer format (if appropriate)
df['clas_cnae'] = df['clas_cnae'].astype(int)

# Create Division (2 digits) and Group (3 digits) columns
df['cnae952d_divisao'] = df['clas_cnae'] // 1000  # First 2 digits
df['cnae953d_grupo'] = df['clas_cnae'] // 100     # First 3 digits

# Rename columns for clarity
df = df.rename(columns={'clas_cnae': 'cnae95'})

# Save the resulting DataFrame to a CSV file
df.to_csv(output_file, index=False)

print(f"CSV file saved as: {output_file}")