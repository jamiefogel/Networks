# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 18:21:09 2024

@author: p13861161
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

###
# Check which years have both CBO1994 and CBO2002

# Define the years of interest
years = range(1990, 2010)  # Modify this range according to your actual data years

# Loop over each year
for year in years:
    filename = f'//storage6/bases/DADOS/RESTRITO/RAIS/csv/brasil{year}.csv'
    
    if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
        sep = ';'
    else:
        sep = ','
    # Read the CSV file
    try:
        df = pd.read_csv(filename, nrows=10, sep=sep)
        
        # Find columns that contain 'cbo'
        cbo_columns = [col for col in df.columns if 'cbo' in col.lower()]
        
        # Print the year and the relevant columns
        print(f"Year: {year}, CBO Columns: {cbo_columns}")
    except Exception as e:
        print(f"Failed to process file for year {year}. Error: {e}")


filename = '//storage6/bases/DADOS/RESTRITO/RAIS/csv/brasil2007.csv'
raw_data = pd.read_csv(filename, usecols=['cbo1994','cbo2002'], encoding='ISO-8859-1')    
raw_data1990 = pd.read_csv( '//storage6/bases/DADOS/RESTRITO/RAIS/csv/brasil1990.csv', usecols=['cbo1994'], encoding='ISO-8859-1', sep=';')    


# Create the frequency table
crosswalk = raw_data.groupby(['cbo1994', 'cbo2002']).size().reset_index(name='count')



############# 
# Modal


# Determine the modal cbo2002 for each cbo1994
modal_cbo2002 = crosswalk.groupby('cbo1994').apply(
    lambda x: x.loc[x['count'].idxmax()]
).reset_index(drop=True)

# Calculate the total counts for each cbo1994
total_counts = crosswalk.groupby('cbo1994')['count'].sum().reset_index(name='total_count')

# Merge to find fractions for modal cbo2002
modal_cbo2002 = modal_cbo2002.merge(total_counts, on='cbo1994')
modal_cbo2002['fraction'] = modal_cbo2002['count'] / modal_cbo2002['total_count']

# Output results
print("Modal cbo2002 Mappings and Fractions:")
print(modal_cbo2002[['cbo1994', 'cbo2002', 'fraction']])


modal_cbo2002[['cbo1994','cbo2002']].to_pickle('//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Data/temp/cbo_1994to2002_cw.p')
modal_cbo2002[['cbo1994','cbo2002']].to_csv('//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Data/temp/cbo_1994to2002_cw.csv')



#########################################################################################################
# Checking the quality of the crosswalk
#########################################################################################################




# Unweighted summary statistics for 'fraction'
unweighted_stats = modal_cbo2002['fraction'].describe(percentiles = [0.01, 0.05, 0.10, 0.25, 0.5] )
print("Unweighted Summary Statistics for 'fraction':")
print(unweighted_stats)


def weighted_percentile(data, weights, percentile):
    """
    Compute the weighted percentile of a 1D numpy array.
    """
    series = np.array(data)
    series_weights = np.array(weights)

    # Sort the series and corresponding weights
    sorted_indices = np.argsort(series)
    sorted_data = series[sorted_indices]
    sorted_weights = series_weights[sorted_indices]

    # Compute the cumulative sum of weights
    cumulative_sum_of_weights = np.cumsum(sorted_weights)

    # Normalize the cumulative weights
    normalized_cumulative_weights = cumulative_sum_of_weights / cumulative_sum_of_weights[-1]

    # Find the place where the normalized cumulative weight exceeds the percentile
    percentile_index = np.where(normalized_cumulative_weights >= percentile)[0][0]
    return sorted_data[percentile_index]

# Calculate weighted percentiles
p1_weighted = weighted_percentile(modal_cbo2002['fraction'], modal_cbo2002['total_count'], 0.01)
p5_weighted = weighted_percentile(modal_cbo2002['fraction'], modal_cbo2002['total_count'], 0.05)
p10_weighted = weighted_percentile(modal_cbo2002['fraction'], modal_cbo2002['total_count'], 0.10)

print("Weighted Percentiles for 'fraction':")
print(f"1st Percentile: {p1_weighted}")
print(f"5th Percentile: {p5_weighted}")
print(f"10th Percentile: {p10_weighted}")


# Assuming modal_cbo2002 is already defined and includes the necessary columns
# Histogram of modal fractions
plt.figure(figsize=(10, 6))
plt.hist(modal_cbo2002['fraction'], bins=30, color='blue', edgecolor='black')
plt.title('Histogram of Modal cbo2002 Fractions')
plt.xlabel('Fraction')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()


# Weighted histogram of modal fractions
plt.figure(figsize=(10, 6))
plt.hist(modal_cbo2002['fraction'], bins=30, weights=modal_cbo2002['total_count'], color='green', edgecolor='black')
plt.title('Weighted Histogram of Modal cbo2002 Fractions')
plt.xlabel('Fraction')
plt.ylabel('Weighted Frequency')
plt.grid(True)
plt.show()


# cbo1994 codes with no corresponding cbo2002
cbo1994_codes = [21140, 21120, 21135, 21130]
'''
"2-11.20","Senators"
"2-11.30","Federal deputies"
"2-11.35","State deputies"
"2-11.40","Councilors"
'''
# Filter the DataFrame
filtered_data = raw_data[raw_data['cbo1994'].isin(cbo1994_codes)]
filtered_data.cbo2002.value_counts()
