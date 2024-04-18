# Check various things about the raw RAIS data

import pandas as pd
import csv
import os

def detect_delimiter(filename):
    # Expand the tilde in the filename
    filename = os.path.expanduser(filename)    
    with open(filename, 'r') as file:
        dialect = csv.Sniffer().sniff(file.readline())
    return dialect.delimiter

# Which CNAE variables exist in each year?
for year in range(1986, 2011):
    print(year)
    # Construct the filename
    filename = '~/rais/RAIS/csv/brasil' + str(year) + '.csv'    
    # Detect the delimiter
    delimiter = detect_delimiter(filename)
    df = pd.read_csv(filename, nrows=5, delimiter=delimiter)    
    # Print the names of the variables that contain "clas_cnae"
    for column in df.columns:
        if 'clas_cnae' in column:
            print(column)
 
    

# Counts of subs_ibge by year
dfs = []
for year in range(1986, 2011):
    print(year)
    # Construct the filename
    filename = '~/rais/RAIS/csv/brasil' + str(year) + '.csv'    
    # Detect the delimiter
    delimiter = detect_delimiter(filename)
    df = pd.read_csv(filename, nrows=None, delimiter=delimiter, usecols=['subs_ibge'])
    df['year'] = year
    print(year)
    print(df.subs_ibge.value_counts())
    dfs.append(df)

subs_ibge_df = pd.concat(dfs, ignore_index=True)    
