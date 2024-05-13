# -*- coding: utf-8 -*-
"""
Created on Mon May 13 20:40:18 2024

@author: p13861161
"""


from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import platform
import sys
import getpass

# Figure out system we're running on, define paths, and only load packages that exist in the relevant environment
homedir = os.path.expanduser('~')
os_name = platform.system()
if getpass.getuser()=='p13861161':
    if os_name == 'Windows':
        print("Running on Windows") 
        root = "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/"
        rais = "//storage6/bases/DADOS/RESTRITO/RAIS/"
        sys.path.append(root + 'Code/Modules')
    elif os_name == 'Linux':
        print("Running on Linux") 
        root = "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/"
        rais = "~/rais/RAIS/"
        sys.path.append(root + 'Code/Modules')
        # These all require torch
        import torch
        from torch_mle import torch_mle
        import bisbm
        from mle_load_fulldata import mle_load_fulldata
        from normalization_k import normalization_k
        from alphas_func import load_alphas
        import solve_model_functions as smf
        from correlogram import correlogram

if getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'
    sys.path.append(root + 'Code/Modules')



keepcols = ['id_estab','cnpj_raiz','data_abertura', 'data_baixa', 'data_encerramento', 'ind_atividade', 'ind_rais_neg', 'qt_vinc_ativos', 'qt_vinc_clt', 'qt_vinc_estat', 'regiao_metro',  'subativ_ibge', 'subs_ibge', 'tamestab', 'tipo_estab', 'uf']



# Function to read CSV with either a comma or semicolon delimiter
def read_csv_with_delimiter(filepath, nrows, encoding):
    try:
        df = pd.read_csv(filepath, delimiter=',', nrows=nrows, encoding=encoding)
        return df
    except pd.errors.ParserError:
        df = pd.read_csv(filepath, delimiter=';', nrows=nrows, encoding=encoding)
        return df


output_file = root + 'Logs/RAIS_info_for_ChatGPT.txt'

# Open the file in write mode
with open(output_file, 'w') as f:
    f.write('Output for employer data\n ')
    nrows = 10
    for year in range(2013, 2019):
        f.write(f"Year: {year}\n")
        df = pd.read_csv(rais + f'csv/estab{year}.csv', delimiter=';', nrows=nrows, encoding='latin1')
        
        f.write("Columns:\n")
        f.write(f"{df.columns}\n")
        
        f.write("Data Types:\n")
        f.write(f"{df.dtypes}\n")
        
        f.write("Describe:\n")
        f.write(f"{df.describe()}\n")
        
        f.write("\n\n")  # Add a new line for separation between years


    f.write('Output for worker data\n ')
    nrows = 10
    for year in range(2013, 2019):
        f.write(f"Year: {year}\n")
        if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
            sep = ';'
        else:
            sep = ','
        df = pd.read_csv(rais + f'csv/brasil{year}.csv', nrows=nrows, encoding='latin1', sep=sep)
        
        f.write("Columns:\n")
        f.write(f"{df.columns}\n")
        
        f.write("Data Types:\n")
        f.write(f"{df.dtypes}\n")
        
        f.write("Describe:\n")
        f.write(f"{df.describe()}\n")
        
        f.write("\n\n")  # Add a new line for separation between years


