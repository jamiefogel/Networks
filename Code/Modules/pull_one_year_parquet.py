# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 16:55:24 2024

@author: p13861161
"""



import os
import sys
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import getpass
import subprocess
import platform
import matplotlib.pyplot as plt
import gc
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.dataset as ds

def pull_one_year_parquet(year, occvar, savefile=None, municipality_codes=None, state_codes=None, nrows=None, othervars=None, age_upper=None, age_lower=None, parse_dates=None, filename=None):
    print('new version')
    print(year)
    now = datetime.now()
    currenttime = now.strftime('%H:%M:%S')
    print('Starting ', year, ' at ', currenttime)
    vars = ['pis','id_estab',occvar,'codemun','tipo_vinculo','idade'] 
    if othervars is not None:
        vars = vars + othervars
    # Use pyarrow dataset to filter data before loading into pandas
    temp = ds.dataset(filename, format='parquet')
    original_schema = temp.schema
    fields = []
    for field in original_schema:
        if field.name == 'codemun':
            fields.append(pa.field('codemun', pa.string()))
        else:
            fields.append(field)
    custom_schema = pa.schema(fields)
    dataset = ds.dataset(filename, format='parquet', schema=custom_schema)
    filter_expression = None
    state_code_expr_full = None 
    if municipality_codes is not None:
        filter_expression = pc.field('codemun').isin(municipality_codes) if filter_expression is None else filter_expression & pc.field('codemun').isin(municipality_codes)
    if state_codes is not None:
        for code in state_codes:
            state_code_str = str(code).zfill(2) 
            state_code_expr = pc.starts_with(pc.field('codemun'), state_code_str)
            state_code_expr_full = state_code_expr if state_code_expr_full is None else (state_code_expr_full | state_code_expr)
        filter_expression = state_code_expr_full if filter_expression is None else filter_expression & state_code_expr_full
    if age_lower is not None:
        filter_expression = pc.field('idade') > age_lower if filter_expression is None else filter_expression & (pc.field('idade') > age_lower)
    if age_upper is not None:
        filter_expression = pc.field('idade') < age_upper if filter_expression is None else filter_expression & (pc.field('idade') < age_upper)
    filter_expression = ~pc.field('tipo_vinculo').isin([30, 31, 35]) if filter_expression is None else filter_expression & ~pc.field('tipo_vinculo').isin([30, 31, 35])
    filter_expression = pc.field('pis').is_valid() if filter_expression is None else filter_expression & pc.field('pis').is_valid()
    filter_expression = pc.field('id_estab').is_valid() if filter_expression is None else filter_expression & pc.field('id_estab').is_valid()
    filter_expression = pc.field(occvar).is_valid() if filter_expression is None else filter_expression & pc.field(occvar).is_valid()
    table = dataset.to_table(columns=vars, filter=filter_expression)
    if nrows is not None:
        table = table.slice(0, nrows)
    raw_data = table.to_pandas()
    date_formats ={    
        2004:"%m/%d/%Y",
        2005:"%m/%d/%Y",
        2006:"%m/%d/%Y",
        2007:"%m/%d/%Y",
        2008:"%m/%d/%Y",
        2009:"%m/%d/%Y",
        2010:"%m/%d/%Y",
        2011:"%m/%d/%Y",
        2012:"%m/%d/%Y",
        2013:"%m/%d/%Y",
        2014:"%m/%d/%Y",
        2015:"%m/%d/%Y",
        2016:"%d/%m/%Y",
        2017:"%d%b%Y",
        2018:"%d/%m/%Y",
        2019:"%d/%m/%Y"}
    # Parse specified columns as dates
    if parse_dates is not None:
        for date_column in parse_dates:
            raw_data[date_column] = pd.to_datetime(raw_data[date_column], format=date_formats.get(year), errors='coerce')
    raw_data['year'] = year
    raw_data['yob'] = raw_data['year'] - raw_data['idade']
    raw_data[occvar] = raw_data[occvar].astype(str)
    raw_data['occ4'] = raw_data[occvar].str[0:4]
    raw_data['id_estab'] = raw_data['id_estab'].astype(str).str.zfill(14)
    raw_data['jid'] = raw_data['id_estab'] + '_' + raw_data['occ4']
    raw_data.rename(columns={'pis':'wid'}, inplace=True)
    raw_data['wid'] = raw_data['wid'].astype(str)
    if 'clas_cnae20' in othervars:
        raw_data['ind2'] = np.floor(raw_data['clas_cnae20']/1000).astype(int)
        raw_data['sector_IBGE'] = np.nan
        raw_data.loc[( 1<=raw_data['ind2']) & (raw_data['ind2'] <= 3), 'sector_IBGE'] = 1  
        raw_data.loc[( 5<=raw_data['ind2']) & (raw_data['ind2'] <= 9), 'sector_IBGE'] = 2 
        raw_data.loc[(10<=raw_data['ind2']) & (raw_data['ind2'] <=33), 'sector_IBGE'] = 3 
        raw_data.loc[(35<=raw_data['ind2']) & (raw_data['ind2'] <=39), 'sector_IBGE'] = 4 
        raw_data.loc[(41<=raw_data['ind2']) & (raw_data['ind2'] <=43), 'sector_IBGE'] = 5 
        raw_data.loc[(45<=raw_data['ind2']) & (raw_data['ind2'] <=47), 'sector_IBGE'] = 6 
        raw_data.loc[(49<=raw_data['ind2']) & (raw_data['ind2'] <=53), 'sector_IBGE'] = 7 
        raw_data.loc[(55<=raw_data['ind2']) & (raw_data['ind2'] <=56), 'sector_IBGE'] = 8 
        raw_data.loc[(58<=raw_data['ind2']) & (raw_data['ind2'] <=63), 'sector_IBGE'] = 9 
        raw_data.loc[(64<=raw_data['ind2']) & (raw_data['ind2'] <=66), 'sector_IBGE'] = 10
        raw_data.loc[(68<=raw_data['ind2']) & (raw_data['ind2'] <=68), 'sector_IBGE'] = 11
        raw_data.loc[(69<=raw_data['ind2']) & (raw_data['ind2'] <=82), 'sector_IBGE'] = 12
        raw_data.loc[(84<=raw_data['ind2']) & (raw_data['ind2'] <=84), 'sector_IBGE'] = 13
        raw_data.loc[(85<=raw_data['ind2']) & (raw_data['ind2'] <=88), 'sector_IBGE'] = 14
        raw_data.loc[(90<=raw_data['ind2']) & (raw_data['ind2'] <=97), 'sector_IBGE'] = 15 
    now = datetime.now()
    currenttime = now.strftime('%H:%M:%S')
    print('Finished ', year, ' at ', currenttime)
    if savefile is not None:
        pickle.dump(raw_data, open(savefile, "wb"))
    else:
        return raw_data
    del raw_data
    gc.collect()


'''
state_codes = [31, 33, 35]

for year in range(2010,2020):
    print(year)
    df_csv             = pull_one_year(year, 'cbo2002', state_codes=state_codes, age_lower=25, age_upper=55, othervars=['data_adm','data_deslig','tipo_salario','rem_dez_r','horas_contr','clas_cnae20','cnpj_raiz','grau_instr'], parse_dates=['data_adm','data_deslig'], nrows=None, filename= rais + 'csv/brasil'  + str(year) + '.csv')
    now = datetime.now()
    currenttime = now.strftime('%H:%M:%S')
    print('Finished csv at ', currenttime)
            
    df_parquet = pull_one_year_parquet(year, 'cbo2002', state_codes=state_codes, age_lower=25, age_upper=55, othervars=['data_adm','data_deslig','tipo_salario','rem_dez_r','horas_contr','clas_cnae20','cnpj_raiz','grau_instr'], parse_dates=['data_adm','data_deslig'], nrows=None, filename= rais + 'parquet_novos/brasil'  + str(year) + '.parquet')
    
    
    
    
    df_csv[df_parquet.columns].equals(df_parquet)
    for c in df_csv.columns:
        check = ((df_csv[c] == df_parquet[c]) | (pd.isna(df_csv[c]) & pd.isna(df_parquet[c]))).mean()
        print(c, check)
        if check!=1:
            print(c)
            ERROR
        

df_csv.data_deslig
df_parquet.data_deslig


df_csv.data_adm
df_parquet.data_adm
'''
