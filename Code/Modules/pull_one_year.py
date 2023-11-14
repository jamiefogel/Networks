


from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import gc

def pull_one_year(year, occvar, savefile=None, municipality_codes=None, state_codes=None, nrows=None, othervars=None, age_upper=None, age_lower=None, parse_dates=None, filename=None):
    print('new version')
    print(year)
    now = datetime.now()
    currenttime = now.strftime('%H:%M:%S')
    print('Starting ', year, ' at ', currenttime)
    if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
        sep = ';'
    else:
        sep = ','
    vars = ['pis','id_estab',occvar,'codemun','tipo_vinculo','idade'] 
    if othervars is not None:
        vars = vars + othervars
    if filename is None:
        filename = '~/rais/RAIS/csv/brasil' + str(year) + '.csv'
    # 2009 csv is corrupted so use Stata instead
    if year!=2009:
        raw_data = pd.read_csv(filename, usecols=vars, sep=sep, dtype={'id_estab':str, 'pis':str, occvar:str}, nrows=nrows, parse_dates=parse_dates)
    elif year==2009:
        filename_stata = filename.replace('.csv','.dta').replace('/csv/','/Stata/')
        raw_data = pd.read_stata(filename_stata, columns=vars)
    if municipality_codes is not None:
        raw_data = raw_data[raw_data['codemun'].isin(municipality_codes)]   
    if state_codes is not None:
        raw_data = raw_data[raw_data['codemun'].fillna(99).astype(str).str[:2].astype('int').isin(state_codes)]
    if age_lower is not None:
        raw_data = raw_data.loc[raw_data.idade>age_lower]
    if age_upper is not None:
        raw_data = raw_data.loc[raw_data.idade<age_upper]
    raw_data = raw_data.dropna(subset=['pis','id_estab',occvar])
    raw_data = raw_data[~raw_data['tipo_vinculo'].isin([30,31,35])]
    raw_data['year'] = year
    raw_data['yob'] = raw_data['year'] - raw_data['idade']
    raw_data['occ4'] = raw_data[occvar].str[0:4]
    raw_data['jid'] = raw_data['id_estab'] + '_' + raw_data['occ4']
    #raw_data['jid6'] = raw_data['id_estab'] + '_' + raw_data['cbo2002']
    raw_data.rename(columns={'pis':'wid'}, inplace=True)
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
    if savefile is not None:
        pickle.dump( raw_data, open(savefile, "wb" ) )
    else:
        return raw_data
    del raw_data
    gc.collect()

