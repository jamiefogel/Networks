

from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc

import bisbm
from pull_one_year import pull_one_year


homedir = os.path.expanduser('~')
os.chdir(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/code/')

run_sbm = False

state_codes = [31, 33, 35]
rio_codes = [330045, 330170, 330185, 330190, 330200, 330227, 330250, 330285, 330320, 330330, 330350, 330360, 330414, 330455, 330490, 330510, 330555, 330575]
region_codes = pd.read_csv(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/external/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})

firstyear = 2013
lastyear = 2016

#maxrows = 100000
maxrows=None

#modelname='junk'
modelname = 'mass_layoffs_3states_'+str(firstyear)+'to'+str(lastyear)
# 2004 appears to be the first year in which we have job start end dates (data_adm and data_deslig)

# CPI: 06/2015=100
cpi = pd.read_csv(homedir + '/labormkt/labormkt_rafaelpereira/ExternalData/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
cpi['cpi'] = cpi.cpi/100
cpi['date'] = cpi['date'].dt.to_period('M')


gammas = pd.read_csv('../data/model_3states_2013to2016_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
gammas['gamma'] = gammas.gamma.fillna(-1)
earn_vars = ['vl_rem_01', 'vl_rem_02', 'vl_rem_03', 'vl_rem_04', 'vl_rem_05', 'vl_rem_06', 'vl_rem_07', 'vl_rem_08', 'vl_rem_09', 'vl_rem_10', 'vl_rem_11', 'vl_rem_12', 'vl_rem_13_adiant', 'vl_rem_13_final']

const_vars = ['wid', 'jid', 'codemun', 'cbo2002', 'causa_deslig', 'start_date', 'end_date', 'year','clas_cnae20', 'yob','uf']

########################################################################################
########################################################################################
# Create a wid-jid-month panel
########################################################################################
########################################################################################

for year in range(firstyear,lastyear+1):
    raw = pull_one_year(year, 'cbo2002', othervars=['data_adm','data_deslig','causa_deslig','clas_cnae20','uf']+earn_vars, state_codes=state_codes, age_lower=25, age_upper=55, nrows=maxrows)
    # Deflate
    raw['months_earn_gt0'] = (raw[earn_vars].drop(columns=['vl_rem_13_adiant', 'vl_rem_13_final']).gt(0)).sum(axis=1)
    raw['vl_rem_13'] = raw[['vl_rem_13_adiant', 'vl_rem_13_final']].sum(axis=1)
    raw['causa_deslig'] = raw.causa_deslig.fillna(-1).astype('int16')
    # Allocate the 13th payment equally over all months with positive earnings
    for i in earn_vars[0:12]:                                         
        cond = ((raw[i]>0) & raw['vl_rem_13']>0)
        raw.loc[cond,i] = raw.loc[cond,i] + raw.loc[cond,'vl_rem_13'] / raw.loc[cond,'months_earn_gt0']
    raw['start_date'] = pd.to_datetime(raw['data_adm'])
    raw['end_date']   = pd.to_datetime(raw['data_deslig'])
    raw.drop(columns=['id_estab','occ4','data_adm','data_deslig','idade', 'tipo_vinculo', 'vl_rem_13_adiant', 'vl_rem_13_final', 'vl_rem_13','months_earn_gt0'], inplace=True) # Can extract id_estab and occ4 from jid if necessary. This saves space
    # Reshape monthly earnings from wide to long
    data_monthly = raw.melt(id_vars=const_vars, var_name='month', value_name='monthly_earnings')
    data_monthly['month'] = data_monthly.month.replace('vl_rem_','', regex=True).astype('int')
    del raw
    # Identify first and last day of the month for checking whether the worker was employed in that month
    data_monthly['month_start'] = pd.to_datetime((data_monthly.year*10000+data_monthly.month*100+1).apply(str),format='%Y%m%d')
    data_monthly['month_end'] = (data_monthly['month_start'] + pd.offsets.MonthEnd())
    # Keep only months in which worker is employed in this job (make panel unbalanced). Done to save memory
    data_monthly = data_monthly.loc[(data_monthly.start_date<=data_monthly.month_end) & ((data_monthly.end_date>=data_monthly.month_start) | (data_monthly.end_date.isna()==True))]
    data_monthly.drop(columns=['month_start','month_end'], inplace=True)
    data_monthly['date'] = pd.to_datetime(data_monthly[['year', 'month']].assign(day=1)).dt.to_period('M')
    data_monthly.drop(columns=['month','year'], inplace=True)
    # Merge on meso region codes and gammas so that we can define markets
    data_monthly = data_monthly.merge(muni_meso_cw, how='left', on='codemun', copy=False) # validate='m:1', indicator=True)
    data_monthly['occ2Xmeso'] = data_monthly.cbo2002.str[0:2] + '_' + data_monthly['code_meso'].astype('str')
    data_monthly = data_monthly.merge(gammas, on='jid', how='left')
    # Deflate monthly earnings
    data_monthly['gamma'] = data_monthly.gamma.fillna(-1)
    data_monthly = data_monthly.merge(cpi, on='date', how='left')
    data_monthly['monthly_earnings'] = data_monthly.monthly_earnings/data_monthly.cpi
    data_monthly.drop(columns='cpi',inplace=True)
    if year==firstyear:
        df = data_monthly
    else:
        df = pd.concat([df,data_monthly])
    del data_monthly
    gc.collect()

''' At this point df is an unbalanced panel of worker-job-months where months in which the worker is not employed are dropped to save memory. It has the following columns:
       'wid', 'jid', 'codemun', 'cbo2002', 'causa_deslig', 'start_date',
       'end_date', 'clas_cnae20', 'yob', 'uf', 'monthly_earnings', 'date',
       'code_meso', 'occ2Xmeso', 'gamma'
'''

# Flag months in which a job started or ended. This will allow us to identify layoffs. Then flag layoffs.
# causa_deslig=11 implies layoff, however many quits may be labeled as layoffs in order to receive UI because Brazil does not have the same sort of experience rating as the US does.
for i in ['start','end']:
    df['job_' + i + '_flag'] = df[i + '_date'].dt.to_period('M')==df.date

df['layoff'] = ((df.causa_deslig==11) & (df.job_end_flag==1))
df['employed_in_month'] = 1

df.sort_values(by=['wid','jid','date'],inplace=True)
gc.collect()
df = df.reset_index().drop(columns='index')

# XX Temp fix because of the issue with UF in 2015
df['uf'] = df.codemun.astype(str).str[0:2]

# BM: we can't drop cbo2002, as we use it in the matching code down below
#df.drop(columns=['codemun','cbo2002','causa_deslig','code_meso'], inplace=True)
df.drop(columns=['codemun','causa_deslig','code_meso'], inplace=True)
df.to_pickle('../dump/' + modelname + '_df.p')
#df = pd.read_pickle('../dump/' + modelname + '_df.p')
gc.collect()



########################################################################################
########################################################################################
# Compute monthly distributions of earnings and other variables.
#    - We will use these for matching treatment and control markets
########################################################################################
########################################################################################


exec(open("matching.py").read())



####################################################################################
####################################################################################
# Mass layoffs by job
####################################################################################
####################################################################################
print('ENTERING MASS LAYOFFS SECTION')

jid_mass_layoff_threshold = 50

# Collapse by jid and date (and also gamma and occ2Xmeso to keep them around but jid should be nested within them, although check this). This will give us the total number of employees, job starts (hires), job ends (separations), and layoffs for each jid-month.
#   - Right now we're not actually using job_starts so maybe just delete it at some point
jid_panel = df.groupby(['jid','date','gamma','occ2Xmeso'])[['job_start_flag','job_end_flag','employed_in_month','layoff']].sum().reset_index().rename(columns={'job_start_flag':'job_starts','job_end_flag':'job_ends','layoff':'layoffs'})

jid_panel['net_emp_change'] = jid_panel.job_starts-jid_panel.job_ends

# XX Don't actually use these so far. Keeping because maybe eventually.
jid_panel['leave_share'] = jid_panel.job_ends/jid_panel.employed_in_month
jid_panel['layoff_share'] = jid_panel.job_ends/jid_panel.employed_in_month

# XX We think we can delete this sorting command
#jid_panel.sort_values(by=['jid','date'], inplace=True)

# Flag months in which a job had a mass layoff
jid_panel['mass_layoff_flag'] = (jid_panel.layoffs>200)&(jid_panel.leave_share>.3)&(jid_panel.net_emp_change<-200)

jid_panel['gamma_mass_layoff_count'] = jid_panel.groupby('gamma')['mass_layoff_flag'].transform('sum')


# List of gammas that experienced exactly 1 mass layoff (could be useful later)
gammas_w_ml = jid_panel[jid_panel.gamma_mass_layoff_count==1].gamma.unique()

jid_panel.to_pickle('../dump/' + modelname + '_jid_panel.p')
jid_panel = pd.read_pickle('../dump/' + modelname + '_jid_panel.p')

# I am restricting to the first mass layoff within each jid
jid_mass_layoffs = jid_panel.loc[jid_panel.mass_layoff_flag==True].drop(columns=['job_starts'])
jid_mass_layoffs['date_lag'] = jid_mass_layoffs.date-1
del jid_panel
gc.collect()

# What info do I want to keep for these? Just a flag for months with a mass layoff? Or do I also want to store the identity of the job with the layoff and the number of mass layoffs? The latter would only be useful if there are multiple mass layoffs, which hopefully will be rare once we're using the full data and defining them appropriately (e.g. based on number laid off AND fraction laid off).

gamma_mass_layoffs = jid_mass_layoffs.sort_values(by=['gamma','date']).groupby('gamma').agg({'date':'first','jid':'first','date_lag':'first','layoff_share':'first','leave_share':'first','mass_layoff_flag':'sum'}).reset_index().rename(columns={'mass_layoff_flag':'layoff_count_gamma'})


occ2Xmeso_mass_layoffs = jid_mass_layoffs.sort_values(by=['occ2Xmeso','date']).groupby('occ2Xmeso').agg({'date':'first','jid':'first','date_lag':'first','layoff_share':'first','leave_share':'first','mass_layoff_flag':'sum'}).reset_index().rename(columns={'mass_layoff_flag':'layoff_count_gamma'})


'''
# Flag whether each jid or market (both definitions ever experienced a mass layoff. Then add the date of the first mass layoff
jid_panel_filtered = jid_panel.query('mass_layoff_flag == 1')
jid_panel['jid_mass_layoff_count'] = jid_panel.groupby(['jid'])['mass_layoff_flag'].transform('sum')
for i in ['jid','gamma','occ2Xmeso']:
    jid_panel[i + '_mass_layoff_ever'] = jid_panel.groupby([i])['mass_layoff_flag'].transform('max')
    jid_panel = jid_panel.merge(jid_panel_filtered.groupby(i).first().date.reset_index().rename(columns={'date':i+'_first_mass_layoff_date'}), on=i, how='left')

del jid_panel_filtered
gc.collect()

jid_panel.to_pickle('../dump/' + modelname + '_jid_panel.p')
'''






####################################################################################
####################################################################################
# Collapse worker-job-month panel to worker-month and define layoff exposure
####################################################################################
####################################################################################

print('ENTERING FINAL PANEL COLLAPSE SECTION')

df.sort_values(by=['wid','date','monthly_earnings'],inplace=True, ascending=False)

# Keep the highest-paying job in each month if there is more than one. But take total monthly earnings across all jobs held in the month. 
df_collapse = df.groupby(['wid', 'date']).agg({'jid':'first', 'gamma':'first', 'occ2Xmeso':'first','yob':'first','uf':'first','monthly_earnings':'sum'}).reset_index()
del df
## XXJSF: play with this. Note that this isn't a balanced panel which may be a problem if we use employment as an outcome. Or at least well have to figure out how to measure it. Next step after this will be to merge on mass layoffs and create event time variable.

# Identify people employed in the shocked market in the month before the layoff
df_collapse_gamma = df_collapse.merge(gamma_mass_layoffs[['jid','gamma','leave_share','layoff_share','date_lag', 'layoff_count_gamma']].rename(columns={'jid':'jid_layoff'}), left_on=['gamma','date'], right_on=['gamma','date_lag'], how='left',validate='m:1', indicator=True)
del df_collapse
gc.collect()

# Identify people who were exposed at the jid-level or market-level. The exposed indicator will be constant within each wid
df_collapse_gamma['tmp_exposed_gamma'] = df_collapse_gamma._merge=='both'
df_collapse_gamma['exposed_gamma'] = df_collapse_gamma.groupby('wid')['tmp_exposed_gamma'].transform(max).drop(columns=['tmp_exposed_gamma'])
df_collapse_gamma['tmp_exposed_jid'] = ((df_collapse_gamma._merge=='both') & (df_collapse_gamma.jid==df_collapse_gamma.jid_layoff))
df_collapse_gamma['exposed_jid'] = df_collapse_gamma.groupby('wid')['tmp_exposed_jid'].transform(max).drop(columns=['tmp_exposed_jid'])

# Dropping workers who were exposed to a gamma that had more than 1 mass layoffs
df_collapse_gamma['layoff_count_gamma'] = df_collapse_gamma['layoff_count_gamma'].fillna(0)
df_collapse_gamma['layoff_count_gamma'] = df_collapse_gamma.groupby('wid')['layoff_count_gamma'].transform(max)
df_collapse_gamma = df_collapse_gamma.loc[df_collapse_gamma['layoff_count_gamma'].isin([0,1])]

# Identify time relative to the date of the layoff (in months)
df_collapse_gamma['layoff_date'] = df_collapse_gamma.groupby('gamma')['date_lag'].transform(max)+1
df_collapse_gamma['event_time'] = ((df_collapse_gamma.date.dt.to_timestamp()-df_collapse_gamma.layoff_date.dt.to_timestamp()).dt.total_seconds()/(30*24*60*60)).round().astype(float)

# Checking stuff
print(df_collapse_gamma[['wid','jid','gamma','date','date_lag','layoff_date','exposed_gamma','exposed_jid','event_time']].head(50))


df_collapse_gamma.to_pickle('../dump/' + modelname + '_df_collapse_gamma.p')


#EVERYTHING BELOW HERE IS RANDOM OLD STUFF



'''
# This is the old version using the jid_panel. For now I'm replacing with a new version based on jid_mass_layoffs, which merges mass layoffs onto the main worker panel by jid and date of the month before the layoff. That is, the new version flags workers employed in the shocked jid/market the month before the layoff. 

df_collapse = df_collapse.merge(jid_panel[['jid','jid_mass_layoff_ever','jid_first_mass_layoff_date','gamma_mass_layoff_ever','gamma_first_mass_layoff_date','occ2Xmeso_mass_layoff_ever','occ2Xmeso_first_mass_layoff_date']], on='jid',how='left')
for v in ['jid','gamma','occ2Xmeso']:
    layoff_date = df_collapse[v + '_first_mass_layoff_date']
    date = df_collapse['date']
    layoff_date = layoff_date.dt.to_timestamp()
    date = date.dt.to_timestamp()
    event_time_days = date-layoff_date
    event_time_months = event_time_days.dt.total_seconds() / (30 * 24 * 60 * 60)
    event_time_months = event_time_months.round().astype(float)
    df_collapse[v+'_event_time'] = event_time_months


    
# What I havent done yet is define exposure based on working in the relevant market/job in the month before the mass layoff. What I want to do is create a dummy for "jid_exposed" and "mkt_exposed" that are 1 if the worker was employed in a mass layoff job/market the period before the mass layoff. I could start with the jid_panel and restrict to the set of mass layoffs. Then create a new variable date_pre_layoff = mass_layoff_date - 1 month. Then merge this on to the worker panel by jid/market and date=date_pre_layoff. Any successful matches will flag a worker who is exposed at either the market or jid level. Then within each wid I can take the max of this flag to flag exposed workers across all observations in the panel. 
'''
    

########################
'''
Next steps
 - Define a mass layoff in the jid_panel. (BLS defines as >50 UI claims at an establishment over 5 weeks).
    - Do we want to define it at the jid level or the estab level? 
    - For now let's define it as a loss of 50 jobs within a jid. In the 2012-2016 4 states data, we have 11403 such "layoffs"
    - In RAIS causa_deslig=11 indicates a layoff. So we should define at least one mass layoff measure using causa_deslig==11
 - Start by coming up with several measures of mass layoffs, hopefully they all tell similar stories, and then we can pick a preferred one and others to be used as robustness checks. 
 - Our measure should be kind of a firm-job hybrid: are you in a firm that had a mass layoff and if so are you in a job that also had a mass layoff? Exposure should be at the market level: did this market have a job that had a mass layoff within a firm that had a mass layoff? 


- To create the arnings panel should we just use the code in this file but add earnings? Then we could do a second collapse by wid-month instead of jid-month

'''



########################################
# Look into monthly earnings vars
'''
year=2014						    
filename = '~/rais/RAIS/csv/brasil' + str(year) + '.csv'
vars =['pis','id_estab','data_adm','data_deslig','vl_rem_01', 'vl_rem_02', 'vl_rem_03', 'vl_rem_04', 'vl_rem_05', 'vl_rem_06', 'vl_rem_07', 'vl_rem_08', 'vl_rem_09', 'vl_rem_10', 'vl_rem_11', 'vl_rem_12', 'vl_rem_13_adiant', 'vl_rem_13_final']

raw_data = pd.read_csv(filename, usecols=vars, sep=',', dtype={'id_estab':str, 'pis':str}, nrows=1000)

pd.set_option('display.max_columns', None)
print(raw_data.drop(columns=['id_estab','pis']))
'''
# Looks like usually when we sum vl_rem_13_adiant and vl_rem_13_final it roughly equals vl_rem_12, consistent with the notion that together they comprise the 13th payment.
# - For annual earnings we should add them to the other 12. For monthly maybe we could either allocate the 13th payments equally over all employed months, allocate them over employed months proportional to that month's share of annual earnings, or define monthly earnings as annual earnings/months employed.
# To start lets go with the first.




