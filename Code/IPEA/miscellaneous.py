# Miscellaneous notes, to-dos, and auxiliary analyses







# Why don't we see a job end date for wid=10000000113?
# How often do we have a causa_deslig without a data_deslig?
# How often does a job end (i.e. not show up in the next year) without a data_deslig?



# XXJSF: do we need to include codemun here? How often is codemun not unique within jid? Is it always unique within a month or year, but can possibly change across years if the establishment moves?
# Not unique:
# jid_panel.duplicated(subset=['jid','year','month']).sum()
# 175487
# Ultimately we only care about non-uniqueness when we collapse by micro, not by codemun. So should try merging on micros and trying again. 
#jid_panel = df.groupby(['jid','codemun','month','year'])[['job_start_flag','job_end_flag','employed_in_month','layoff']].sum().reset_index()




# Calculate frequency of multiple job holding
multiple_job_holding = df.loc[(df.job_start_flag==0) & (df.job_end_flag==0)].groupby(['wid', 'date'])['jid'].nunique().reset_index()['jid'].value_counts()
print('Multiple job holding: frequency counts for number of jobs per month (excluding first and last month of employment spell)')
print(multiple_job_holding)










''' Trying to figure out differencing dates. I think I've got it now but leaving here for now just in case
dfa = pd.DataFrame({'start_date': [pd.Period('2022-01', 'M'), pd.Period('2022-02', 'M'), pd.Period('2022-03', 'M')],'end_date': [pd.Period('2022-03', 'M'), pd.Period('2022-05', 'M'), pd.Period('NaT', 'M')]})
dfa['diff_in_months'] = dfa['end_date'] - dfa['start_date']

dfa = pd.DataFrame({'start_date': [pd.Period('2022-01', 'M'), pd.Period('2022-02', 'M'), pd.Period('2022-03', 'M')], 'end_date': [pd.Period('2022-03', 'M'), pd.Period('2022-05', 'M'), pd.Period('NaT', 'M')]})

# Convert the Period objects to datetime objects to perform the subtraction
dfa['start_date'] = dfa['start_date'].dt.to_timestamp()
dfa['end_date'] = dfa['end_date'].dt.to_timestamp()

# Subtract the two columns to get the difference in time
dfa['diff_in_time'] = dfa['end_date'] - dfa['start_date']

# Divide the number of days in the difference by the number of days in a month (30)
dfa['diff_in_months'] = dfa['diff_in_time'].dt.total_seconds() / (30 * 24 * 60 * 60)
dfa['diff_in_months'] = dfa['diff_in_months'].round().astype(int)

'''
