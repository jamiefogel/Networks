import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

# Define directories
rais_dir = "//storage6/bases/DADOS/RESTRITO/RAIS/"  # Updated to the correct RAIS data path

# Function to determine the delimiter based on the year
def get_delimiter(year):
    if ((year < 1998) | (year == 2016) | (year == 2018) | (year == 2019)):
        return ';'
    else:
        return ','

# Define states of interest
states_of_interest = [31, 33, 35]

# Function to read employer data (always using semicolon delimiter)
def read_employer_data(year, nrows=None, states_of_interest=states_of_interest):
    filepath = os.path.join(rais_dir, f'csv/estab{year}.csv')
    cols = ['id_estab', 'cnpj_raiz', 'qt_vinc_ativos', 'codemun']  # Load only necessary columns
    df = pd.read_csv(filepath, delimiter=';', usecols=cols, nrows=nrows, encoding='latin1')
    df['year'] = year
    df['uf'] = df.codemun.astype(str).str[0:2].astype(int)
    return df[df['uf'].isin(states_of_interest)]  # Restrict to the 3 states early

# Function to read worker data
def read_worker_data(year, nrows=None, states_of_interest=states_of_interest):
    delimiter = get_delimiter(year)
    filepath = os.path.join(rais_dir, f'csv/brasil{year}.csv')
    cols = ['pis', 'id_estab', 'rem_med_sm', 'codemun']  # Load only necessary columns
    df = pd.read_csv(filepath, delimiter=delimiter, usecols=cols, nrows=nrows, encoding='latin1')
    df['year'] = year
    df['uf'] = df.codemun.astype(str).str[0:2].astype(int)
    return df[df['uf'].isin(states_of_interest)]  # Restrict to the 3 states early

# Read data for 2013-2018 (example with nrows for testing)
years = range(2013, 2019)
employer_dfs = [read_employer_data(year, nrows=None, states_of_interest=states_of_interest) for year in years]
worker_dfs = [read_worker_data(year, nrows=None, states_of_interest=states_of_interest) for year in years]


# df2013 = pd.read_csv('//storage6/bases/DADOS/RESTRITO/RAIS/csv/brasil2013.csv', delimiter=delimiter, usecols=cols, nrows=nrows, encoding='latin1')
# df2014 = pd.read_csv('//storage6/bases/DADOS/RESTRITO/RAIS/csv/brasil2014.csv', delimiter=delimiter, usecols=cols, nrows=nrows, encoding='latin1')
# df2015 = pd.read_csv('//storage6/bases/DADOS/RESTRITO/RAIS/csv/brasil2015.csv', delimiter=delimiter, usecols=cols, nrows=nrows, encoding='latin1')
# df2016 = pd.read_csv('//storage6/bases/DADOS/RESTRITO/RAIS/csv/brasil2016.csv', delimiter=delimiter, usecols=cols, nrows=nrows, encoding='latin1')

# Combine dataframes for all years
employer_df = pd.concat(employer_dfs)
worker_df = pd.concat(worker_dfs)

print("Employer Data:")
print(employer_df.head())

print("Worker Data:")
print(worker_df.head())

# Identify firm closures and mass layoffs
def identify_closures_and_layoffs(df):
    df = df.sort_values(by=['cnpj_raiz', 'year'])
    df['prev_year_emp'] = df.groupby('cnpj_raiz')['qt_vinc_ativos'].shift(1)
    
    df['closure'] = (df['prev_year_emp'] >= 10) & (df['qt_vinc_ativos'] == 0)
    df['mass_layoff'] = (df['prev_year_emp'] >= 100) & (df['qt_vinc_ativos'] <= 0.7 * df['prev_year_emp'])
    
    return df

employer_df = identify_closures_and_layoffs(employer_df)

print("\nEmployer Data with Closures and Mass Layoffs:")
print(employer_df[['cnpj_raiz', 'year', 'qt_vinc_ativos', 'prev_year_emp', 'closure', 'mass_layoff']].head())

# Merge worker data with employer data to get layoff information
worker_df = worker_df.merge(employer_df[['id_estab', 'year', 'closure', 'mass_layoff']], on=['id_estab', 'year'], how='left')

# Define indicator for workers affected by closure or mass layoff
worker_df['affected'] = worker_df['closure'] | worker_df['mass_layoff']

print("\nWorker Data with Affected Indicators:")
print(worker_df[['pis', 'id_estab', 'year', 'rem_med_sm', 'closure', 'mass_layoff', 'affected']].head())

# Create indicator for being employed two years after the layoff
worker_df['year_plus_two'] = worker_df['year'] + 2
worker_df = worker_df.merge(worker_df[['pis', 'year', 'rem_med_sm']], left_on=['pis', 'year_plus_two'], right_on=['pis', 'year'], suffixes=('', '_plus_two'))
worker_df['employed_two_years_after'] = worker_df['rem_med_sm_plus_two'] > 0

# Calculate earnings change from year before to two years after
worker_df['year_minus_one'] = worker_df['year'] - 1
worker_df = worker_df.merge(worker_df[['pis', 'year', 'rem_med_sm']], left_on=['pis', 'year_minus_one'], right_on=['pis', 'year'], suffixes=('', '_minus_one'))
worker_df['earnings_change'] = np.log(worker_df['rem_med_sm_plus_two'] + 1) - np.log(worker_df['rem_med_sm_minus_one'] + 1)

print("\nWorker Data with Employment and Earnings Change Indicators:")
print(worker_df[['pis', 'id_estab', 'year', 'rem_med_sm', 'employed_two_years_after', 'earnings_change']].head())

# Exploratory Analysis: Employment Status Two Years After Layoff
employment_status = worker_df.groupby(['year', 'affected'])['employed_two_years_after'].mean().reset_index()
print("\nEmployment Status Two Years After Layoff:")
print(employment_status)

# Exploratory Analysis: Earnings Change from Year Before to Two Years After Layoff
earnings_change = worker_df.groupby(['year', 'affected'])['earnings_change'].mean().reset_index()
print("\nEarnings Change from Year Before to Two Years After Layoff:")
print(earnings_change)

# Plotting the results

# Employment Status Plot
plt.figure(figsize=(10, 6))
for label, df in employment_status.groupby('affected'):
    plt.plot(df['year'], df['employed_two_years_after'], marker='o', label=f'Affected: {label}')
plt.xlabel('Year')
plt.ylabel('Employment Rate Two Years After Layoff')
plt.title('Employment Rate Two Years After Layoff by Year')
plt.legend()
plt.grid(True)
plt.show()

# Earnings Change Plot
plt.figure(figsize=(10, 6))
for label, df in earnings_change.groupby('affected'):
    plt.plot(df['year'], df['earnings_change'], marker='o', label=f'Affected: {label}')
plt.xlabel('Year')
plt.ylabel('Average Earnings Change (Log Difference)')
plt.title('Earnings Change from Year Before to Two Years After Layoff by Year')
plt.legend()
plt.grid(True)
plt.show()
