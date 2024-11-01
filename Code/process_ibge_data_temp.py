import pandas as pd

states = [20, 22, 23]
for state in states:
    print(state)
    # Read and process individual sheets
    all_data = []
    for i in range(1, 17):
        # Read sector name
        sector_df = pd.read_excel(f"{sector_data_filepath}/Tabela{state}.xls", sheet_name=f"Tabela{state}.{i}", 
                                  skiprows=4, nrows=1, header=None)
        sector = sector_df.iloc[0, 0]
        # Read data
        df = pd.read_excel(f"{sector_data_filepath}/Tabela{state}.xls", sheet_name=f"Tabela{state}.{i}", 
                           skiprows=5, nrows=16, usecols="A:F", header=0)
        df.columns = df.columns.str.strip()
        # Generate and manipulate data
        df['sector_pt'] = sector
        df.rename(columns={'ANO': 'year', 
                           'VALOR DO ANO ANTERIOR'         : 'lag_nom_gdp', 
                           'ÍNDICE DE VOLUME'              : 'volume_index', 
                           'VALOR A PREÇOS DO ANO ANTERIOR': 'gdp_last_year_price', 
                           'ÍNDICE DE PREÇO'               : 'inflation_rate', 
                           'VALOR A PREÇO CORRENTE'        : 'nominal_gdp'}, inplace=True)
        df['price_index'] = 1.0
        for j in range(1, len(df)):
            df.loc[j, 'price_index'] = df.loc[j-1, 'price_index'] * df.loc[j, 'inflation_rate']
        df['real_gdp'] = df['nominal_gdp'] / df['price_index']
        df['sector_ibge'] = i - 1
        all_data.append(df)
    # Append all dataframes
    final_df = pd.concat(all_data, ignore_index=True)    
    # Final data manipulation and export
    final_df.sort_values(by=['sector_ibge', 'year'], inplace=True)
    final_df.drop(columns=['lag_nom_gdp', 'volume_index'], inplace=True)
    final_df.rename(columns={'real_gdp': 'y_s', 'price_index': 'p_s', 'sector_ibge': 's'}, inplace=True)
    final_df = final_df[['year','p_s', 'y_s', 's']]
    final_df = final_df[final_df['s'] != 0]
    final_df.to_csv(f"{sector_data_filepath}/sectors_{state}.csv", index=False)



# Mapping of regions to tables
for i in range(1, 34):
    region_df = pd.read_excel(f"{sector_data_filepath}/Tabela{i}.xls", sheet_name=f"Tabela{i}.1", 
                              skiprows=3, nrows=1, header=None)
    region = region_df.iloc[0, 0]
    print(f"Table {i} corresponds to {region}")



states = [20, 22, 23]
dfs = []

for state in states:
    df = pd.read_csv(f"{sector_data_filepath}/sectors_{state}.csv")
    df.drop(columns=['p_s'], inplace=True)
    df = df.sort_values(['s', 'year'])
    df['y_s_pct_change'] = df.groupby('s')['y_s'].pct_change()
    df['y_s_norm'] = df.groupby('s')['y_s'].transform(lambda x: x / x.iloc[0])
    df = df.pivot(index='year', columns='s', values=['y_s','y_s_pct_change','y_s_norm'])
    dfs.append(df)
    
df_MG, df_RJ, df_SP = dfs
                                    

sector_labels_abbr = ["Agriculture, livestock, forestry, fisheries and aquaculture",
                  "Extractive industries",
                  "Manufacturing industries",
                  "Utilities",
                  "Construction",
                  "Retail, Wholesale and Vehicle Repair",
                  "Transport, storage and mail",
                  "Accommodation and food",
                  "Information and communication",
                  "Financial, insurance and related services",
                  "Real estate activities",
                  "Professional, scientific and technical svcs",
                  "Public admin, defense, educ, health and soc security",
                  "Private health and education",
                  "Arts, culture, sports and recreation and other svcs"]



# Plotting the time series of percent changes for s=5 for all MG, RJ, and SP
# It appears that sector-level output is pretty similar across states
import matplotlib.pyplot as plt

for s in range(1,16):
    plt.figure(figsize=(14, 7))
    plt.plot(df_MG.index, df_MG.xs(s, axis=1, level='s')['y_s_pct_change'], label='MG')
    plt.plot(df_RJ.index, df_RJ.xs(s, axis=1, level='s')['y_s_pct_change'], label='RJ')
    plt.plot(df_SP.index, df_SP.xs(s, axis=1, level='s')['y_s_pct_change'], label='SP')
    plt.title(f'Percent Change of y_s for s={s} Across Years')
    plt.xlabel('Year')
    plt.ylabel('Percent Change')
    plt.legend()
    plt.grid(True)
    plt.show()
    plt.savefig(root + f'Results/comparing_sector_output_by_state_{s}.pdf')


## There doesn~t appear to be a meaningful increase in construction output in Rio relative to other sectors
# Create the plot
plt.figure(figsize=(14, 7))
# Plot all sectors in grey
for s in range(1,16):    
    plt.plot(df_RJ.index, df_RJ.xs(s, axis=1, level='s')['y_s_pct_change'], color='grey', alpha=0.5)

# Highlight sector 5 in color
plt.plot(df_RJ.index, df_RJ.xs(5, axis=1, level='s')['y_s_pct_change'], color='blue', label='Sector 5 - Construction', linewidth=2.5)
plt.title('Percent Change of y_s for All Sectors in RJ')
plt.xlabel('Year')
plt.ylabel('Percent Change')
plt.legend()
plt.grid(True)    
plt.savefig(root + f'Results/comparing_construction_to_other_sectors_output_RJ.pdf', format='pdf')

# Plotting the time series of normalized y_s values for all MG, RJ, and SP
for s in range(1, 16):
    plt.figure(figsize=(14, 7))
    plt.plot(df_MG.index, df_MG.xs(s, axis=1, level='s')['y_s_norm'], label='MG')
    plt.plot(df_RJ.index, df_RJ.xs(s, axis=1, level='s')['y_s_norm'], label='RJ')
    plt.plot(df_SP.index, df_SP.xs(s, axis=1, level='s')['y_s_norm'], label='SP')
    plt.title(f'Normalized y_s for s={s} Across Years')
    plt.xlabel('Year')
    plt.ylabel('Normalized y_s')
    plt.legend()
    plt.grid(True)
    plt.show()
    plt.savefig(root + f'Results/comparing_sector_output_by_state_normalized_{s}.pdf')
