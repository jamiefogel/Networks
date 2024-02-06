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



