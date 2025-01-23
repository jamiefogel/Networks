import pandas as pd
import pyarrow.parquet as pq
import numpy as np
from config import root, rais

# Base paths
WORKER_BASE = rais + "parquet_novos/brasil{}.parquet"
ESTAB_BASE  = rais + "parquet_novos/estab{}.parquet"

def get_schema_info(file_path):
    """Get variable names and types from a parquet file without loading data"""
    schema = pq.read_schema(file_path)
    return {name: str(dtype) for name, dtype in zip(schema.names, schema.types)}

def create_variable_catalog():
    # Initialize results dictionary
    all_schemas = {
        'worker': {},  # year -> {var -> type}
        'estab': {}    # year -> {var -> type}
    }
    
    # Worker files (1986-2021)
    for year in range(1986, 2022):
        file_path = WORKER_BASE.format(year)
        try:
            all_schemas['worker'][year] = get_schema_info(file_path)
        except Exception as e:
            print(f"Error reading worker file for {year}: {e}")
    
    # Establishment files (1985-2021)
    for year in range(1985, 2022):
        file_path = ESTAB_BASE.format(year)
        try:
            all_schemas['estab'][year] = get_schema_info(file_path)
        except Exception as e:
            print(f"Error reading establishment file for {year}: {e}")
    
    # Create variable presence matrix for each file type
    results = {}
    for file_type in ['worker', 'estab']:
        if not all_schemas[file_type]:
            continue
            
        # Get all unique variables
        all_vars = set()
        for year_schema in all_schemas[file_type].values():
            all_vars.update(year_schema.keys())
        
        # Create DataFrame with years as columns
        years = sorted(all_schemas[file_type].keys())
        data = []
        
        for var in sorted(all_vars):
            row = {'variable': var}
            types = set()
            for year in years:
                schema = all_schemas[file_type].get(year, {})
                type_str = schema.get(var, np.nan)
                row[str(year)] = type_str
                if pd.notna(type_str):
                    types.add(type_str)
            row['types'] = ', '.join(sorted(types))
            data.append(row)
            
        results[file_type] = pd.DataFrame(data)
    
    return results

# Run the analysis
catalogs = create_variable_catalog()

# Save results
for file_type, df in catalogs.items():
    df.to_csv(f'{root}/Code/replicate_mayara/code_to_explore_varnames_etc/{file_type}_variables.csv', index=False)
    print(f"\nFirst few rows of {file_type} catalog:")
    print(df.head())