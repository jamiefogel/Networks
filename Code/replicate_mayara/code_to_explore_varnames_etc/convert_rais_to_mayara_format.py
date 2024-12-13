#Created by Claude: https://claude.ai/chat/3075c54c-ddfb-4bb5-91d5-f133ec890acb
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import os
from datetime import datetime
from config import root, rais

# Base paths
WORKER_BASE = rais + "parquet_novos/brasil{}.parquet"
ESTAB_BASE = rais + "parquet_novos/estab{}.parquet"

def get_variable_mapping(year, data_type):
    """Return appropriate variable mapping based on year and data type (worker/establishment)"""
    
    # Base mappings for worker data
    worker_mapping = {
        'id_variables': {
            'pis': 'workerid_pis',
            'id_estab': 'estabid_cnpj_cei',
            'cei_vinc': 'estabid_constructioncei'
        },
        'worker_chars': {
            'idade': 'age',
            'fx_etaria': 'agegroup',
            'genero': 'gender',
            'grau_instr': 'educ',
            'raca_cor': 'race'
        },
        'employment': {
            'tipo_vinculo': 'contracttype',
            'horas_contr': 'contracthours',
            'emp_31dez': 'emp1231',
            'temp_empr': 'empmonths'
        },
        'earnings': {
            'rem_dez_r': 'earningsdecnom',
            'rem_med_r': 'earningsavgnom',
            'rem_dez_sm': 'earningsdecmw',
            'rem_med_sm': 'earningsavgmw'
        }
    }
    
    # Base mappings for establishment data
    estab_mapping = {
        'id_variables': {
            'id_estab': 'estabid_cnpj_cei',
            'cei_vinc': 'estabid_constructioncei',
            'razao_social': 'estabid_name',
            'tipo_estab': 'estabid_type'
        },
        'location': {
            'codemun': 'municipality',
            'cep': 'estabzip'
        },
        'classification': {
            'subs_ibge': 'ibgesubsector',
            'subativ_ibge': 'ibgesubactivity'
        },
        'administrative': {
            'ind_pat': 'estabpat',
            'ind_simples': 'indsimples',
            'ind_rais_neg': 'indraisnegativa'
        }
    }
    
    # Year-specific modifications for worker data
    if data_type == 'worker':
        if year >= 1994:
            worker_mapping['worker_chars'].update({
                'nacionalidad': 'nationality' if year >= 1996 else None,
                'nacionalid': 'nationality' if year < 1996 else None
            })
            
        if year >= 2002:
            worker_mapping['id_variables']['cpf'] = 'workerid_cpf'
        
        # CBO/CNAE codes
        worker_mapping['classification'] = {}
        if year < 2002:
            worker_mapping['classification']['cbo1994'] = 'cbo94'
        else:
            worker_mapping['classification']['cbo2002'] = 'cbo02'
        
        if 1994 <= year <= 2001:
            worker_mapping['classification']['clas_cnae'] = 'cnaeclass95'
        elif year >= 2006:
            worker_mapping['classification'].update({
                'clas_cnae20': 'cnaeclass20',
                'sbcl_cnae20': 'cnaesubclass20'
            })
        
        return worker_mapping
    
    # Year-specific modifications for establishment data
    else:
        if 1994 <= year <= 2001:
            estab_mapping['classification']['clas_cnae'] = 'cnaeclass95'
        elif year >= 2006:
            estab_mapping['classification'].update({
                'clas_cnae20': 'cnaeclass20',
                'sbcl_cnae20': 'cnaesubclass20'
            })
        
        return estab_mapping

def transform_data(df, year, data_type):
    """Transform data to match Mayara's format"""
    mapping = get_variable_mapping(year, data_type)
    out = pd.DataFrame()

    # Apply mappings from each category
    for category in mapping.values():
        for orig_var, new_var in category.items():
            if new_var and orig_var in df.columns:
                out[new_var] = df[orig_var]

    # Handle worker-specific transformations
    if data_type == 'worker':
        # Handle dates robustly
        def parse_dates(date_col):
            return pd.to_datetime(df[date_col], errors='coerce')

        # XX we end up with a very small (33 out of 70 millio) of invalid/missing dates here. Need to 'coerce' them to missing to prevent code from failing. Probably doesn't matter, but noting it in case.
        if 'data_adm' in df:
            adm_date = parse_dates('data_adm')
            out['admyear'] = adm_date.dt.year
            out['admmonth'] = adm_date.dt.month
            out['admdate'] = pd.to_numeric(adm_date.dt.strftime('%Y%m%d'), errors='coerce')
        else:
            out['admyear'] = year

        if 'data_deslig' in df:
            deslig_date = parse_dates('data_deslig')
            out['sepmonth'] = deslig_date.dt.month
            out['sepday'] = deslig_date.dt.day

        # Handle separation reason based on year
        if 'causa_desli' in df:
            out['sepreason'] = df['causa_desli']
        elif 'causa_deslig' in df:
            out['sepreason'] = df['causa_deslig']
        elif 'causa_resc' in df and year < 1994:
            out['sepreason'] = df['causa_resc']

    # Handle establishment-specific transformations
    else:
        # Handle dates robustly
        date_mappings = {
            'data_abertura': 'estabopendate',
            'data_encerramento': 'estabclosedate',
            'data_baixa': 'estabbaixadate'
        }

        for orig_date, new_date in date_mappings.items():
            if orig_date in df.columns:
                try:
                    date_series = pd.to_datetime(df[orig_date], errors='coerce')
                    out[new_date] = pd.to_numeric(date_series.dt.strftime('%Y%m%d'), errors='coerce')
                except Exception as e:
                    print(f"Warning: Could not convert {orig_date} for year {year}: {e}")

    return out


def process_year(year):
    """Process both worker and establishment data for a given year"""
    success = {'worker': False, 'estab': False}
    
    # Process worker data (1986-2021)
    if year >= 1986:
        try:
            worker_file = WORKER_BASE.format(year)
            df_worker = pq.read_table(worker_file).to_pandas()
            df_worker_clean = transform_data(df_worker, year, 'worker')
            
            # Save one file per state per year
            if 'uf' in df_worker_clean.columns:
                unique_states = df_worker_clean['uf'].unique()
                for state in unique_states:
                    state_data = df_worker_clean.loc[df_worker_clean['uf'] == state]
                    state_output = f"{root}/Code/replicate_mayara/unzipped/{state}{year}ID.txt"
                    state_data.to_csv(state_output, sep=';', index=False)
                print(f"Successfully processed worker data for {year}")
                success['worker'] = True
            else:
                print(f"Worker data for {year} is missing 'uf' column.")
        except Exception as e:
            print(f"Error processing worker data for {year}: {e}")
    
    # Process establishment data (1985-2021)
    try:
        estab_file = ESTAB_BASE.format(year)
        df_estab = pq.read_table(estab_file).to_pandas()
        df_estab_clean = transform_data(df_estab, year, 'estab')
        estab_output = f"{root}/Code/replicate_mayara/unzipped/Estb{year}ID.txt"
        df_estab_clean.to_csv(estab_output, sep=';', index=False)
        print(f"Successfully processed establishment data for {year}")
        success['estab'] = True
    except Exception as e:
        print(f"Error processing establishment data for {year}: {e}")
    
    return success


def main():
    # Create output directory if it doesn't exist
    output_dir = f"{root}/Code/replicate_mayara/unzipped"
    os.makedirs(output_dir, exist_ok=True)
    
    # Process all years and track results
    results = {}
    for year in range(2018, 2022):
        print(f"\nProcessing year {year}...")
        results[year] = process_year(year)
    
    # Print summary
    print("\nProcessing Summary:")
    for year, status in results.items():
        print(f"{year}:")
        print(f"  Worker data: {'Success' if status['worker'] else 'Failed'}")
        print(f"  Establishment data: {'Success' if status['estab'] else 'Failed'}")

if __name__ == "__main__":
    main()
