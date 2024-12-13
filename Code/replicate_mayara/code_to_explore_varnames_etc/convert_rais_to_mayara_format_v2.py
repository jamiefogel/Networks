#Created by Claude: https://claude.ai/chat/3075c54c-ddfb-4bb5-91d5-f133ec890acb
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import os
from datetime import datetime
from config import root, rais
import re

# Base paths
WORKER_BASE = rais + "parquet_novos/brasil{}.parquet"
ESTAB_BASE = rais + "parquet_novos/estab{}.parquet"

# Variables missing from the 1986 data that cause Mayara's 2_rais_import.do to fail
#cnpjcei nacionalidade ibgesubatividade ibgesubsetor anoadmissão tipovínculo motivodesligamento cboocupação

'''
# Estab vars
dataabertura
databaixa
dataencerramento
dtabertcom
dtbaixacom
indsimples
natestb
natjurid
razaosocial
razãosocial
'''

crosswalk = {'anochegadabrasil': 'ano_cheg_pais',
        'causaafastamento1': 'causa_afast_1',
        'causaafastamento2': 'causa_afast_2',
        'causaafastamento3': 'causa_afast_3',
        'motivodesligamento': 'causa_deslig',
        'causadesli': 'causa_deslig', #XX
        #'motivodesligamento': 'causa_resc' # 1994 version
        'cboocupação2002': 'cbo2002',
        'ocup2002': 'cbo2002', #XX
        'cbo94ocupação': 'cbo1994', # Added mapping for CBO 1994
        'ocupacao94': 'cbo1994', # XX
        'ceivinculado': 'cei_vinc',
        'ceivinc': 'cei_vinc', #XX
        'clascnae10': 'clas_cnae10', # Ensure 'clascnae10' exists in Mayara's data
        'clascnae20': 'clas_cnae20',
        'cnae20classe': 'clas_cnae20', #XX
        'cnpjraiz': 'cnpj_raiz',
        'municipio': 'codemun',
        'município': 'codemun',
        'cpf': 'cpf',
        'númeroctps': 'ctps_num',
        'dataadmissao': 'data_adm',
        'dataadmissãodeclarada': 'data_adm', #XX
        'dtadmissao': 'data_adm', #XX
        'diadedesligamento': 'data_deslig',
        'diadesl': 'data_deslig', #XX
        'diafimaf1': 'data_fim_afast_1',
        'diafimaf2': 'data_fim_afast_2',
        'diafimaf3': 'data_fim_afast_3',
        'diainiaf1': 'data_ini_afast_1',
        'diainiaf2': 'data_ini_afast_2',
        'diainiaf3': 'data_ini_afast_3',
        'datadenascimento': 'data_nasc',
        'dtnasciment': 'data_nasc', #XX
        'vinculoativo3112': 'emp_31dez',
        'vínculoativo3112': 'emp_31dez',
        'empem3112': 'emp_31dez', #XX
        'sexotrabalhador': 'genero', #XX
        'genero': 'genero', #XX
        'sexo': 'genero',
        'grauinstrucao': 'grau_instr',
        'escolaridadeapós2005': 'grau_instr', #XX
        'grauinstr': 'grau_instr', #XX
        'grauinstrução20051985': 'grau_instr', #XX
        'grinstrucao': 'grau_instr', #XX
        'horascontr': 'horas_contr',
        'qtdhoracontr': 'horas_contr', #XX
        'identificad': 'id_estab', #XX
        'cnpjcei': 'id_estab',
        'idade': 'idade',
        'indalvara': 'ind_alvara',
        'indcpfval': 'ind_cpf_val',
        'inddefic': 'ind_defic',
        'indhorasextras': 'ind_horas_extras',
        'indpisval': 'ind_pis_val', # Confirmed mapping
        'indsindic': 'ind_sindic',
        'indtrabintermitente': 'ind_trab_intermitente',
        'indtrabparcial': 'ind_trab_parcial',
        'mesadmissao': 'mes_adm',
        'mêsadmissão': 'mes_adm', #XX
        'mesdesligamento': 'mes_deslig',
        'mesdeslig': 'mes_deslig', #XX
        'mêsdesligamento': 'mes_deslig', #XX
        'mesrem13adiant': 'mes_rem_13_adiant',
        'mesrem13final': 'mes_rem_13_final',
        'nacionalidad': 'nacionalidad', # Existing mapping
        'nacionalidade': 'nacionalidad', #XX
        'natjurid': 'nat_jur2016',
        'natjuridica': 'nat_jur2016',
        'naturezajurídica': 'nat_jur2016',
        'naturjur': 'nat_jur2016',
        'nome': 'nome_trab',
        'peso': 'peso',
        'pis': 'pis',
        'qtddiasafastamento': 'qt_dias_afast',
        'qt_horas_extras_01': 'qt_horas_extras_01',
        'qt_horas_extras_02': 'qt_horas_extras_02',
        'qt_horas_extras_03': 'qt_horas_extras_03',
        'qt_horas_extras_04': 'qt_horas_extras_04',
        'qt_horas_extras_05': 'qt_horas_extras_05',
        'qt_horas_extras_06': 'qt_horas_extras_06',
        'qt_horas_extras_07': 'qt_horas_extras_07',
        'qt_horas_extras_08': 'qt_horas_extras_08',
        'qt_horas_extras_09': 'qt_horas_extras_09',
        'qt_horas_extras_10': 'qt_horas_extras_10',
        'qt_horas_extras_11': 'qt_horas_extras_11',
        'qt_horas_extras_12': 'qt_horas_extras_12',
        'qt_meses_resc_bc_horas': 'qt_meses_resc_bc_horas',
        'qt_meses_resc_gratif': 'qt_meses_resc_gratif',
        'qt_meses_resc_reaj_col': 'qt_meses_resc_reaj_col',
        'racacor': 'raca_cor',
        'raca_cor': 'raca_cor', #XX
        'raçacor': 'raca_cor', #XX
        'regiaometropolitana': 'regiao_metro',
        'remdezembror': 'rem_dez_r',
        'remdezr': 'rem_dez_r', #XX
        'vlremundezembronom': 'rem_dez_r', #XX
        'remdezembrosm': 'rem_dez_sm',
        'remdezembro': 'rem_dez_sm', #XX
        'vlremundezembrosm': 'rem_dez_sm', #XX
        'remmedr': 'rem_med_r',
        'vlremunmédianom': 'rem_med_r', #XX
        'remmedia': 'rem_med_sm',
        'vlremunmédiasm': 'rem_med_sm', #XX
        'salcontr': 'salario',
        'vlsaláriocontratual': 'salario', #XX
        'sbclas20': 'sbcl_cnae22',
        'cnae20subclasse': 'sbcl_cnae22', #XX
        'subativibge': 'subativ_ibge',
        'ibgesubatividade': 'subativ_ibge', #XX
        'ibgesubsetor': 'subs_ibge', #XX
        'subsibge': 'subs_ibge',
        'tamestab': 'tamestab',
        'tempempr': 'temp_empr',
        'tempoemprego': 'temp_empr', #XX
        'tipoadm': 'tipo_adm',
        'tipoadmissão': 'tipo_adm', #XX
        'tipodefic': 'tipo_defic',
        'tpdefic': 'tipo_defic', #XX
        'tipoestab': 'tipo_estab',
        'tipoestb': 'tipo_estab', #XX
        'tipoestbl': 'tipo_estab', #XX
        'tiposalario': 'tipo_salario',
        'tiposal': 'tipo_salario', #XX
        'tiposalário': 'tipo_salario', #XX
        'tipovinculo': 'tipo_vinculo',
        'tipovínculo': 'tipo_vinculo', #XX
        'tpvincl': 'tipo_vinculo', #XX
        'tpvinculo': 'tipo_vinculo', #XX
        'uf': 'uf',
        'vl_contr_assist': 'vl_contr_assist',
        'vl_contr_assoc1': 'vl_contr_assoc1',
        'vl_contr_assoc2': 'vl_contr_assoc2',
        'vl_contr_confed': 'vl_contr_confed',
        'vl_contr_sindic': 'vl_contr_sindic',
        'vl_rem_01': 'vl_rem_01',
        'vl_rem_02': 'vl_rem_02',
        'vl_rem_03': 'vl_rem_03',
        'vl_rem_04': 'vl_rem_04',
        'vl_rem_05': 'vl_rem_05',
        'vl_rem_06': 'vl_rem_06',
        'vl_rem_07': 'vl_rem_07',
        'vl_rem_08': 'vl_rem_08',
        'vl_rem_09': 'vl_rem_09',
        'vl_rem_10': 'vl_rem_10',
        'vl_rem_11': 'vl_rem_11',
        'vl_rem_12': 'vl_rem_12',
        'vl_rem_13_adiant': 'vl_rem_13_adiant',
        'vl_rem_13_final': 'vl_rem_13_final',
        'vl_resc_banco_horas': 'vl_resc_banco_horas',
        'vl_resc_ferias': 'vl_resc_ferias',
        'vl_resc_gratif': 'vl_resc_gratif',
        'vl_resc_multa': 'vl_resc_multa',
        'vl_resc_reajuste_col': 'vl_resc_reajuste_col',
        'vl_ult_rem_ano': 'vl_ult_rem_ano',
        'faixaetária': 'fx_etaria', # Faixa etária do empregado em 31 de dezembro do ano base
        'situaçãovínculo': 'sit_vinculo', # Situação do vínculo empregatício
        'naturezajuridica': 'nat_vinculo', # Natureza do vínculo empregatício
        # Only exists pre-1994
        'cboocupação':'cbo1994', #This is the only CBO var we have back then so that's my best guess. Also cboocupação only exists before 1994
        # These don't exist in our data but I don't think they're needed so creating placeholdeers
        'anoadmissão'         :'anoadmissão'          ,
        'cep'		      :'cep'                  ,
        'cepestab'	      :'cepestab'             ,
        'clascnae95'	      :'clascnae95'           ,
        'cnae95classe'       :'clascnae95'           ,
        'indestabparticipapat':'indestabparticipapat' ,
        'indpat'	      :'indpat'               ,
        'indsimples'	      :'indsimples'               ,
        'indraisneg'	      :'indraisneg'           ,
        'indraisnegativa'     :'indraisnegativa'      ,
        'natestb'         :'nat_jur2016'          
}



jamie_mayara_cw = pd.DataFrame(list(crosswalk.items()), columns=['mayara','jamie'])
print(jamie_mayara_cw)
if not jamie_mayara_cw.mayara.duplicated().sum() == 0:
    print('Not unique!!!!')
    raise ValueError("Duplicated values found in `jamie_mayara_cw.mayara`")

# Read the Excel file into a DataFrame
#df_full = pd.read_excel(r'\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized\descsave_rais_files_20180829_clean.xlsx', sheet_name="clean")                   
df_full = pd.read_excel(root + 'Code/replicate_mayara/raisdictionaries/harmonized/descsave_rais_files_20180829_clean.xlsx', sheet_name="clean")                   


# Keep rows where 'keep' column has value 'yes'
df = df_full[df_full['keep'] == "yes"].copy()

# Extract the state and year using regex
df['state'] = df['file'].apply(lambda x: re.search(r"([A-Za-z]+)(\d{4})", x).group(1) if re.search(r"([A-Za-z]+)(\d{4})", x) else None)
df['year'] = df['file'].apply(lambda x: re.search(r"([A-Za-z]+)(\d{4})", x).group(2) if re.search(r"([A-Za-z]+)(\d{4})", x) else None)



df = df.dropna(subset=['year'])
df = df[['name','cleanname','state','year']]
df = df.drop_duplicates()
if not df[['cleanname','state','year']].duplicated().sum() == 0:
    print("Not unique!!!!")
    raise ValueError("Duplicated values found in `df[['name', 'year']]`")

'''
temp = df[['name','cleanname']].drop_duplicates()
temp = temp.loc[temp.cleanname.notna()]
temp['duplicates'] = temp.groupby('cleanname').transform('count')
temp.sort_values(['duplicates','cleanname'], ascending=False, inplace=True)


temp.cleanname.duplicated().mean()

# Get unique years from df_final
unique_years = df['year'].unique()
print(f"Unique years found: {unique_years}")


# Create a DataFrame with all combinations of 'jamie' and 'year'
# This is effectively a cross join between jamie_mayara_cw and unique_years
jamie_year = jamie_mayara_cw.assign(key=1).merge(
    pd.DataFrame({'year': unique_years, 'key': 1}),
    on='key'
).drop('key', axis=1)
print(f"Total jamie-year combinations: {jamie_year.shape[0]}")

    
# Step 3: Merge `jamie_mayara_cw` with `df`
crosswalk_combined = pd.merge(
    jamie_year,
    df,
    left_on=['mayara', 'year'],
    right_on=['name', 'year'],
    how='outer',  # Use left join to retain all jamie-year pairs
    indicator = True  
)

'''

# Group by 'name' and 'cleanname' and aggregate the state-year pairs into a list
df['state_year'] = df['state'] + '-' + df['year']
df_collapsed = df.groupby(['name', 'cleanname'])['state_year'].apply(list).reset_index()

crosswalk_combined = pd.merge(
    jamie_mayara_cw,
    df_collapsed,
    left_on=['mayara'],
    right_on=['name'],
    how='outer',  # Use left join to retain all jamie-year pairs
    indicator = True  
)
crosswalk_combined._merge.value_counts()


# Confirm that unmatched variables from the Jamie-Mayara crosswalk all correspond to variables where keep='no' in descsave_rais_files_20180829_clean.xlsx
filter_values = crosswalk_combined.loc[crosswalk_combined._merge == 'left_only', 'mayara'].unique()
filtered_df = df_full[df_full['name'].isin(filter_values)]
tabulation = filtered_df['keep'].value_counts()
print(tabulation)



crosswalk_combined = crosswalk_combined.loc[crosswalk_combined._merge == 'both']
crosswalk_exploded = crosswalk_combined.explode("state_year").reset_index(drop=True)
crosswalk_exploded[['state', 'year']] = crosswalk_exploded['state_year'].str.split('-', expand=True)
crosswalk_exploded['year'] = crosswalk_exploded['year'].astype('int')


cw_dict = {}
for (state, year), group in crosswalk_exploded.groupby(["state", "year"]):
    state_year_key = f"{state}-{year}"
    state_year_dict = dict(zip(group["jamie"], group["mayara"]))
    cw_dict[state_year_key] = state_year_dict




def process_by_year(cw_dict, year, only_first_state=False):
    file_path = rais + f"parquet_novos/brasil{year}.parquet"
    try:
        # Load only the 'uf' column to identify unique states
        table = pq.read_table(file_path, columns=['uf'])
        unique_states = table['uf'].unique()
    
        # Iterate through unique states
        for state in unique_states:
            print(state)
    
            # Prepare list of columns to read
            state_year_key = f"{state}-{year}"
            if state_year_key not in cw_dict:
                print(f"No mapping found for state-year {state_year_key}. Skipping.")
                return
    
            columns_to_read = ['uf'] + list(cw_dict[state_year_key].keys())
    
            # Check if columns exist in the table metadata
            available_columns = set(pq.read_table(file_path, columns=None).schema.names)
            valid_columns = [col for col in columns_to_read if col in available_columns]
    

            # Read only rows corresponding to the current state and the valid columns
            state_data = pq.read_table(file_path, filters=[[('uf', '=', state)]], columns=valid_columns).to_pandas()

            # Generate categorical age fx_etaria
            if year>=1994:
                bins = [0, 14, 17, 24, 29, 39, 49, 64, np.inf]
                state_data['fx_etaria'] = pd.cut(state_data['idade'], bins=bins, right=True)

            # Log and impute missing columns
            missing_columns = set(columns_to_read) - set(valid_columns)
            for col in missing_columns:
                print(f"Column '{col}' does not exist in the table for year {year}... Imputing as missing")
                state_data[col] = ''
    
            # Generate variables that don't exist in our data but I don't think are actually used
            #for var in ['anoadmissão','cep','cepestab','clascnae95','indestabparticipapat','indpat','indraisneg','indraisnegativa','indsimples','nat_jur2016']:
            #    state_data[var] = ''

            # Rename columns based on the crosswalk dictionary for the year
            state_data = state_data.rename(columns=cw_dict[state_year_key])

            # Save the processed state data
            state_output = f"{root}/Code/replicate_mayara/unzipped/{state}{year}ID.txt"
            state_data.to_csv(state_output, sep=';', index=False)
            if only_first_state==True:
                return
            
        print(f"Successfully processed data for year {year}")
    except Exception as e:
        print(f"Error processing file {file_path} for year {year}: {e}")
        
# Process all years and track results
results = {}
for year in range(1985, 1987):
    print(f"\nProcessing year {year}...")
    results[year] = process_by_year(cw_dict, year)
    


def main():
    # Create output directory if it doesn't exist
    output_dir = f"{root}/Code/replicate_mayara/unzipped"
    os.makedirs(output_dir, exist_ok=True)
    
    # Process all years and track results
    results = {}
    for year in range(1985, 2022):
        print(f"\nProcessing year {year}...")
        results[year] = process_by_year(cw_dict, year)
    
    # Print summary
    print("\nProcessing Summary:")
    for year, status in results.items():
        print(f"{year}:")
        print(f"  Worker data: {'Success' if status['worker'] else 'Failed'}")
        print(f"  Establishment data: {'Success' if status['estab'] else 'Failed'}")

if __name__ == "__main__":
    main()






# Sample 10 rows from the data set
year = 1993
file_path = f'/home/DLIPEA/p13861161/rais/RAIS/parquet_novos/brasil{year}.parquet'
from pyarrow.parquet import ParquetFile
import pyarrow as pa 

pf = ParquetFile(file_path) 
first_ten_rows = next(pf.iter_batches(batch_size = 10000)) 
df = pa.Table.from_batches([first_ten_rows]).to_pandas()