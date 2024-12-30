#Created by Claude: https://claude.ai/chat/3075c54c-ddfb-4bb5-91d5-f133ec890acb
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import os
from datetime import datetime
from config import root, rais
import re


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




def create_codemun2_to_uf_crosswalk(year):
    file_path = rais + f"parquet_novos/brasil{year}.parquet"
    table = pq.read_table(file_path, columns=['uf','codemun'])
    df = table.to_pandas()
    df = df.loc[df.codemun.notna()]
    df['codemun2'] = df['codemun'].astype(str).str[:2]
    cw = df[['uf','codemun2']].drop_duplicates()
    codemun2_to_uf = cw.set_index('codemun2').sort_index()['uf'].to_dict()
    return codemun2_to_uf

codemun2_to_uf = create_codemun2_to_uf_crosswalk(1989)

def create_crosswalk():
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
            'clascnae95'	      :'clas_cnae10',
            'cnae95classe'       :'clas_cnae10',
            # Only exists pre-1994
            'cboocupação':'cbo1994', #This is the only CBO var we have back then so that's my best guess. Also cboocupação only exists before 1994
            # These don't exist in our data but I don't think they're needed so creating placeholdeers
            'anoadmissão'         :'anoadmissão'          ,
            'cep'		      :'cep'                  ,
            'cepestab'	      :'cepestab'             ,
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
    
    crosswalk_combined = crosswalk_combined.loc[crosswalk_combined._merge == 'both']
    crosswalk_exploded = crosswalk_combined.explode("state_year").reset_index(drop=True)
    crosswalk_exploded[['state', 'year']] = crosswalk_exploded['state_year'].str.split('-', expand=True)
    crosswalk_exploded['year'] = crosswalk_exploded['year'].astype('int')
    
    
    cw_dict = {}
    for (state, year), group in crosswalk_exploded.groupby(["state", "year"]):
        state_year_key = f"{state}-{year}"
        state_year_dict = dict(zip(group["jamie"], group["mayara"]))
        cw_dict[state_year_key] = state_year_dict
    
    return cw_dict
    
def process_by_year(cw_dict, year, codemun2_to_uf, rais, root, only_first_state=False):
    """
    Processes Parquet files by year, handling cases where 'uf' is missing by deriving it from 'codemun'.
    """
    
    file_path = os.path.join(rais, f"parquet_novos/brasil{year}.parquet")
    using_uf = True
    codemun_crosswalk = {}
    
    cw_dict = create_crosswalk()

    try:
        # Wrap the entire function logic within try-except for file-level errors
        try:
            table = pq.read_table(file_path, columns=['uf'])
            unique_states = table['uf'].unique()
            if unique_states.null_count == len(unique_states):
                raise ValueError("'uf' column contains only null values.")
            else:
                unique_states = [state.upper() for state in unique_states.drop_null().to_pylist()]
        except (KeyError, ValueError):
            print("Warning: 'uf' column not found or contains only null values. Attempting to generate from 'codemun'.")
            try:
                table = pq.read_table(file_path, columns=['codemun'])
                df = table.to_pandas()
                df = df[df['codemun'].notna()]
                df['codemun2'] = df['codemun'].astype(str).str.zfill(6).str[:2]
                codemun_crosswalk = df.groupby('codemun2')['codemun'].apply(list).to_dict()
                unique_states = list(codemun_crosswalk.keys())
                using_uf = False
            except Exception as e:
                print(f"Error generating 'uf' from 'codemun': {e}")
                return  # Exit the function as 'uf' cannot be derived

        for state in unique_states:
            try:
                # Original state-level processing block
                if not using_uf:
                    uf = codemun2_to_uf.get(state)
                    if not uf:
                        print(f"No 'uf' mapping found for codemun2 '{state}'. Skipping.")
                        continue
                    state_year_key = f"{uf}-{year}"
                else:
                    uf = state
                    state_year_key = f"{state}-{year}"

                print(f"Processing state-year: {state_year_key}")

                if state_year_key not in cw_dict:
                    print(f"No mapping found for state-year '{state_year_key}'. Skipping.")
                    continue

                column_rename_dict = cw_dict.get(state_year_key, {})
                columns_to_read = set(['uf', 'codemun'] + list(column_rename_dict.keys()))
                available_columns = set(pq.read_table(file_path, columns=None).schema.names)
                valid_columns = [col for col in columns_to_read if col in available_columns]

                if using_uf:
                    state_data = pq.read_table(
                        file_path,
                        filters=[[('uf', '=', state)]],
                        columns=valid_columns
                    ).to_pandas()
                else:
                    codemuns = codemun_crosswalk.get(state, [])
                    if not codemuns:
                        print(f"No 'codemun' values found for codemun2 '{state}'. Skipping.")
                        continue

                    state_data = pq.read_table(
                        file_path,
                        filters=[[('codemun', 'in', codemuns)]],
                        columns=valid_columns
                    ).to_pandas()
                    state_data['uf'] = state_data['codemun'].astype(str).str.zfill(6).str[:2].map(codemun2_to_uf)

                # Rename and save
                state_data = state_data.rename(columns=column_rename_dict)
                state_output = os.path.join(root, "Code", "replicate_mayara", "unzipped", f"{uf.upper()}{year}ID.TXT")
                os.makedirs(os.path.dirname(state_output), exist_ok=True)
                state_data.to_csv(state_output, sep=';', index=False)
                print(f"Successfully processed data for state '{uf.upper()}' for year {year}")

                if only_first_state:
                    return

            except Exception as e:
                print(f"Error processing state '{state}' for year {year}: {e}")
                continue

        print(f"Successfully processed all applicable states for year {year}")

    except FileNotFoundError as e:
        print(f"File not found for year {year}: {e}")
        return
    except Exception as e:
        print(f"Unexpected error processing year {year}: {e}")
        return



# XX I don't think this is gonig to work because the naming is all just too messed up   
def process_by_year_03_07(cw_dict, year, codemun2_to_uf, rais, root, only_first_state=False):
    """
    Processes Parquet files by year, handling cases where 'uf' is missing by deriving it from 'codemun'.
    """
    uf_to_region = {
        'RO': 'north', 'AC': 'north', 'AM': 'north', 'RR': 'north', 'PA': 'north', 'AP': 'north', 'TO': 'north',
        'MA': 'northeast', 'PI': 'northeast', 'CE': 'northeast', 'RN': 'northeast', 'PB': 'northeast',
        'PE': 'northeast', 'AL': 'northeast', 'SE': 'northeast', 'BA': 'northeast',
        'MG': 'southeast', 'ES': 'southeast', 'RJ': 'southeast', 'SP': 'southeast',
        'PR': 'south', 'SC': 'south', 'RS': 'south',
        'MS': 'centerwest', 'MT': 'centerwest', 'GO': 'centerwest', 'DF': 'centerwest'
    }

    # Step 2: Define year-specific region names
    region_names_by_year = {
        2003: {'north': 'Norte2003', 'northeast': 'Nordeste2003', 'centerwest': 'Centro_Oeste2003',
               'south': 'Sul2003', 'southeast': 'ES_RJ_MG2003'},
        2004: {'north': 'Norte', 'northeast': 'nordeste', 'centerwest': 'centro_oeste',
               'south': 'sul', 'southeast': 'sudeste'},
        2005: {'north': 'norte05', 'northeast': 'nordeste05', 'centerwest': 'centro05',
               'south': 'sul05', 'southeast': 'sudeste05'},
        2006: {'north': 'norte06', 'northeast': 'nordeste06', 'centerwest': 'centro06',
               'south': 'sul06', 'southeast': 'sudeste06'},
        2007: {'north': 'norte07', 'northeast': 'nordeste07', 'centerwest': 'centro_oeste07',
               'south': 'sul07', 'southeast': 'sudeste07'}
    }

    
    
    file_path = os.path.join(rais, f"parquet_novos/brasil{year}.parquet")
    using_uf = True
    codemun_crosswalk = {}


    table = pq.read_table(file_path, columns=['uf']).to_pandas()
    table['base_region'] = table['uf'].map(uf_to_region)
    # Map the base region to the year-specific region names
    table['region'] = table['base_region'].map(region_names_by_year[year])
    
    unique_states = table['region'].unique()
    
    for state in unique_states:
        try:
            # Original state-level processing block
            if not using_uf:
                uf = codemun2_to_uf.get(state)
                if not uf:
                    print(f"No 'uf' mapping found for codemun2 '{state}'. Skipping.")
                    continue
                state_year_key = f"{uf}-{year}"
            else:
                uf = state
                state_year_key = f"{state}-{year}"

            print(f"Processing state-year: {state_year_key}")

            if state_year_key not in cw_dict:
                print(f"No mapping found for state-year '{state_year_key}'. Skipping.")
                continue

            column_rename_dict = cw_dict.get(state_year_key, {})
            columns_to_read = set(['uf', 'codemun'] + list(column_rename_dict.keys()))
            available_columns = set(pq.read_table(file_path, columns=None).schema.names)
            valid_columns = [col for col in columns_to_read if col in available_columns]

            if using_uf:
                state_data = pq.read_table(
                    file_path,
                    filters=[[('uf', '=', state)]],
                    columns=valid_columns
                ).to_pandas()
            else:
                codemuns = codemun_crosswalk.get(state, [])
                if not codemuns:
                    print(f"No 'codemun' values found for codemun2 '{state}'. Skipping.")
                    continue

                state_data = pq.read_table(
                    file_path,
                    filters=[[('codemun', 'in', codemuns)]],
                    columns=valid_columns
                ).to_pandas()
                state_data['uf'] = state_data['codemun'].astype(str).str.zfill(6).str[:2].map(codemun2_to_uf)

            # Rename and save
            state_data = state_data.rename(columns=column_rename_dict)
            state_output = os.path.join(root, "Code", "replicate_mayara", "unzipped", f"{uf.upper()}{year}ID.TXT")
            os.makedirs(os.path.dirname(state_output), exist_ok=True)
            state_data.to_csv(state_output, sep=';', index=False)
            print(f"Successfully processed data for state '{uf.upper()}' for year {year}")

            if only_first_state:
                return

        except Exception as e:
            print(f"Error processing state '{state}' for year {year}: {e}")
            continue

    print(f"Successfully processed all applicable states for year {year}")









def main():
    # Create output directory if it doesn't exist
    output_dir = f"{root}/Code/replicate_mayara/unzipped"
    os.makedirs(output_dir, exist_ok=True)
    
    cw_dict = create_crosswalk()
    
    # Process all years
    for year in range(1985, 2017):
        print(f"\nProcessing year {year}...")
        process_by_year(cw_dict, year, codemun2_to_uf, rais, root)
    
if __name__ == "__main__":
    main()





'''
# Sample 10 rows from the data set
year = 1993
file_path = f'/home/DLIPEA/p13861161/rais/RAIS/parquet_novos/brasil{year}.parquet'
from pyarrow.parquet import ParquetFile
import pyarrow as pa 

pf = ParquetFile(file_path) 
first_ten_rows = next(pf.iter_batches(batch_size = 10000)) 
df = pa.Table.from_batches([first_ten_rows]).to_pandas()
'''