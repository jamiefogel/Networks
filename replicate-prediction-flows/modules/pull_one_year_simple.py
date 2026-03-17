"""
Simplified version of pull_one_year.py focused only on Section 5.3 requirements.

This module loads raw RAIS data and performs only the essential preprocessing
needed for the job transition prediction analysis.
"""

import pandas as pd
import numpy as np
from datetime import datetime


def pull_one_year_simple(year, state_codes=None, age_lower=None, age_upper=None, filename=None):
    """
    Load and preprocess one year of RAIS data for Section 5.3 analysis.

    Parameters:
    - year: Year to process
    - state_codes: List of state codes to include (e.g., [31, 33, 35])
    - age_lower: Minimum age (e.g., 25)
    - age_upper: Maximum age (e.g., 55)
    - filename: Path to RAIS CSV file

    Returns:
    - DataFrame with processed data
    """
    print(f'Processing {year} at {datetime.now().strftime("%H:%M:%S")}')

    # Determine separator based on year
    if (year < 1998) | (year == 2016) | (year == 2018) | (year == 2019):
        sep = ';'
    else:
        sep = ','

    # Essential variables for Section 5.3
    vars_needed = [
        'pis',           # Worker ID
        'id_estab',      # Establishment ID
        'cbo2002',       # Occupation
        'codemun',       # Municipality
        'tipo_vinculo',  # Employment relationship type
        'idade',         # Age
        'data_adm'       # Start date (for transitions)
    ]

    # Set filename if not provided
    if filename is None:
        filename = f'~/rais/RAIS/csv/brasil{year}.csv'

    # Load raw data
    raw_data = pd.read_csv(
        filename,
        usecols=vars_needed,
        sep=sep,
        dtype={'id_estab': str, 'pis': str, 'cbo2002': str},
        parse_dates=['data_adm']
    )

    print(f"  Loaded {len(raw_data)} records")

    # Apply filters

    # Geographic filter
    if state_codes is not None:
        initial_size = len(raw_data)
        raw_data = raw_data[raw_data['codemun'].fillna(99).astype(str).str[:2].astype('int').isin(state_codes)]
        print(f"  After state filter: {len(raw_data)} records ({len(raw_data)/initial_size*100:.1f}%)")

    # Age filters
    if age_lower is not None:
        raw_data = raw_data.loc[raw_data.idade > age_lower]
    if age_upper is not None:
        raw_data = raw_data.loc[raw_data.idade < age_upper]

    print(f"  After age filter: {len(raw_data)} records")

    # Remove missing essential variables
    raw_data = raw_data.dropna(subset=['pis', 'id_estab', 'cbo2002'])

    # Remove certain employment relationship types
    raw_data = raw_data[~raw_data['tipo_vinculo'].isin([30, 31, 35])]

    print(f"  After data cleaning: {len(raw_data)} records")

    # Create derived variables
    raw_data['year'] = year
    raw_data['yob'] = raw_data['year'] - raw_data['idade']  # Year of birth
    raw_data['occ4'] = raw_data['cbo2002'].str[0:4]        # 4-digit occupation
    raw_data['jid'] = raw_data['id_estab'] + '_' + raw_data['occ4']  # Job ID
    raw_data.rename(columns={'pis': 'wid'}, inplace=True)   # Worker ID

    # Fix date formatting issues
    raw_data['start_date'] = pd.to_datetime(raw_data['data_adm'], errors='coerce')

    print(f"  Final dataset: {len(raw_data)} records with {raw_data.wid.nunique()} workers and {raw_data.jid.nunique()} jobs")

    return raw_data