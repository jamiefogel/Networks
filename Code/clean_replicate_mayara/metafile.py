# -*- coding: utf-8 -*-
"""
Created on Thu Mar 27 12:18:42 2025

@author: p13861161
"""

from config import root

scripts = ['pull_raw.py','rais_020_earliest_firm_cnae.py','rais_030_040_combined.py','rais_050_market_collapsed_new.py']

for script in scripts:
    print(f"Running {script}")
    with open(root + f'Code/clean_replicate_mayara/{script}', 'r') as f:
        code = f.read()
        exec(code)