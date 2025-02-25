#!/usr/bin/env python
import subprocess
import sys
from config import root
from datetime import datetime

python_scripts = [
    "rais_010_annual_files_20210802_w_sbm.py",
    "rais_020_earliest_estab_location.py", 
    "rais_020_municipality_changes.py", 
    "rais_020_earliest_worker_characteristic_20200715.py", 
    "rais_020_earliest_firm_cnae.py",
    "rais_030_for_earnings_premia_gamma.py",
    "rais_040_firm_collapsed_gamma.py", 
    "rais_050_market_collapsed_v2_gamma.py"
]

for s in python_scripts:
    start_time = datetime.now()
    print(f"Starting {s} at {start_time}")

    script_path = root + 'Code/replicate_mayara/Felix_JMP/2_setup/' + s
    try:
        # Run the script and capture output in real-time
        process = subprocess.Popen(
            ['python', script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Print the output line by line as it's produced
        for line in iter(process.stdout.readline, ''):
            sys.stdout.write(line)
            sys.stdout.flush()

        process.stdout.close()
        return_code = process.wait()

        # Capture and print any error output
        if return_code != 0:
            error_output = process.stderr.read()
            sys.stderr.write(error_output)
            sys.stderr.flush()
            print(f"Error running {s}: {error_output}")

    except Exception as e:
        print(f"Error running {s}: {e}")

    end_time = datetime.now()
    print(f"Finished {s} at {end_time}")

    # Compute runtime
    runtime = end_time - start_time
    hours, remainder = divmod(runtime.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Total runtime: {hours} hours, {minutes} minutes, {seconds} seconds")


'''
do 1_1_earnings_premia_mmc_none.do
do 1_2_earnings_premia_mmc_cbo942d.do
do 1_3_earnings_premia_mmc_cbo942d_firm.do
do 1_4_earnings_premia_mmc_none_firm.do
do 1_5_earnings_premia_Herfindahl.do
do 3_1_eta_estimation_jsf_v3.do)
do 4_1_theta_estimation_simpler_jsf.do
'''