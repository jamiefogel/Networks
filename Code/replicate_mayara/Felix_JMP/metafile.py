#!/usr/bin/env python
import subprocess
import sys
import argparse
from datetime import datetime
from config import root


import os
import time

# Define the full paths to your files
output_prefix = root + '/Data/derived/sbm_output/'
jblocks_csv = output_prefix + 'model_sbm_mayara_1986_1990_3states_7500_jblocks.csv'
wblocks_csv = output_prefix + 'model_sbm_mayara_1986_1990_3states_7500_wblocks.csv'

# Wait until both files exist
while not (os.path.exists(jblocks_csv) and os.path.exists(wblocks_csv)):
    print("Waiting for both files to be created...")
    time.sleep(69)  # sleep for 1 second before checking again

print("Both files found. Continuing with the pipeline...")


# Define the specifications
# Each spec includes a name, a renaming rule (if applicable),
# market variables, and a file suffix used for output naming.
specs = [
    {
        "name": "gamma",
        "market_vars": ["gamma"],
        "file_suffix": "gamma",
        "_3states":""
    },
    {
        "name": "original",
        "market_vars": ["mmc", "cbo942d"],
        "file_suffix": "mmc_cbo942d",
        "_3states":""
    },
    {
        "name": "3states_gamma",
        "market_vars": ["gamma"],
        "file_suffix": "3states_gamma",
        "_3states":"_3states"
    },
    {
        "name": "3states_original",
        "market_vars": ["mmc", "cbo942d"],
        "file_suffix": "3states_mmc_cbo942d",
        "_3states":"_3states"
    },
    {
        "name": "3states_gamma1",
        "market_vars": ["gamma1"],
        "file_suffix": "3states_gamma1",
        "_3states":"_3states"
    },
    {
        "name": "3states_gamma_mcmc",
        "market_vars": ["gamma_mcmc"],
        "file_suffix": "3states_gamma_mcmc",
        "_3states":"_3states"
    },
    {
        "name": "3states_gamma1_mcmc",
        "market_vars": ["gamma1_mcmc"],
        "file_suffix": "3states_gamma1_mcmc",
        "_3states":"_3states"
    },
    {
        "name": "3states_gamma_7500",
        "market_vars": ["gamma_7500"],
        "file_suffix": "3states_gamma_7500",
        "_3states":"_3states"
    },
    {
        "name": "3states_gamma1_7500",
        "market_vars": ["gamma1_7500"],
        "file_suffix": "3states_gamma1_7500",
        "_3states":"_3states"
    },
    {
        "name": "3states_gamma_7500_mcmc",
        "market_vars": ["gamma_7500_mcmc"],
        "file_suffix": "3states_gamma_7500_mcmc",
        "_3states":"_3states"
    },
    {
        "name": "3states_gamma1_7500_mcmc",
        "market_vars": ["gamma1_7500_mcmc"],
        "file_suffix": "3states_gamma1_7500_mcmc",
        "_3states":"_3states"
    }
]

# Define which scripts to run
# Upstream Python scripts (to run once) and spec-dependent scripts.
'''
        
        
        
'''
run_configs = {
    "python_scripts": [
        "rais_010_annual_files_20210802_w_sbm.py",
        "rais_020_earliest_estab_location.py", 
        "rais_020_municipality_changes.py", 
        "rais_020_earliest_worker_characteristic_20200715.py", 
        "rais_020_earliest_firm_cnae.py",
        "rais_030_for_earnings_premia_gamma.py",
        "rais_040_firm_collapsed_gamma.py", 
        "rais_050_market_collapsed_v2_gamma.py"
        
    ],
    "stata_scripts": [
        # List only the spec-dependent do files you want to run in this batch.
        "1_2_earnings_premia_gamma.do",
        "1_3_earnings_premia_gamma_firm.do",
        "3_1_eta_estimation_gamma.do",
        "4_1_theta_estimation_simpler_jsf_3states_gamma.do"
    ]
    
}
# -------------------------------------------------------


def generate_specs_config(specs, output_path):
    with open(output_path, "w") as f:
        for spec in specs:
            name = spec["name"]
            # Join market_vars list into a space-separated string
            market_vars = " ".join(spec["market_vars"])
            file_suffix = spec["file_suffix"]
            _3states = spec["_3states"]
            f.write(f'global s_{name}_mv "{market_vars}"\n')
            f.write(f'global s_{name}_fs "{file_suffix}"\n')
            f.write(f'global s_{name}_3s "{_3states}"\n')

# Suppose your specs are defined in your metafile
config_file_path = root + 'Code/replicate_mayara/Felix_JMP/3_analysis/specs_config.do'
generate_specs_config(specs, config_file_path)

# Parse command-line arguments to control the pipeline
parser = argparse.ArgumentParser(description="Run the full pipeline with options for specs and script selection.")
parser.add_argument("--specs", nargs="+", help="List of specs to run, e.g. gamma original")
parser.add_argument("--run_python", action="store_true", help="Run the upstream Python scripts")
parser.add_argument("--run_stata", action="store_true", help="Run the spec-dependent Stata do files")
args, unknown = parser.parse_known_args()

print(f"args.run_python = {args.run_python}")
print(f"args.run_stata = {args.run_stata}")
print(f"args.specs = {args.specs}")


# Determine which specs to run; if not specified, run all available specs
if args.specs:
    specs_to_run = args.specs
else:
    specs_to_run = [spec['name'] for spec in specs]

def run_python_script(script_path, *args):
    start_time = datetime.now()
    print(f"Starting {script_path} at {start_time}")
    try:
        command = ['python', script_path] + list(args)  # Dynamically pass arguments
        print(f"Executing command: {' '.join(command)}")  # Debugging output
        sys.stdout.flush()

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # Print output line by line in real-time
        for line in iter(process.stdout.readline, ''):
            sys.stdout.write(line)
            sys.stdout.flush()
        process.stdout.close()
        return_code = process.wait()
        if return_code != 0:
            error_output = process.stderr.read()
            sys.stderr.write(error_output)
            sys.stderr.flush()
            raise Exception(f"Error running {script_path}: {error_output}")
    except Exception as e:
        raise e
    end_time = datetime.now()
    runtime = end_time - start_time
    print(f"Finished {script_path} at {end_time} (runtime: {runtime})")


def run_stata_script(do_file, spec):
    start_time = datetime.now()
    print(f"Starting Stata do file {do_file} for spec {spec} at {start_time}")
    try:
        # Adjust the command below according to your OS and Stata installation.
        # Here we pass the spec as a command-line argument to the do file.
        stata_executable = "/usr/local/stata18/stata-mp"
        process = subprocess.Popen(
            [stata_executable, '-b', 'do', do_file, f'{spec}'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        for line in iter(process.stdout.readline, ''):
            sys.stdout.write(line)
            sys.stdout.flush()
        process.stdout.close()
        return_code = process.wait()
        if return_code != 0:
            error_output = process.stderr.read()
            sys.stderr.write(error_output)
            sys.stderr.flush()
            raise Exception(f"Error running {do_file} for spec {spec}: {error_output}")
    except Exception as e:
        raise e
    end_time = datetime.now()
    runtime = end_time - start_time
    print(f"Finished {do_file} for spec {spec} at {end_time} (runtime: {runtime})")





def main():
    # Run the upstream Python scripts if flagged
    if args.run_python:
        for s in run_configs.get("python_scripts", []):
            script_path = root + 'Code/replicate_mayara/Felix_JMP/2_setup/' + s
            try:
                run_python_script(script_path, *args.specs)
            except Exception as e:
                print(str(e))
                sys.exit(1)

    # Run the Stata do files for each spec if flagged
    if args.run_stata:
        for spec in specs_to_run:
            for do_file in run_configs.get("stata_scripts", []):
                do_file_path = root + 'Code/replicate_mayara/Felix_JMP/3_analysis/' + do_file
                try:
                    run_stata_script(do_file_path, spec)
                except Exception as e:
                    print(str(e))
                    sys.exit(1)




# Only override sys.argv if the script is run interactively in Spyder.
if __name__ == '__main__':
    if len(sys.argv) == 1:
        # Simulate passing the arguments: --run_python --specs gamma
        sys.argv.extend(["--run_python", "--specs", "gamma"])
    main()
    
    
    
# Command line usage:
# python metafile.py --run_python --run_stata --specs gamma

# parallel --jobs 4 python metafile.py --run_python --run_stata --specs ::: gamma  original 3states_gamma 3states_original 3states_gamma1 3states_gamma_mcmc 3states_gamma1_mcmc 3states_gamma_7500 3states_gamma1_7500 3states_gamma_7500_mcmc 3states_gamma1_7500_mcmc

'''
echo "3states_gamma 3states_original 3states_gamma1 3states_gamma_mcmc 3states_gamma1_mcmc 3states_gamma_7500 3states_gamma1_7500 3states_gamma_7500_mcmc 3states_gamma1_7500_mcmc" | xargs -n 1 -P 4 -I {} python metafile.py  --run_stata --specs {}
'''
