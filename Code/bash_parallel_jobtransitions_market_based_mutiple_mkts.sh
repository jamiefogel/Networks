#!/bin/bash

# Define an array of parameter values
cores=26
cores_list=($(seq 1 ${cores}))

# Get the current date/time
timestamp=$(date +%Y-%m-%d_%H-%M-%S)

#directory="../../Logs/job_transitions_${timestamp}"
#mkdir "${directory}"
directory="./Logs/job_transitions"

# Iterate through the array and execute the script with each parameter value
for c in "${cores_list[@]}"; do
    nohup python3 ./Code/parallel_jobtransitions_market_based_multiple_mkts.py "$c" "$cores" > "${directory}/core${c}_${cores}.log" 2>&1 &
done


wait

nohup python3 ./Code/parallel_jobtransitions_stack_results.py
