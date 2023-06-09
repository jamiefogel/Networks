#!/bin/bash

# Define an array of parameter values
cores=5
cores_list=($(seq 1 ${cores}))

# Iterate through the array and execute the script with each parameter value
for c in "${cores_list[@]}"; do
    nohup python3 parallel_jobtransitions_market_based_multiple_mkts.py "$c" "$cores" > "output_${c}_${cores}.log" 2>&1 &
done

