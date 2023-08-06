#!/bin/bash

# Get the current date/time
timestamp=$(date +%Y-%m-%d_%H-%M-%S)

directory="../../Logs/"

nohup python3 do_all_ipea.py  > "${directory}/do_all_ipea_${timestamp}.log" 2>&1 &

