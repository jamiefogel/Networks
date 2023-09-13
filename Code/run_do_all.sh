#!/bin/bash

timestamp=$(date +%Y-%m-%d_%H-%M-%S)
directory="../Logs/"
nohup python3 -u do_all.py > "${directory}/do_all_${timestamp}.log" 2>&1 &
PYTHON_PID=$!
echo "Linux Process ID of python3 script: $PYTHON_PID" 
echo "Linux Process ID of python3 script: $PYTHON_PID" >> "${directory}/do_all_${timestamp}.log"
