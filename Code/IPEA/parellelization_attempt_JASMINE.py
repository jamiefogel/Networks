#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 14:21:36 2023

@author: bm
"""

import concurrent.futures


if __name__ == '__main__':

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_cores) as executor:
        # submit each loop iteration to the executor
        futures = [executor.submit(prediction_error, j, cjid, cg, ajid, ag) for j in range(J)]

# TASKS
# how do we define the number of cores we are using?
# how to save the results from time to time?



# how to print the progress from time to time? include the printing inside the function
# how to retrieve the results?
results = [future.result() for future in futures]
results




