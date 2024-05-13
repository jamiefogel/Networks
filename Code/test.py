# -*- coding: utf-8 -*-
"""
Created on Sun May 19 21:00:00 2024

@author: p13861161
"""


########################################################
# Simulating AKM with controls (see https://tlamadon.github.io/pytwoway/notebooks/fe_example.html#FE-WITH-CONTROLS)


from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import platform
import sys
import getpass
import pytwoway as tw
import bipartitepandas as bpd


### 
# Defining parameters

# Cleaning
clean_params = bpd.clean_params(
    {
        'connectedness': 'leave_out_spell',
        'collapse_at_connectedness_measure': True,
        'drop_single_stayers': True,
        'drop_returns': 'returners',
        'copy': False
    }
)
# Simulating
nl = 3
nk = 4
n_control = 2 
sim_cat_params = tw.sim_categorical_control_params({
    'n': n_control,
    'worker_type_interaction': False,
    'stationary_A': True, 'stationary_S': True
})
sim_cts_params = tw.sim_continuous_control_params({
    'worker_type_interaction': False,
    'stationary_A': True, 'stationary_S': True
})
sim_blm_params = tw.sim_blm_params({
    'nl': nl,
    'nk': nk,
    'categorical_controls': {
        'cat_control': sim_cat_params
    },
    'continuous_controls': {
        'cts_control': sim_cts_params
    },
    'stationary_A': True, 'stationary_S': True,
    'linear_additive': True
})

###
# Generating data
blm_true = tw.SimBLM(sim_blm_params)
sim_data = blm_true.simulate(return_parameters=False)
jdata, sdata = sim_data['jdata'], sim_data['sdata']
sim_data = pd.concat([jdata, sdata]).rename({'g': 'j', 'j': 'g'}, axis=1, allow_optional=True, allow_required=True)[['i', 'j1', 'j2', 'y1', 'y2', 'cat_control1', 'cat_control2', 'cts_control1', 'cts_control2']].construct_artificial_time(is_sorted=True, copy=False)



###
# Prepare data

# Convert into BipartitePandas DataFrame
bdf = bpd.BipartiteDataFrame(sim_data)
# Clean and collapse
bdf = bdf.clean(clean_params)
# Convert to long format
bdf = bdf.to_long(is_sorted=True, copy=False)


###
# Initialize and run FE estimator



# FE
fecontrol_params1 = tw.fecontrol_params(
    {
        #'he': True,
        'attach_fe_estimates': True,
        'categorical_controls': 'cat_control',
        'continuous_controls': 'cts_control',
        'Q_var': [
            tw.Q.VarCovariate('psi'),
            tw.Q.VarCovariate('alpha'),
            tw.Q.VarCovariate('cat_control'),
            tw.Q.VarCovariate('cts_control'),
            tw.Q.VarCovariate(['psi', 'alpha']),
            tw.Q.VarCovariate(['cat_control', 'cts_control'])
                 ],
        'Q_cov': [
            tw.Q.CovCovariate('psi', 'alpha'),
            tw.Q.CovCovariate('cat_control', 'cts_control'),
            tw.Q.CovCovariate(['psi', 'alpha'], ['cat_control', 'cts_control'])
        ],
        'ncore': 8
    }
)


# FE
fecontrol_params2 = tw.fecontrol_params(
    {
        'he': False,
        'ho': False,
        'attach_fe_estimates': True,
        'categorical_controls': 'cat_control',
        #'continuous_controls': 'cts_control',
        'Q_var': [
            tw.Q.VarCovariate('psi'),
            tw.Q.VarCovariate('alpha'),
            tw.Q.VarCovariate('cat_control'),
            #tw.Q.VarCovariate('cts_control'),
            tw.Q.VarCovariate(['psi', 'alpha']),
            #tw.Q.VarCovariate(['cat_control', 'cts_control'])
                 ],
        'Q_cov': [
            tw.Q.CovCovariate('psi', 'alpha'),
            #tw.Q.CovCovariate('cat_control', 'cts_control'),
            tw.Q.CovCovariate(['psi'], ['cat_control']),
            tw.Q.CovCovariate(['alpha'], ['cat_control']),
            tw.Q.CovCovariate(['psi', 'alpha'], ['cat_control'])
        ],
        'ncore': 8
    }
)

# Initialize FE estimator
fe_estimator = tw.FEControlEstimator(bdf, fecontrol_params2)
# Fit FE estimator
fe_estimator.fit()

summary = fe_estimator.summary