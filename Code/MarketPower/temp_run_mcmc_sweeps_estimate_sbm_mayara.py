
import pandas as pd
import numpy as np
from config import root
import time
import psutil
import graph_tool.all as gt
import gc
import matplotlib.pyplot as plt
import pickle
import bisbm
from datetime import datetime


print('Starting SBM section at ', datetime.now())
modelname = 'sbm_mayara_1986_1990_3states'
model = pickle.load( open(root + 'Data/derived/sbm_output/model_'+modelname+'.p', "rb" ))
model_mcmc=pickle.load(open(root + './Data/derived/sbm_output/model_sbm_mayara_1986_1990_3states_mcmc.p', "rb"))
print(model.state)
model.state = model_mcmc[0]
print(model.state)
print('SBM section complete at ', datetime.now())

run_sbm_mcmc=True
if run_sbm_mcmc==True:
    model.mcmc_sweeps(root + './Data/derived/sbm_output/model_'+modelname+'_mcmc.p', tempsavedir=root + './Data/derived/sbm_output/', numiter=1000, seed=734)
    pickle.dump( model, open(root + './Data/derived/sbm_output/model_'+modelname+'_mcmc.p', "wb" ) )


