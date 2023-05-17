# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 16:25:54 2023

@author: p13861161
"""

from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import random
from scipy.sparse import coo_matrix

if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')

os.chdir(homedir + '/labormkt_rafaelpereira/aug2022/dump')


a = pickle.load(open('pred_flows_jid_degs.p', 'rb'))

pred_flows_jid_cw = 





print(sparse_matrix.toarray())

for j in range(J):
    transition_matrix.getrow(j).toarray()    - degs 
    
