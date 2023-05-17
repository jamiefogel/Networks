from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import sys
import bisbm
import functions

homedir = os.path.expanduser('~')
os.chdir(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/code/')

m = sys.argv[1]
print('Now running micro region', m)

occvar = 'cbo1994'

modelname = str(m)
appended = pd.read_pickle('../dump/appended_sbm_'+str(m)+'.p')
# It's kinda inefficient to pickle the edgelist then load it from pickle but kept this for flexibility
bipartite_edgelist = appended[['wid','jid']].drop_duplicates(subset=['wid','jid'])
jid_occ_cw = appended[['jid',occvar]].drop_duplicates(subset=['jid',occvar])
pickle.dump( bipartite_edgelist,  open('../data/bipartite_edgelist_'+modelname+'.p', "wb" ) )
model = bisbm.bisbm()                                                                       
model.create_graph(filename='../data/bipartite_edgelist_'+modelname+'.p',min_workers_per_job=5)
model.fit(n_init=1)
# In theory it makes more sense to save these as pickles than as csvs but I keep getting an error loading the pickle and the csv works fine
model.export_blocks(output='../data/model_'+modelname+'_blocks.csv', joutput='../data/model_'+modelname+'_jblocks.csv', woutput='../data/model_'+modelname+'_wblocks.csv')
pickle.dump( model, open('../data/model_'+modelname+'.p', "wb" ) )

