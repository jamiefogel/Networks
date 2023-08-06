
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import bisbm


def do_everything(firstyear_panel, lastyear_panel, firstyear_sbm, lastyear_sbm, municipality_codes, modelname, vars, nrows=None, run_sbm=False, pull_raw_data=False, append_raw_data=False, run_create_earnings_panel=False):


    print('Run SBM: ', run_sbm)
    print('Pull raw data: ', pull_raw_data)
    print('Append raw data: ', append_raw_data)
    print('Run create_earnings_panel(): ', run_create_earnings_panel)
    #################################################################
    # Load and append raw data
    ################################
    #################################

    if pull_raw_data == True:
        for year in range(firstyear_panel,lastyear_panel+1):
            pull_one_year(year, vars, municipality_codes, savefile='./Data/derived/raw_data_' + modelname + '_' + str(year) + '.p', nrows=nrows)
        
    if append_raw_data==True:
        for year in range(firstyear_panel,lastyear_panel+1):
            print(year)
            df = pickle.load( open('./Data/derived/raw_data_' + modelname + '_' + str(year) + '.p', "rb" ) )
            if year>firstyear_panel:
                appended = df.append(appended, sort=True)
            else:
                appended = df
            del df
        appended.to_pickle('./Data/derived/appended_'+modelname+'.p')
    else:
        appended = pd.read_pickle('./Data/derived/appended_'+modelname+'.p')
       
    
    #################################################################
    # Run SBM
    #################################################################
    
    if run_sbm == True:
        # It's kinda inefficient to pickle the edgelist then load it from pickle but kept this for flexibility
        bipartite_edgelist = appended.loc[(appended['year']>=firstyear_sbm) & (appended['year']<=lastyear_sbm)][['wid','jid']].drop_duplicates(subset=['wid','jid'])
        jid_occ_cw = appended.loc[(appended['year']>=firstyear_sbm) & (appended['year']<=lastyear_sbm)][['jid','cbo2002']].drop_duplicates(subset=['jid','cbo2002'])
        pickle.dump( bipartite_edgelist,  open('./Data/derived/bipartite_edgelist_'+modelname+'.p', "wb" ) )
        model = bisbm.bisbm()                                                                       
        model.create_graph(filename='./Data/derived/bipartite_edgelist_'+modelname+'.p',min_workers_per_job=5)
        model.fit(n_init=1)
        # In theory it makes more sense to save these as pickles than as csvs but I keep getting an error loading the pickle and the csv works fine
        model.export_blocks(output='./Data/derived/sbm_output/model_'+modelname+'_blocks.csv', joutput='./Data/model_'+modelname+'_jblocks.csv', woutput='./Data/derived/sbm_output/model_'+modelname+'_wblocks.csv')
        pickle.dump( model, open('./Data/derived/sbm_output/model_'+modelname+'.p', "wb" ) )
        
    
        
    #################################################################
    # Create earnings panel
    #################################################################
    
    if run_create_earnings_panel==True:
        create_earnings_panel(modelname, appended, firstyear_panel, lastyear_panel)



