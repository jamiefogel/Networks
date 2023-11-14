#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  5 09:54:00 2021

@author: jfogel
"""

import pandas as pd
import os
from sklearn.cluster import KMeans
import pickle
import numpy as np

homedir = os.path.expanduser('~')

# These are O*NET scores and factors. I believe that the variable 'O' is a unique identifier corresponding to unique O*NET occupations. 
onet_scores = pd.read_sas('../Data/from_aguinaldo/db_24_1/scores_o_br.sas7bdat')

# This is a mapping of O*NET occs to Brazilian occ-sector pairs. Some Brazilian occs are mapped to different O*NET occs depending on the sector. Otherwise, this would just be a mapping between Brazilian and O*NET occs.
spine =  pd.read_sas(root + 'Data/from_aguinaldo/db_24_1/cbo2002_cnae20_o.sas7bdat').astype(float)

# Merge the spine of Brazilian occ-sector pairs to the O*NET scores
onet_merged = onet_scores.merge(spine, on=['O'], how="left")


cols = [c for c in onet_merged.columns if c[0] == '_']
X = onet_merged[cols]
kmeans = KMeans(n_clusters=290, random_state=0).fit(X)
labels  = onet_merged[['cbo2002','cla_cnae20']].rename(columns={'cla_cnae20':'clas_cnae20'})
labels['kmeans'] = kmeans.labels_ + 1

pickle.dump(kmeans, open('kmeans.p', 'wb'))
kmeans = pickle.load(open('kmeans.p', 'rb'))


data_full = pd.read_csv(root + 'Data/RAIS_exports/earnings_panel/panel_rio_2009_2012.csv') 

data_full = data_full.merge(labels, left_on=['cbo2002_first','clas_cnae20_first'], right_on=['cbo2002','clas_cnae20'], how='left', suffixes=[None, '_y'], indicator=True)

# missing 1 and 71, 115 only appears once. Look at temp.csv for more details
# data_full.kmeans.value_counts(dropna=False).to_csv('temp.csv')
# labels.loc[labels['kmeans']==1]['cbo2002'].unique()
# labels.loc[labels['kmeans']==71]['cbo2002'].unique()
# labels.loc[labels['kmeans']==115]['cbo2002'].unique()
# data_full.kmeans.value_counts(dropna=False)


kmeans_count = data_full.groupby('kmeans')['kmeans'].transform('count')
data_full['kmeans'].loc[kmeans_count<5000] = np.nan
data_full['kmeans'] = data_full.kmeans.rank(method='dense', na_option='keep')
data_full['kmeans'].loc[np.isnan(data_full.kmeans)] = -1
data_full['kmeans']  = data_full['kmeans'].astype(int)



data_full.to_csv( root + 'Data/RAIS_exports/earnings_panel/panel_rio_2009_2012_w_kmeans.csv')

# The issue here is that there's one kmeans group that isn't present in all years so that throws things off.


