# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 11:56:19 2024

@author: p13861161
"""

import pandas as pd


def merge_aguinaldo_onet(df, aguinaldo_dir=root+'/Data/raw/db_24_1/'):
    # Factor names and descriptions from Aguinaldo. See https://mail.google.com/mail/u/0/#inbox/FMfcgzGxTPDrccpzvkjfSwhqQhQxvlKl
    factor_info = {
        "Factor1": {
            "name": "Cognitive skills",
            "description": "Verbal, linguistic and logical abilities"
        },
        "Factor2": {
            "name": "Operational skills",
            "description": "Maintenance, repair and operation skills"
        },
        "Factor3": {
            "name": "Social and emotional skills",
            "description": "Interpersonal skills"
        },
        "Factor4": {
            "name": "Management skills",
            "description": "Manage resources; build teams; coordinate, motivate and guide subordinates"
        },
        "Factor5": {
            "name": "Physical skills",
            "description": "Bodily-Kinesthetic abilities: body strength; equilibrium; stamina; flexibility"
        },
        "Factor6": {
            "name": "Transportation skills",
            "description": "Visual-spatial skills: Spatial orientation; far and night vision; geography and transportation knowledge"
        },
        "Factor7": {
            "name": "Social sciences skills",
            "description": "Social sciences, education and foreign language skills"
        },
        "Factor8": {
            "name": "Accuracy skills",
            "description": "Being exact and accurate; paying attention to detail; work under pressure and in repetitive settings"
        },
        "Factor9": {
            "name": "Design & engineering skills",
            "description": "Design, engineering and construction skills"
        },
        "Factor10": {
            "name": "Artistic skills",
            "description": "Artistic skills, creativity, unconventional; communications and media"
        },
        "Factor11": {
            "name": "Life sciences skills",
            "description": "Biology, chemistry and medical sciences skills"
        },
        "Factor12": {
            "name": "Information technology skills",
            "description": "Telecommunications, computer operation and programming skills"
        },
        "Factor13": {
            "name": "Sales skills",
            "description": "Sales and Marketing, deal with customers, work under competition"
        },
        "Factor14": {
            "name": "Self-reliance skills",
            "description": "Independence, initiative, innovation"
        },
        "Factor15": {
            "name": "Information processing skills",
            "description": "Retrieve, process and pass-on information"
        },
        "Factor16": {
            "name": "Teamwork skills",
            "description": "Work with colleagues, coordinate others"
        }
    }
    factor_rename_map = {f"Factor{i}": factor_info[f"Factor{i}"]["name"] for i in range(1, 17)}
    
    # Extract all the names and descriptions into lists (for future reference)
    factor_descriptions = [info["description"] for info in factor_info.values()]
    factor_names        = [info["name"]        for info in factor_info.values()]
    
    
    # These are O*NET scores and factors. I believe that the variable 'O' is a unique identifier corresponding to unique O*NET occupations. 
    onet_scores = pd.read_sas(aguinaldo_dir + 'scores_o_br.sas7bdat')
    # Keep only the unique identifier and the factors
    onet_scores = onet_scores[[f'Factor{i}' for i in range(1, 17)] + ['O']]

    # This is a mapping of O*NET occs to Brazilian occ-sector pairs. Some Brazilian occs are mapped to different O*NET occs depending on the sector. Otherwise, this would just be a mapping between Brazilian and O*NET occs.
    spine =  pd.read_sas(aguinaldo_dir + 'cbo2002_cnae20_o.sas7bdat').astype(float)
    
    # Merge the spine of Brazilian occ-sector pairs to the O*NET scores
    onet_merged = onet_scores.merge(spine, on=['O'], how="left")
    
    # Convert columns in onet_merged to string if they are not already
    onet_merged['cbo2002'] = onet_merged['cbo2002'].astype('Int64')
    onet_merged['cla_cnae20'] = onet_merged['cla_cnae20'].astype('Int64')
    
    # Rename columns using Aguinaldo's labels 
    onet_merged.rename(columns=factor_rename_map, inplace=True)

    # Assuming raw and onet_merged are your DataFrames
    merge_keys = {'left': ['cbo2002', 'clas_cnae20'], 'right': ['cbo2002', 'cla_cnae20']}
    
    dups = onet_merged.groupby(['cbo2002', 'cla_cnae20']).size().reset_index(name='dup')
    onet_merged = onet_merged.drop_duplicates(subset=['cbo2002', 'cla_cnae20'])

    df = df.merge(
        onet_merged,
        left_on=merge_keys['left'],
        right_on=merge_keys['right'],
        how='left',
        suffixes=[None, '_y'],
        validate='m:1',
        indicator='_merge_ONET'
    )
    return df
  
  

  
''' Explore correlations between the Aguinaldo factors
# https://chatgpt.com/c/e64c9c7f-e448-4f53-8694-9639f859207e

import pandas as pd

# Assuming onet_merged is your DataFrame
correlation_matrix = onet_merged.corr()


from scipy.cluster.hierarchy import linkage, dendrogram
import matplotlib.pyplot as plt
import seaborn as sns

# Compute the linkage matrix
linkage_matrix = linkage(correlation_matrix, method='ward')

# Plot the dendrogram
plt.figure(figsize=(10, 7))
dendrogram(linkage_matrix, labels=correlation_matrix.columns, leaf_rotation=90)
plt.title('Hierarchical Clustering Dendrogram')
plt.xlabel('Feature')
plt.ylabel('Distance')
plt.show()


'''