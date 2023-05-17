
df_matching = df[['cbo2002','clas_cnae20','yob','monthly_earnings','date','gamma','occ2Xmeso','employed_in_month','uf']]

df_matching['monthly_earnings_decile'] = df_matching.loc[df_matching.monthly_earnings>0].groupby('date')['monthly_earnings'].apply(lambda x: pd.qcut(x, q=10, labels=False))
df_matching['occ2'] = df_matching.cbo2002.str[0:2]
df_matching['ind2'] = df_matching.clas_cnae20.astype('str').str[0:2]
df_matching['age_bin'] = pd.cut((df_matching.date.dt.year - df_matching.yob), np.arange(25,70,5), labels=False)


print('COMPUTING DISTRIBUTIONS')

############################################

def compute_monthly_distribution(df_matching, mkt, var):
    print(df_matching.columns)
    ct = df_matching.groupby([mkt, 'date', var]).size().reset_index(name='count')
    # pivot the data so that the earnings deciles are the columns and the markets and months are the index
    ct_pivot = ct.pivot_table(index=[mkt, 'date'], columns=var, values='count').fillna(0)
    for c in ct_pivot.columns:
        ct_pivot = ct_pivot.rename(columns = {c: var + "_" + str(c)})
    # compute the total number of workers in each market and month
    ct_pivot['total'] = ct_pivot.sum(1)
    # compute the share of workers in each market and month that are in each earnings decile
    monthly_dist = ct_pivot.div(ct_pivot['total'], axis=0).drop(columns=['total'])
    return monthly_dist




# For each treated market identified by id, matches the closest market in the control group
## obs_id: Observation identifier. should be unique per observation.
## cov_df: Dataframe containing the numerical covariates that should be considered for the matching
## treat: Dataframe column indicator for treated markets for which we want to find a match
def match_markets(obs_id, cov_df, treat):
    import pandas as pd
    import numpy as np
    from scipy.spatial.distance import cdist
    from sklearn.preprocessing import StandardScaler
    treat  = pd.DataFrame(treat)
    cov_df = pd.DataFrame(cov_df)
    obs_id = pd.Series(obs_id)    
    #if len(obs_id) != len(obs_id.unique()):
    if obs_id.duplicated().any() == True:
        raise Exception('The variable obs_id does not uniquely identify observations.')
    if not ((len(obs_id)==len(treat)) & (len(obs_id)==cov_df.shape[0])):
        raise Exception('obs_id, cov_df, and treat should all have the same number of observations')    
    # standardizing data    
    scaler = StandardScaler()
    cov_df_std = pd.DataFrame(scaler.fit_transform(cov_df))
    # computing a squared symmetric matrix with pairwise euclidean distances based on the standardized data
    dists = pd.DataFrame(cdist(cov_df_std, cov_df_std, metric='euclidean'))
    # slicing the dists matrix to get controls only once
    dists_controls = dists.loc[(treat == 0).values,:]
    # lists store 1st and 2nd matches based on shortest and 2nd shortest distances
    matched = {'match_mkt': [], 'match_dist': []}
    for i in range(len(obs_id)):
        if treat.iloc[i].values[0] == 0:
            matched['match_mkt'].append(np.nan)
            matched['match_dist'].append(np.nan)
        elif treat.iloc[i].values[0] == 1:
            # control markets 
            matched['match_mkt'].append(obs_id[dists_controls[i].idxmin()])
            matched['match_dist'].append(dists_controls[i].min())
    return pd.DataFrame(matched)

    



for mkt in ["gamma", "occ2Xmeso"]:
    exec("earn_dist_{}_month = compute_monthly_distribution(df_matching, mkt, 'monthly_earnings_decile')".format(mkt))
    exec("occ2_dist_{}_month = compute_monthly_distribution(df_matching, mkt, 'occ2')".format(mkt))
    exec("ind2_dist_{}_month = compute_monthly_distribution(df_matching, mkt, 'ind2')".format(mkt))
    exec("age_dist_{}_month = compute_monthly_distribution(df_matching, mkt, 'age_bin')".format(mkt))
    exec("earn_dist_{}_month.to_pickle('../data/' + modelname + 'earn_dist_{}_month_df.p')".format(mkt,mkt))
    exec("occ2_dist_{}_month.to_pickle( '../data/' + modelname + 'occ_dist_{}_month_df.p')".format(mkt,mkt))
    exec("ind2_dist_{}_month.to_pickle( '../data/' + modelname + 'ind_dist_{}_month_df.p')".format(mkt,mkt))
    exec("age_dist_{}_month.to_pickle( '../data/' + modelname + 'age_dist_{}_month_df.p')".format(mkt,mkt))




# Merge together all of the distributions. Check whether we need to rename them for when multiple categorical variables have the same values (e.g. same column names)
merged_dists_gamma = pd.merge(earn_dist_gamma_month,occ2_dist_gamma_month, left_index=True, right_index=True).reset_index()
df_new = df_matching.groupby(['gamma', 'date']).agg({'monthly_earnings':'mean', 'uf':lambda x: stats.mode(x)[0][0], 'employed_in_month':'sum'}).reset_index().rename(columns={'monthly_earnings':'mean_monthly_earnings','uf':'modal_state','employed_in_month':'employment_level'})
merged_dists_gamma = merged_dists_gamma.merge(df_new, on=['gamma','date'])
merged_dists_gamma.to_csv('../data/'+modelname + '_merged_dists_gamma.csv', index=False)
del df_new

merged_dists_occ2Xmeso = pd.merge(earn_dist_occ2Xmeso_month,occ2_dist_occ2Xmeso_month, left_index=True, right_index=True).reset_index()
df_new = df_matching.groupby(['occ2Xmeso', 'date']).agg({'monthly_earnings':'mean', 'uf':'max', 'employed_in_month':'sum'}).reset_index().rename(columns={'monthly_earnings':'mean_monthly_earnings','uf':'modal_state','employed_in_month':'employment_level'})
merged_dists_occ2Xmeso = merged_dists_occ2Xmeso.merge(df_new, on=['occ2Xmeso','date'])
merged_dists_occ2Xmeso.to_csv('../data/'+modelname + '_merged_dists_occ2Xmeso.csv', index=False)
del df_new

del df_matching





############################################################################################################
# Actually do the matching
'''
df_2016 = merged_dists_gamma.loc[merged_dists_gamma.date=='2016-01']
match_markets(, cov_df, treat):
'''


# XX Add column to merged_dists with a dummy for this market treated in this month


# Compute Mahalanobis distance for all pairs of rows in the distributions
# mahal_dists = cdist(merged_dists_gamma,merged_dists_gamma,'mahalanobis')
# XX the above line was giving me the error "TypeError: float() argument must be a string or a number, not 'Period'"




# Next: find the minimum subject to constraints (e.g. cant be in same macro region or whatever; same year, same month, month before layoff, etc). 
'''
PSEUDO CODE for cross tab of counts by gamma-state

df = pd.DataFrame({'id': [1, 2, 3, 4, 5, 6],
                   'gamma': ['a', 'b', 'c', 'a', 'c', 'a'],
                   'state': ['NY', 'CA', 'NY', 'TX', 'TX', 'CA']})

# use pivot_table to count the number of observations within each gamma-state pair
pivot = pd.pivot_table(df, 
                       values='id', 
                       index='gamma', 
                       columns='state', 
                       aggfunc='count', 
                       fill_value=0)

print(pivot)

'''
pivot = pd.pivot_table(df, 
                        values='id', 
                       index='gamma', 
                       columns='state', 
                       aggfunc='count', 
                       fill_value=0)

