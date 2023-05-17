from scipy.spatial.distance import cdist

df_matching = df[['cbo2002', 'clas_cnae20','yob','monthly_earnings','date','gamma','occ2Xmeso','employed_in_month','uf']]

# BM: we are doing earnings decile per month? should it be per market or something like that?
df_matching['monthly_earnings_decile'] = df_matching.loc[df_matching.monthly_earnings>0].groupby('date')['monthly_earnings'].apply(lambda x: pd.qcut(x, q=10, labels=False))

df_matching['occ2'] = df_matching.cbo2002.str[0:2]
df_matching['ind2'] = df_matching.clas_cnae20.astype('str').str[0:2]
df_matching['age_bin'] = pd.cut((df_matching.date.dt.year - df_matching.yob), np.arange(25,70,5), labels=False)


print('COMPUTING DISTRIBUTIONS')

############################################

# BM: final file: per market and month, the distribution of the other variable
def compute_monthly_distribution(df_matching, mkt, var):
    print(df_matching.columns)
    # BM: counting unique observations by e.g. month, gamma, occ2 ??
    ct = df_matching.groupby([mkt, 'date', var]).size().reset_index(name='count')
    # pivot the data so that the earnings deciles are the columns and the markets and months are the index
    # BM: now creating a count per gamma occ2 month, with occ2 as a column
    ct_pivot = ct.pivot_table(index=[mkt, 'date'], columns=var, values='count').fillna(0)
	for c in ct_pivot.columns:
		ct_pivot = ct_pivot.rename(columns = {c: var + "_" + c})
    # compute the total number of workers in each market and month
    # BM: now summing over all columns, i.e. getting the # of gammas that apear in the month. occ2 disappeared (?), why?. Just baseline for total!
    ct_pivot['total'] = ct_pivot.sum(1)
    # compute the share of workers in each market and month that are in each earnings decile
    monthly_dist = ct_pivot.div(ct_pivot['total'], axis=0).drop(columns=['total'])
    return monthly_dist



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
    
# Q-BM: gamma goes to rows in this merge and (also) gammas and occ2 goes to columns! why? how to differentiate them?
merged_dists_gamma = pd.merge(earn_dist_gamma_month,occ2_dist_gamma_month, left_index=True, right_index=True).reset_index()

# Q-BM: aggregates by gamma and date: summing earnings, max UF and sum employed in month (aggregating jobs then). Max UF? Max is the same as modal for string I guess
df_new = df_matching.groupby(['gamma', 'date']).agg({'monthly_earnings':'mean', 'uf':'max', 'employed_in_month':'sum'}).reset_index().rename(columns={'monthly_earnings':'mean_monthly_earnings','uf':'modal_state','employed_in_month':'employment_level'})

# BM-other merged dists
merged_dists_gamma = merged_dists_gamma.merge(df_new, on=['gamma','date'])
merged_dists_gamma.to_csv('../data/'+modelname + '_merged_dists_gamma.csv')
del df_new

merged_dists_occ2Xmeso = pd.merge(earn_dist_occ2Xmeso_month,occ2_dist_occ2Xmeso_month, left_index=True, right_index=True).reset_index()
df_new = df_matching.groupby(['occ2Xmeso', 'date']).agg({'monthly_earnings':'mean', 'uf':'max', 'employed_in_month':'sum'}).reset_index().rename(columns={'monthly_earnings':'mean_monthly_earnings','uf':'modal_state','employed_in_month':'employment_level'})
merged_dists_occ2Xmeso = merged_dists_occ2Xmeso.merge(df_new, on=['occ2Xmeso','date'])
merged_dists_occ2Xmeso.to_csv('../data/'+modelname + '_merged_dists_occ2Xmeso.csv')
del df_new



del df_matching

# XX Add column to merged_dists with a dummy for this market treated in this month


# Compute Mahalanobis distance for all pairs of rows in the distributions
# mahal_dists = cdist(merged_dists_gamma,merged_dists_gamma,'mahalanobis')
# XX the above line was giving me the error "TypeError: float() argument must be a string or a number, not 'Period'"




# Next: find the minimum subject to constraints (e.g. cant be in same macro region or whatever; same year, same month, month before layoff, etc). 
