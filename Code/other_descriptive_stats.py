import seaborn as sns
import sklearn.metrics

####################################################################################
# Compute NMI
####################################################################################


data_full = pd.read_csv(mle_data_filename)
data_full = data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1) & (data_full['occ4_first_recode']!=-1)]
data_full = data_full.loc[(data_full.year>=2009) & (data_full.year<=2011)]

print('Printing normalized mutual information between pairs of variables (in correlogram.py).')
with open(figuredir + "nmi.tex", "w") as f:  
    for idx in [('iota','gamma'), ('iota','cbo2002_first'), ('iota','clas_cnae20_first'), ('gamma','cbo2002_first'), ('gamma','clas_cnae20_first'), ('cbo2002_first','clas_cnae20_first')]:
        nmi = sklearn.metrics.normalized_mutual_info_score(data_full[idx[0]], data_full[idx[1]], average_method='arithmetic')
        f.write(idx[0].replace('_','\_') + ' & ' + idx[1].replace('_','\_')  + ' & ' + str(round(nmi,3)) + '\n')
        print(idx[0], idx[1], round(nmi, 3))

print('Done with NMI')



####################################################################################
# Compute occupation and industry transition rates
####################################################################################

# XX This doesn't work because I didn;t keep grau_instr, id_estab, or cnpj_raiz and jid/wid need to jid_masked/wid_masked.
df = pd.read_csv(mle_data_filename)
df = df.loc[(df.year>=2009) & (df.year<=2011)][['wid_masked','jid_masked','cbo2002','gamma','sector_IBGE','clas_cnae20','grau_instr','cnpj_raiz','id_estab']].rename(columns={'cnpj_raiz':'id_firm'})

df['occ4'] = pd.to_numeric(df['cbo2002'].astype(str).str.slice(0,4), errors='coerce')
df['occ2'] = pd.to_numeric(df['cbo2002'].astype(str).str.slice(0,2), errors='coerce')
df['occ1'] = pd.to_numeric(df['cbo2002'].astype(str).str.slice(0,1), errors='coerce')
df['jid_masked'].loc[df.jid_masked==-1] = np.nan

df['jid_masked_lag'] = df.groupby(['wid_masked'])['jid_masked'].shift(1)
df['id_estab_lag'] = df.groupby(['wid_masked'])['id_estab'].shift(1)
df['id_firm_lag'] = df.groupby(['wid_masked'])['id_firm'].shift(1)
df['occ1_lag'] = df.groupby(['wid_masked'])['occ1'].shift(1)
df['occ2_lag'] = df.groupby(['wid_masked'])['occ2'].shift(1)
df['occ4_lag'] = df.groupby(['wid_masked'])['occ4'].shift(1)
df['gamma_lag'] = df.groupby(['wid_masked'])['gamma'].shift(1)
df['cbo2002_lag'] = df.groupby(['wid_masked'])['cbo2002'].shift(1)
df['clas_cnae20_lag'] = df.groupby(['wid_masked'])['clas_cnae20'].shift(1)
df['sector_IBGE_lag'] = df.groupby(['wid_masked'])['sector_IBGE'].shift(1)
df['job_change'] = (df.jid_masked != df.jid_masked_lag) & (pd.isnull(df.jid_masked)==False) & (pd.isnull(df.jid_masked_lag)==False)
df['educ'] = df.grau_instr.map({1:'dropout',2:'dropout',3:'dropout',4:'dropout',5:'dropout',6:'dropout',7:'hs',8:'some_college',9:'college',10:'grad',11:'grad'})

changes_df = df.loc[df.job_change==True][['cbo2002','cbo2002_lag','clas_cnae20','clas_cnae20_lag','gamma','gamma_lag','occ1','occ1_lag','occ2','occ2_lag','occ4','occ4_lag','sector_IBGE', 'sector_IBGE_lag', 'educ','id_estab','id_estab_lag','id_firm','id_firm_lag']]
changes_df['change_id_estab'] = changes_df.id_estab!=changes_df.id_estab_lag
changes_df['change_id_firm'] = changes_df.id_firm!=changes_df.id_firm_lag
changes_df['change_occ6'] = changes_df.cbo2002!=changes_df.cbo2002_lag
changes_df['change_occ1']= changes_df.occ1!=changes_df.occ1_lag
changes_df['change_occ2']= changes_df.occ2!=changes_df.occ2_lag
changes_df['change_occ4']= changes_df.occ4!=changes_df.occ4_lag
changes_df['change_ind'] = changes_df.clas_cnae20!=changes_df.clas_cnae20_lag
changes_df['change_sector'] = changes_df.sector_IBGE!=changes_df.sector_IBGE_lag
changes_df['change_gamma']= changes_df.gamma!=changes_df.gamma_lag

print('Printing fractions of job changes that also change iota/gamma/occ/etc (in correlogram.py)')
print(changes_df.change_occ1.mean())
print(changes_df.change_occ2.mean())
print(changes_df.change_occ4.mean())
print(changes_df.change_occ6.mean())
print(changes_df.change_ind.mean())
print(changes_df.change_sector.mean())
print(changes_df.change_gamma.mean())
print(changes_df.change_id_firm.mean())
print(changes_df.change_id_estab.mean())
output_df = pd.DataFrame(columns=['Variable','All Job Changes','Firm Change Only', 'No Firm Change'])
vars = ('change_occ1', 'change_occ2', 'change_occ4', 'change_occ6', 'change_ind', 'change_sector', 'change_gamma', 'change_id_firm', 'change_id_estab')
varnames = ('1-digit Occupation', '2-digit Occupation', '4-digit Occupation', '6-digit Occupation', '5-digit Industry', 'Sector (IBGE)', 'Market $\g$', 'Firm', 'Establishment')

idx = 0
for v in vars:
    newrow = {'Variable':varnames[idx],'All Job Changes':round(changes_df[v].mean(),3),'Firm Change Only':round(changes_df.loc[changes_df.change_id_firm==1][v].mean(),3),'No Firm Change':round(changes_df.loc[changes_df.change_id_firm==0][v].mean(),3)}
    output_df = output_df.append(newrow, ignore_index=True)
    idx = idx+1

print(output_df)
output_df.to_latex(root + '/Results/summary_stats/transitions_table.tex', index=False)








'''
1	Analfabeto, inclusive o que, embora tenha recebido instrução, não se alfabetizou.
2	Até o 5º ano incompleto do Ensino Fundamental (antiga 4ª série) que se tenha alfabetizado sem ter frequentado escola regular.
3	5º ano completo do Ensino Fundamental.
4	Do 6º ao 9º ano do Ensino Fundamental incompleto (antiga 5ª à 8ª série).
5	Ensino Fundamental completo.
6	Ensino Médio incompleto.
7	Ensino Médio completo.
8	Educação Superior incompleta.
9	Educação Superior completa.
10	Mestrado completo.
11	Doutorado completo.
'''


# From page 9 of David Arnold's JMP: "Figure A1 computes the probability a job transition is within a given occupation or industry cell using Brazilian matched employer-employee data. As can be seen in the figure, at the 1-digit level, about 60 percent of transitions are within the same occupation, while about 50 percent are within the same industry. At the 4-digit level, about 22 percent of job transitions are within the same occupation, while about 19 percent are within the same industry." So we're in the same ballpark even if we're using somewhat different samples.


# If two rows of a matrix are identical, then the matrix is singular (https://math.libretexts.org/Bookshelves/Linear_Algebra/Book%3A_A_First_Course_in_Linear_Algebra_(Kuttler)/03%3A_Determinants/3.02%3A_Properties_of_Determinants). Is there a continuous metric of matrix singularity (like if it's equal to 1 the matrix is singular and if it's equal to 0 then the matrix is as orthogonal as possible)? If so, this could complement the correlograms. 



#correlogram(psi_hat, figuredir+'correlograms_' + worker_type_var + '_' + job_type_var + '.png' , figuredir+'c' + worker_type_var + '_' + job_type_var + '.png' ,sorted=False)

####################################################################################
# Variance decomposition with earnings that didn't work
####################################################################################

data_full = pd.read_csv(mle_data_filename)
data_full = data_full.loc[(data_full['gamma']>0) & (data_full['iota']!=-1)]

df = data_full[['iota','gamma','occ4_first_recode','sector_IBGE','ln_real_hrly_wage_dec']]
df['iota_mean_wage'] = df.groupby('iota'             )['ln_real_hrly_wage_dec'].transform('mean') 
df['occ4_mean_wage'] = df.groupby('occ4_first_recode')['ln_real_hrly_wage_dec'].transform('mean')
df['demeaned_ln_wage_iota'] = df.ln_real_hrly_wage_dec - df.iota_mean_wage
df['demeaned_ln_wage_occ4'] = df.ln_real_hrly_wage_dec - df.occ4_mean_wage

df.ln_real_hrly_wage_dec.var()
df.demeaned_ln_wage_iota.var()
df.iota_mean_wage.var()

df.ln_real_hrly_wage_dec.var()
df.demeaned_ln_wage_occ4.var()
df.occ4_mean_wage.var()





####################################################################################
# iota-occ4 crosstab
# - If all we did was replicate occ4 this would be diagonal. Clearly it is not. 
####################################################################################



data_full = pd.read_csv(mle_data_filename)
crosstab_iota_occ4 = pd.crosstab(index = data_full.iota.loc[data_full.iota!=-1], columns = data_full.occ4_first_recode.loc[data_full.occ4_first_recode!=-1])

from matplotlib.colors import LogNorm, Normalize
fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
sns.heatmap(
     crosstab_iota_occ4+.000001, # Add epsilon to avoid log(0)
     norm=LogNorm(),
     #vmin=-1, vmax=1, center=0,
     cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
     #cmap=sns.color_palette("coolwarm", n_colors=20),
     #cmap=sns.color_palette("RdBu", 100),
     square=False,
     xticklabels=False,
     yticklabels=False,
     ax=ax
 )
#ax.set_aspect(1.2)
ax.tick_params(axis='both', which='major', labelsize=18)
ax.figure.savefig(figuredir + 'iota_occ4_crosstab_heatmap.png', dpi=300, bbox_inches="tight")



# Same thing but rescaling it so it's the share of the iota not the raw count
crosstab_iota_occ4_scale = crosstab_iota_occ4.div(crosstab_iota_occ4.sum(axis=1), axis=0)

from matplotlib.colors import LogNorm, Normalize
fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
sns.heatmap(
     crosstab_iota_occ4_scale+.0000000001, # Add epsilon to avoid log(0)
     norm=LogNorm(),
     #vmin=-1, vmax=1, center=0,
     cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
     #cmap=sns.color_palette("coolwarm", n_colors=20),
     #cmap=sns.color_palette("RdBu", 100),
     square=False,
     xticklabels=False,
     yticklabels=False,
     ax=ax
 )
#ax.set_aspect(1.2)
ax.tick_params(axis='both', which='major', labelsize=18)
ax.figure.savefig(figuredir + 'iota_occ4_crosstab_heatmap_share.png', dpi=300, bbox_inches="tight")



############3
# Occ6 
crosstab_iota_occ6 = pd.crosstab(index = data_full.iota.loc[data_full.iota!=-1], columns = data_full.cbo2002_first.loc[data_full.cbo2002_first!=-1])

from matplotlib.colors import LogNorm, Normalize
fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
sns.heatmap(
     crosstab_iota_occ6+.000001, # Add epsilon to avoid log(0)
     norm=LogNorm(),
     #vmin=-1, vmax=1, center=0,
     cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
     #cmap=sns.color_palette("coolwarm", n_colors=20),
     #cmap=sns.color_palette("RdBu", 100),
     square=False,
     xticklabels=False,
     yticklabels=False,
     ax=ax
 )
#ax.set_aspect(1.2)
ax.tick_params(axis='both', which='major', labelsize=18)
ax.figure.savefig(figuredir + 'iota_occ6_crosstab_heatmap.png', dpi=300, bbox_inches="tight")

# Same thing but rescaling it so it's the share of the iota not the raw count
crosstab_iota_occ6_scale = crosstab_iota_occ6.div(crosstab_iota_occ6.sum(axis=1), axis=0)

fig, ax = plt.subplots(figsize=(5.76,4.8))         # Sample figsize in inches
sns.heatmap(
     crosstab_iota_occ6_scale+.0000000001, # Add epsilon to avoid log(0)
     norm=LogNorm(),
     #vmin=-1, vmax=1, center=0,
     cmap=sns.diverging_palette(255, 15, n=500, s=100, center='light'),
     #cmap=sns.color_palette("coolwarm", n_colors=20),
     #cmap=sns.color_palette("RdBu", 100),
     square=False,
     xticklabels=False,
     yticklabels=False,
     ax=ax
 )
#ax.set_aspect(1.2)
ax.tick_params(axis='both', which='major', labelsize=18)
ax.figure.savefig(figuredir + 'iota_occ6_crosstab_heatmap_share.png', dpi=300, bbox_inches="tight")


