import graph_tool.all as gt
import matplotlib.pyplot as plt

balanced = pd.read_pickle('../data/panel_'+modelname+'.p')
model = pickle.load( open('../data/model_'+modelname+'.p', "rb" ) )


print(model.num_workers, 'unique workers,',model.num_jobs, 'unique jobs, and', model.num_edges, 'edges in graph after restricting to at least 5 workers per job' )


df_raw = balanced.loc[(balanced.year>=2009) & (balanced.year<=2012)][['wid','wid_masked','jid','jid_masked','cbo2002','gamma_level_0','sector_IBGE','clas_cnae20','grau_instr','cnpj_raiz','id_estab']].rename(columns={'gamma_level_0':'gamma','cnpj_raiz':'id_firm'})

df = df_raw 
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
output_df.to_latex('~/labormkt/labormkt_rafaelpereira/april2021/export/transitions_table.tex', index=False)

# Now split the table by firm/estab changes or not)

degrees = model.g.get_out_degrees(model.g.get_vertices())
worker_degs = degrees[model.g.vp.kind.a==1]
job_degs    = degrees[model.g.vp.kind.a==0]

pickle.dump([worker_degs,job_degs], open('../export/degree_distribution.p', "wb"))


workers = model.edgelist_w_blocks[['wid','worker_blocks_level_0']].drop_duplicates()
iota_sizes = workers.groupby(['worker_blocks_level_0']).size()


jobs = model.edgelist_w_blocks[['jid','job_blocks_level_0']].drop_duplicates()
gamma_sizes = jobs.groupby(['job_blocks_level_0']).size()

pickle.dump([iota_sizes,gamma_sizes], open('../export/iota_gamma_sizes.p', "wb"))


# Confirming that all workers and jobs are assigned an iota/gamma
model.edgelist_w_blocks.worker_blocks_level_0.isnull().sum()
model.edgelist_w_blocks.job_blocks_level_0.isnull().sum()   


# Not all vertices belong to the giant component but 99% do
gt.extract_largest_component(model.g)
model.g
print(4814812/4868046)

# A better way of doing the same thing as above
g_comp = gt.label_largest_component(model.g)
g_comp.a.mean()



temp = pd.DataFrame({'block':model.state.project_level(0).get_blocks().a,'g_comp':gt.label_largest_component(model.g),'worker_node':model.g.vp.kind.a})

# It seems like nodes not in the giant component are kind of just randomly assigned but it doesn't matter since 99% of nodes are in the giant component
temp.loc[temp.g_comp==0].block.value_counts()
