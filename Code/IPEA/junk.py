
nrows= 100
for y in range(2000,2018):
    print(y)
    if y<1998:
        sep=';'
    else:
        sep=','
    raw_data = pd.read_csv('~/rais/RAIS/csv/brasil' + str(y) +'.csv', sep=sep, nrows=nrows)
    #raw_data.pis.unique().shape
    #raw_data.cpf.unique().shape
    #raw_data.ind_pis_val.sum()
    #raw_data.ind_cpf_val.sum()
    #print(raw_data[['pis','ind_pis_val']])
    print(raw_data.columns)
    print()
