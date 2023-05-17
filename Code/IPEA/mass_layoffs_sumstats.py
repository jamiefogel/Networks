



jid_panel.loc[(jid_panel.layoffs>500)&(jid_panel.leave_share>.3)&(jid_panel.net_emp_change<-500)].gamma.value_counts().value_counts()
1    28
2     7
7     2
3     2
8     1
5     1
4     1
Name: gamma, dtype: int64
>>> jid_panel.loc[(jid_panel.layoffs>500)&(jid_panel.leave_share>.3)].gamma.value_counts().value_counts()
1    29
2     7
7     2
3     2
8     1
5     1
4     1
Name: gamma, dtype: int64
>>> jid_panel.loc[(jid_panel.layoffs>200)&(jid_panel.leave_share>.3)&(jid_panel.net_emp_change<-200)].gamma.value_counts().value_counts()
1     114
2      35
3      12
4       6
8       3
16      2
7       2
6       2
5       2
15      1
12      1
11      1
Name: gamma, dtype: int64
>>> jid_panel.loc[(jid_panel.layoffs>200)&(jid_panel.leave_share>.3)].gamma.value_counts().value_counts()
1     119
2      35
3      14
4       5
8       4
5       3
6       2
18      1
16      1
15      1
12      1
11      1
7       1
Name: gamma, dtype: int64
>>> 



jid_panel.groupby('gamma')['mass_layoff_flag'].sum().value_counts()
