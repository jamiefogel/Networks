#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 07:45:49 2023

@author: jfogel

The goal here is to figure out how to recover P_\i\g (the block-level edge propensity matrix) after estimating the SBM. I am using the sample data set that the graph-tool docs use to estimate the nested SBM since it's way smaller and thus easier to use than our model. 

See email from Bernardo at 12:53am on 2023/06/27: https://mail.google.com/mail/u/2/#inbox/QgrcJHsbhNcSkrbbwbdnPMXxNpcRRlQXZzG
"""


import graph_tool.all as gt
import numpy as np
g = gt.collection.data["power"]
state = gt.minimize_nested_blockmodel_dl(g)
#state.draw(output="power_nested_mdl.pdf")
state.draw()

state.get_levels()[0].get_bg()
state.get_levels()[0].get_ers().a.shape


bg = gt.adjacency(state.get_levels()[0].get_bg()).toarray()
np.savetxt('./Data/derived/dump/bg.csv', bg, delimiter=',')       


# There are 27 non-empty blocks. This is reflected in the blockgraph by 27 non-empty rows and columns. 
(bg.sum(axis=0)>0).sum()
(bg.sum(axis=1)>0).sum()


# I think what I need to do is just extract the non-empty rows and columns of the block graph. This should be a (I+G)x(I+G) matrix, but I need to ensure that the row and column orders are correct. 
nonzero_rows = np.nonzero(np.any(bg, axis=1))[0]
nonzero_columns = np.nonzero(np.any(bg, axis=0))[0]
result = bg[nonzero_rows][:, nonzero_columns]
np.set_printoptions(linewidth=np.inf)
np.set_printoptions(suppress=True)

# Print the resulting array
print(result)


a_big = state.get_levels()[0].get_matrix().toarray()
nonzero_rows = np.nonzero(np.any(a_big, axis=1))[0]
nonzero_columns = np.nonzero(np.any(a_big, axis=0))[0]
a = a_big[nonzero_rows][:, nonzero_columns]
print(a)