
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import graph_tool.all as gt
import scipy.sparse as sp
import copy

homedir = os.path.expanduser('~')
os.chdir(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/code/')


import bisbm
from pull_one_year import pull_one_year


#g = gt.lattice([5,5])
bip_edgelist = [(0, 5), (1, 5), (2,6), (2, 5), (3,6), (3,7), (4,7)]
g = gt.Graph(directed=False)
ids = g.add_edge_list(bip_edgelist)
is_biparitite, part = gt.is_bipartite(g, partition=True)
gt.graph_draw(g, vertex_fill_color=part, vertex_text=g.vertex_index, output='lattice.pdf', vorder=part)

from itertools import combinations

g_temp = g.copy()  # this is a deepcopy

for v, bipartite_label in enumerate(part):
    if bipartite_label == 0:
        neighbours = list(g.vertex(v).all_neighbours())
        for s, t in combinations(neighbours, 2):
            g_temp.add_edge(s, t)

g_projected = gt.Graph(gt.GraphView(g_temp, vfilt=part.a==1), prune=True)

gt.graph_draw(g_projected, vertex_text=g_projected.vertex_index, output='lattice_projected.pdf')
