# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 18:46:44 2023

@author: p13861161
"""
from graph_tool.all import *

g = Graph()

v1 = g.add_vertex()
v2 = g.add_vertex()

e = g.add_edge(v1, v2)

graph_draw(g, vertex_text=g.vertex_index)

graph_draw(g, vertex_text=g.vertex_index, output="two-nodes.pdf")