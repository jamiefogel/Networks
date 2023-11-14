# -*- coding: utf-8 -*-
"""
Created on Fri Jul 21 10:38:25 2023

@author: p13861161
"""

import pandas as pd
import graph_tool.all as gt


def create_unipartite_adjacency_and_degrees(mkt, df_trans):
    # Compute the adjacency matrix
    g = gt.Graph(directed=False)
    g_vertices = g.add_edge_list(df_trans[[mkt+'_prev',mkt]].values, hashed=True)
    adjacency = gt.adjacency(g)
    # Compute the total degrees associated  with each mkt. Note that the number of degrees is 2x the number of edges b/c it counts both in- and out-degrees
    mkt_id = g.new_vertex_property("string")
    g.vp[mkt] = mkt_id
    for v in g.vertices():
        mkt_id[v] = g_vertices[v]
    mkt_degreecount = pd.DataFrame({mkt:g.vp[mkt].get_2d_array([0]).ravel(),mkt+'_degreecount':g.degree_property_map('total').a}).reset_index()
    # Gammas will have been converted to objects, not floats. Convert it back. I am leaving it flexible rather than hard-coding gamma in case we want to deal with other numeric mkt indices in the future, e.g. occ or ind.
    try:
        mkt_degreecount[mkt] = pd.to_numeric(mkt_degreecount[mkt])
        print(f"Column '{mkt}' succesfully converted to float")
    except:
        print(f"Column '{mkt}' not converted to float")
    return[adjacency,mkt_degreecount]   

    
