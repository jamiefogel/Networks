# The purpose of this function is to take the vertex property map returned when using gt.add_edge_list() with the option hashed=True and add it as an internal vertex property.
# - For some reason I have only gotten this to consistently work when I define the vertex property map by looping over vertices. I had issues with using get_2d_array([0]) when the ids are strings (e.g. for jids or occ2Xmesos)



def add_ids_as_veertex_property(graph, ids):
    id_prop = graph.new_vertex_property("string")
    graph.vp["ids"] = id_prop
    for g in graph.vertices():
        id_prop[g] = ids[g]
    return graph, id_prop

new, id_prop = add_ids_as_property(g_jid, vmap)
new.vp.ids.get_2d_array([0])
