# Generate the network entirely using fake data

# Load packages
import os
import graph_tool.all as gt
import numpy
import matplotlib
import csv
import datetime
import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd 
import pickle
import sys

os.chdir('/Users/jfogel/Dropbox (University of Michigan)/Networks/Code/aug2021/')

# Defined in ~/labormkt/MayDraft/code/functions/
sys.path.append('/Users/jfogel/Dropbox (University of Michigan)/Networks/Code/aug2021')
import bisbm as bisbm
# n - Number of nodes
n_w = 60
n_j = 40 #20


homedir = os.path.expanduser('~')
root = homedir + '/NetworksGit/'
figuredir = root + 'Results/'

print(homedir)
print(root)
print(figuredir)

#P_ig = np.array([[.8, .15, .05],[.2,.3,.5]])
#P_ig = np.array([[.8, .15, .05],[.1,.4,.5]])
P_ig = np.array([[.5, .4, .03, .07],[.08,.02,.4, .5]])
I, G = np.shape(P_ig) 
probs = np.concatenate( (np.concatenate((np.zeros((G,G)), np.transpose(P_ig)), axis=1),np.concatenate((P_ig,np.zeros((I,I))), axis=1)), axis=0) * n_w * 50


# g - Number of groups
g = I + G

#group_membership_j_temp = numpy.random.randint(0, high=G, size=n_j)
#group_membership_j = np.ones(len(group_membership_j_temp))
#g_counts = pd.DataFrame({'count':pd.DataFrame(group_membership_j_temp).value_counts()}).reset_index().reset_index()
#B = g_counts.shape[0]
#for b in range(B):
#    recode_val = g_counts.iloc[b][0]
#    group_membership_j[group_membership_j_temp==recode_val] = b
#
#del group_membership_j_temp

group_membership_j = numpy.random.randint(0, high=G, size=n_j)
group_membership_w = numpy.random.randint(G, high=I+G, size=n_w)
group_membership = np.append(group_membership_j,group_membership_w)
indegrees_j = numpy.random.poisson(lam=9, size=n_j)+1
indegrees_w = numpy.random.poisson(lam=3,  size=n_w)+1
indegrees = np.append(indegrees_j,indegrees_w)

A = gt.generate_sbm(group_membership, probs, in_degs=indegrees, directed=False)

 
edgelist = pd.DataFrame({'jid':A.get_edges()[:,0],'wid':A.get_edges()[:,1]})
edgelist.to_pickle('./dump/temp.p')

model_fake = bisbm.bisbm()
model_fake.create_graph(filename='./dump/temp.p',min_workers_per_job=1)

# I should edit bisbm so that it takes the worker and job ID variables as arguments rather than assuming they are called wid and jid
model_fake.fit(n_init=1)
print(model_fake.state)

levels = len(model_fake.state.levels)



#pickle.dump([model_fake,A], open('./dump/intro_figs.p', 'wb'))
pickle.load(open('./dump/intro_figs.p', 'rb'))


colormap_maizeblue =  {0:(1,.8,.02,1),1:(0,.15,.3,1)}
#0 / 39 / 76  255 / 203 / 5
plot_color1 = model_fake.g.new_vertex_property('vector<double>')
for v in model_fake.g.vertices():
    plot_color1[v] = colormap_maizeblue[model_fake.g.vp['kind'][v]]

beta = 0.5
blackwhite_colormap  =  {0:(1,1,1,1,.25),1:(1,0,0,0,.9),2:(1,1,.8,.02,.9)}
edge_color = model_fake.g.new_ep('vector<double>')
edge_color_white = model_fake.g.new_ep('vector<double>')
edge_color_black = model_fake.g.new_ep('vector<double>')
# Set all edge colors to white
for e in model_fake.g.edges():
    edge_color_white[e] = blackwhite_colormap[0]
    edge_color_black[e] = blackwhite_colormap[1]



alpha=0.75
#vertex_colormap = {0:(255/255.0,0/255.0,0/255.0,alpha),	1:(0/255.0,0/255.0,255/255.0,alpha),	2:(1,.8,.02,alpha),	3:(0/255.0,0/255.0,0/255.0,alpha),	4:(255/255.0,0/255.0,255/255.0,alpha)}
vertex_colormap = {0:(255/255.0,0/255.0,0/255.0,alpha),	1:(0/255.0,0/255.0,255/255.0,alpha),	2:(1,.8,.02,alpha),	3:(0/255.0,0/255.0,0/255.0,alpha),	4:(255/255.0,0/255.0,255/255.0,alpha),	5:(128/255.0,128/255.0,128/255.0,alpha)}


#vertex_shapemap = {0:"triangle", 1:"square", 2:"circle", 3:"double_circle", 4:"double_triangle"}
vertex_shapemap = {0:"triangle", 1:"square", 2:"circle", 3:"double_circle", 4:"double_triangle", 5:"double_square"}


blocks = model_fake.g.new_vertex_property('vector<double>')
blocks = model_fake.state.project_level(0).get_blocks().a
pd.DataFrame(blocks).value_counts()
pd.DataFrame(group_membership).value_counts()

# Recode blocks so that they go from 0 to B-1
blocks_recode = pd.DataFrame(blocks)
counts = pd.DataFrame({'count':pd.DataFrame(blocks).value_counts()}).reset_index().reset_index()
B = counts.shape[0]
for b in range(B):
    recode_val = counts.iloc[b][0]
    blocks_recode.loc[blocks_recode[0]==recode_val] = b

blocks_recode = blocks_recode[0].to_numpy()
    
#blocks_recode = pd.DataFrame(blocks).replace({56:5,42:4,35:0,78:1,55:2,69:3})[0].to_numpy()
#blocks_recode = pd.DataFrame(blocks).replace({69:4,25:3,31:0,52:2,8:1})[0].to_numpy()
vertex_color =  model_fake.g.new_vertex_property('vector<double>')
vertex_shape =  model_fake.g.new_vertex_property('string')
for v in model_fake.g.vertices():
    vertex_color[v] = vertex_colormap[blocks_recode[int(v)]]
    vertex_shape[v] = vertex_shapemap[blocks_recode[int(v)]]



filename = figuredir + 'intro_figs_part3_1.png'
model_fake.state.draw(layout='bipartite', output=filename,subsample_edges=200, hshortcuts=levels-2, hide=levels, vertex_color=plot_color1, vertex_fill_color=plot_color1, vertex_size=10, output_size=(1690,1000), bip_aspect=1.69, vertex_shape = vertex_shape, beta=.8 )


im = plt.imread(filename)
f = plt.figure(figsize = ((im.shape[1]+100)/300, im.shape[0]/300)) #figure with correct aspect ratio
ax = plt.axes((0,0,1,1)) #axes over whole figure
ax.imshow(im)
ax.text(0, 0.5, "Jobs (j)",  horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, rotation=90, color=(1, .8, .02), fontsize='x-large' ) #whatever text arguments
ax.text(1, 0.5, "Workers (i)",  horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, rotation=270, color=(0, .15, .3), fontsize='x-large' ) #whatever text arguments
ax.axis('off')
f.savefig(filename,dpi=300)

    
filename = figuredir + 'intro_figs_part3_2.png'
model_fake.state.draw(layout='bipartite', output=filename,subsample_edges=150, hshortcuts=levels-2, hide=levels, vertex_color=vertex_color, vertex_fill_color=vertex_color, vertex_size=10, output_size=(1690,1000), bip_aspect=1.69, vertex_shape = vertex_shape, rel_order=blocks )


im = plt.imread(filename)
f = plt.figure(figsize = ((im.shape[1]+100)/300, im.shape[0]/300)) #figure with correct aspect ratio
ax = plt.axes((0,0,1,1)) #axes over whole figure
ax.imshow(im)
ax.text(0, 0.5, "Jobs (j)",  horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, rotation=90, color=(0, .15, .3), fontsize='x-large' ) #whatever text arguments
ax.text(1, 0.5, "Workers (i)",  horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, rotation=270, color=(0, .15, .3), fontsize='x-large' ) #whatever text arguments
ax.axis('off')
f.savefig(filename,dpi=300)










filename = figuredir + 'intro_figs_part3_3.png'
model_fake.state.draw(layout='bipartite', output=filename,subsample_edges=100, hshortcuts=levels-2, hide=levels, vertex_color=vertex_color, vertex_fill_color=vertex_color, vertex_size=10, edge_gradient=blackwhite_colormap[0], output_size=(1690,1000), bip_aspect=1.69, vertex_shape = vertex_shape )


im = plt.imread(filename)
f = plt.figure(figsize = ((im.shape[1]+100)/300, im.shape[0]/300)) #figure with correct aspect ratio
ax = plt.axes((0,0,1,1)) #axes over whole figure
ax.imshow(im)
ax.text(0, 0.5, "Jobs (j)",  horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, rotation=90, color=(0, .15, .3), fontsize='x-large' ) #whatever text arguments
ax.text(1, 0.5, "Workers (i)",  horizontalalignment='center', verticalalignment='center', transform=ax.transAxes, rotation=270, color=(0, .15, .3), fontsize='x-large' ) #whatever text arguments
ax.axis('off')
f.savefig(filename,dpi=300)
    
