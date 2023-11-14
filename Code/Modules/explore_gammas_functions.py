
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 15:55:54 2023

@author: jsf656
"""
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import sys
import gc
import matplotlib.pyplot as plt
import scipy.stats as stats
import matplotlib.colors as colors
from haversine import haversine, Unit
from shapely.geometry import MultiPoint
from scipy.stats import mstats


# Compute HHIs of other variables within each gamma
def gamma_hhi(data, gamma,var):
    hhis = {}
    gv_counts = data.groupby([gamma,var])[gamma].count().reset_index(name='gv_count')
    g_counts  = data.groupby([gamma])[gamma].count().reset_index(name='g_count')
    gv_counts = gv_counts.merge(g_counts, on=gamma, validate='m:1')
    gv_counts['gv_share_sq'] = (gv_counts.gv_count/gv_counts.g_count).pow(2)
    hhi = gv_counts.groupby(gamma)['gv_share_sq'].sum().reset_index(name='hhi_'+var)
    return hhi



def corr_plots(var1,var2):
    # Create the scatter plot
    corr = np.corrcoef(var1, var2)[0][1]
    print('Correlation: ', round(corr,3))
    fig, ax = plt.subplots()
    ax.scatter(var1, var2, s=5)
    ax.annotate("Correlation = {:.2f}".format(corr), xy=(0.05, 0.95), xycoords='axes fraction')
    ax.set_xlabel(var1.name)            
    ax.set_ylabel(var2.name)
    plt.savefig('./Results/hhi_scatterplot_' + var1.name + '_' + var2.name +' .pdf', format='pdf')
    plt.close()


def compute_distance(row):
    coords_1 = (row['lat'], row['lon'])
    coords_2 = (row['mean_lat'], row['mean_lon'])
    return haversine(coords_1, coords_2)



def plot_mesos(gamma, gammas_w_attributes, meso_share_df, meso_share_norm_df, gamma_dict):
    plt.rcParams.update({"font.size": 7})
    # Plot the raw shares
    fig1, ax1 = plt.subplots(figsize=(4, 4), dpi=200)
    meso_share_df.plot(
        column=gamma,
        cmap="cividis",
        legend=True,
        legend_kwds={
            "label": "Fraction of Gamma "+str(gamma)+" jobs in each meso region",
            "orientation": "horizontal",
            "shrink": 0.6,
        },
        ax=ax1,
    )
    # Plot the normalized shares
    meso_share_norm_df_winsor = meso_share_norm_df[['code_meso','name_meso','geometry']]
    meso_share_norm_df_winsor[gamma] = meso_share_norm_df[gamma].clip(lower=0.1, upper=10.0)
    fig2, ax2 = plt.subplots(figsize=(4, 4), dpi=200)
    meso_share_norm_df_winsor.plot(
        column=gamma,
        cmap="cividis",
        norm=colors.LogNorm(vmin=0.1, vmax=10),
        legend=True,
        legend_kwds={
            "label": "Fraction of Gamma "+str(gamma)+" jobs in each meso region \n divided by meso population share.\n Large and small shares recoded to 10 and 0.1, respectively.",
            "orientation": "horizontal",
            "shrink": 0.6,
        },
        ax=ax2,
    )
    # Combine the three figures into a single plot
    fig, axs = plt.subplots(1, 3, figsize=(8, 4))
    fig.suptitle('Gamma = '+str(gamma),fontsize=12)
    # Add the first figure to the left side of the plot
    axs[0].axis('off')
    axs[0].imshow(fig1.canvas.renderer.buffer_rgba())
    # Add the second figure to the middle of the plot
    axs[1].axis('off')
    axs[1].imshow(fig2.canvas.renderer.buffer_rgba())
    # Add the third figure to the right side of the plot
    occ_df = gamma_dict[gamma][['description','share']].head(10)
    occ_df['share'] = occ_df.share.round(3)
    occ_df.rename(columns={'description':'Occupation','share':'Share'},inplace=True)
    axs[2].axis('off')
    table = axs[2].table(cellText=occ_df.values, colLabels=occ_df.columns, loc='center', colWidths=[.8,.15])
    table.auto_set_font_size(False)
    table.set_fontsize(7)
    axs[0].set_title('Spatial Distribution', fontsize=10)
    axs[1].set_title('Spatial Distribution (Normalized)', fontsize=10)
    axs[2].set_title('10 Most-Frequent Occupations', fontsize=10)
    # remove the padding between subplots
    fig.tight_layout(pad=0)
    # Print gamma stats at the bottom
    [educ,earn,dist,var,educ_rank,earn_rank,dist_rank,var_rank] = gammas_w_attributes.loc[gammas_w_attributes.gamma==gamma][['educ_mean','mean_monthly_earnings','j2j_dist_mean','spatial_var_km','educ_mean_rank','mean_monthly_earnings_rank','j2j_dist_mean_rank','spatial_var_km_rank']].values.tolist()[0]
    textstr1 = 'Mean education: {:.2f};                   Rank: {:.2f} \nMean monthly earnings: {:.2f};     Rank: {:.2f} \nMean move distance (km): {:.2f};     Rank: {:.2f}\nSpatial variance (km): {:.2f};          Rank: {:.2f}'.format(educ,educ_rank,earn,earn_rank,dist,dist_rank,var,var_rank)
    fig.text(0.05, 0.08, textstr1, ha='left', va='center')
    [hhi_jid,num_unique_jids,num_unique_wids,num_unique_wid_jids] = gammas_w_attributes.loc[gammas_w_attributes.gamma==gamma][['hhi_jid','num_unique_jids','num_unique_wids','num_unique_wid_jids']].values.tolist()[0]
    textstr2 = 'HHI of jids within gamma: {:.3f} \nNumber of Unique Jobs: {:.0f} \nNumber of Unique Workers: {:.0f}\nNumber of Unique Worker-Job Pairs: {:.0f}'.format(hhi_jid,num_unique_jids,num_unique_wids,num_unique_wid_jids)
    fig.text(0.7, 0.08, textstr2, ha='left', va='center')
    plt.axis('off')
    plt.savefig("./Results/meso_maps/map_mesos_gamma_"+str(gamma)+".pdf")
    plt.savefig("./Results/meso_maps/map_mesos_gamma_"+str(gamma)+".png")


 
