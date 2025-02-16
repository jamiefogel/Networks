# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 16:51:54 2024

@author: p13861161
"""

import pandas as pd
import geobr
from tqdm import tqdm # to calculate progress of some operations for geolocation
import pyproj # for converting geographic degree measures of lat and lon to the UTM system (i.e. in meters)

transformer = pyproj.Transformer.from_crs("epsg:4326", "epsg:32723", always_xy=True)
root = "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/"

#modelname = '3states_2009to2011_mcmc'
modelname = '3states_2013to2016_mcmc'



# Pull meso codes for our states of interest. Choosing 2014 b/c this isn't available for all years and 2014 is in the middle of our sample
year_code_munic = 2014

muni_sp = geobr.read_municipality(code_muni="SP", year=year_code_munic)
muni_rj = geobr.read_municipality(code_muni='RJ', year=year_code_munic)
muni_mg = geobr.read_municipality(code_muni='MG', year=year_code_munic)
munis = pd.concat([muni_sp, muni_rj, muni_mg], ignore_index=True)
munis['lon_munic'] = munis.geometry.centroid.x
munis['lat_munic'] = munis.geometry.centroid.y
munis['codemun'] = munis.code_muni//10

# Converting coordinates to UTM, so the units are in meters
# Function to convert geographic coordinates to UTM using a fixed zone 23S for Sao Paulo
# Create the transformer object for UTM zone 23S
transformer = pyproj.Transformer.from_crs("epsg:4326", "epsg:32723", always_xy=True)
# Function to convert geographic coordinates to UTM
def convert_to_utm(lon, lat):
    return transformer.transform(lon, lat)

# Initialize lists to store the UTM coordinates
utm_lon = []
utm_lat = []

# Convert latitude and longitude to UTM with progress indicator
for lon, lat in tqdm(zip(munis['lon_munic'].values, munis['lat_munic'].values), total=len(munis)):
    utm_x, utm_y = convert_to_utm(lon, lat)
    utm_lon.append(utm_x)
    utm_lat.append(utm_y)

# Assign the UTM coordinates back to the DataFrame
munis['utm_lon_munic'] = utm_lon
munis['utm_lat_munic'] = utm_lat

# geobr not installed on Stata server so I added this to allow me to run it on linux and then load it elsewhere. Also convert from geopandas to regular pandas df
munis = pd.DataFrame(munis)
munis.to_file(root + f'/Data/derived/munis_{modelname}.geojson', driver='GeoJSON')
