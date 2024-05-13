from datetime import datetime
import pandas as pd
import numpy as np
import os
import sys
import matplotlib.pyplot as plt
import geobr
import pyproj # for converting geographic degree measures of lat and lon to the UTM system (i.e. in meters)
from scipy.integrate import simps # to calculate the AUC for the decay function
import binsreg
import statsmodels.api as sm
