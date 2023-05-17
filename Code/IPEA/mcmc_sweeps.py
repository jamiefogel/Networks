# LOADING PACKAGES
import graph_tool.all as gt
import os
import pickle
import datetime as dt
import numpy as np

os.chdir('/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/aug2022/code')
import bisbm
import functions

os.chdir('/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/aug2022/data')

modelname = '3states_2013to2016'

# LOADING THE MODEL ORIGINALLY FROM THE SBM (state is an object inside the model)
#model = pickle.load(open('model_' + modelname + '.p',  "rb" ) )
#state = model.state
#del model

# LOADING THE STATE ALREADY
state = pickle.load(open('state_mcmc_iters.p', 'rb'))[0]

gt.seed_rng(734)

entropy = []
entropy.append(state.entropy())

t0 = dt.datetime.now()

for i in range(1000): # this should be sufficiently large
    print(i)
    state.multiflip_mcmc_sweep(beta=np.inf, niter=5)
    entropy.append(state.entropy())
    pickle.dump(entropy, open('entropy.p', "wb" ) )
    print("Improvement: ", round(((entropy[0] - entropy[-1])/entropy[0])*100,4))
    print("Time spent: ", dt.datetime.now()-t0)
    pickle.dump([state,i,entropy], open('tmp_state_mcmc_iters.p', "wb" ), protocol = 4)
    os.rename('tmp_state_mcmc_iters.p','state_mcmc_iters.p')

print("printing this just to avoid indentation errors")

#os.chdir('/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/aug2022/data')
#import os
#import pickle
#entropy = pickle.load(open('entropy.p',  "rb" ) )
#round(((entropy[0] - entropy[-1])/entropy[0])*100,2)

