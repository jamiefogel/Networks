# Complete Data Pipeline: From Raw RAIS Data to Section 5.3 Job Flow Predictions

## Pipeline Overview

This document traces the complete data processing pipeline from raw Brazilian administrative labor data (RAIS) to the final Section 5.3 analysis that predicts out-of-sample job-to-job transitions.

## Stage 1: Raw Data Processing

### **File: `Code/do_all.py`** (Master orchestration script)

**Purpose**: Main orchestration script that coordinates the entire analysis pipeline

**Key Parameters**:
- `modelname = '3states_2009to2012'` - Defines the dataset identifier
- `firstyear_sbm = 2009, lastyear_sbm = 2012` - SBM estimation period
- `firstyear_panel = 2009, lastyear_panel = 2014` - Full panel period
- `state_codes = [31, 33, 35]` - Minas Gerais, Espírito Santo, Rio de Janeiro
- `rais_filename_stub = '~/rais/RAIS/csv/brasil'` - Raw RAIS file location

**Raw Data Source**:
- Brazilian annual employment records (RAIS): `~/rais/RAIS/csv/brasil[YEAR].csv`
- Contains universe of formal employment relationships in Brazil
- Key variables: worker ID (PIS), establishment ID, occupation (CBO), municipality, wages, hours

### **File: `Code/Modules/pull_one_year.py`**

**Purpose**: Loads and preprocesses one year of RAIS data

**Input**: Raw RAIS CSV files (e.g., `brasil2009.csv`, `brasil2010.csv`, etc.)

**Processing Steps**:
1. **Geographic Filtering**: Restricts to specified states (`state_codes=[31,33,35]`)
2. **Age Filtering**: Workers aged 25-55 only
3. **Job Definition**: Creates `jid = establishment_id + "_" + 4digit_occupation`
4. **Data Cleaning**:
   - Drops military occupations (CBO starting with '0')
   - Removes certain employment relationship types (30,31,35)
   - Requires non-missing worker ID, establishment ID, occupation
5. **Variable Creation**:
   - `wid` (worker ID) = PIS number
   - `jid` (job ID) = establishment + 4-digit occupation
   - `yob` (year of birth) = year - age
   - Industry classification from CNAE codes

**Output**: `./Data/derived/raw_data_sbm_[modelname]_[year].p` (one file per year)

### **File: Data Appending (in `do_all.py`)**

**Purpose**: Combines multiple years of raw data

**Processing**:
- Loads individual year files and concatenates
- Creates multi-year worker-job panel

**Output**: `./Data/derived/appended_sbm_[modelname].p`

## Stage 2: Network Construction & SBM Estimation

### **File: `Code/Modules/bisbm.py`** (Bipartite Stochastic Block Model)

**Purpose**: Implements bipartite stochastic block model for worker-job network

**Key Methods**:

1. **`create_graph()`**:
   - Loads bipartite edgelist of worker-job matches
   - Applies minimum workers per job filter (`min_workers_per_job=5`)
   - Creates graph-tool bipartite graph with vertex kinds (0=job, 1=worker)
   - Assigns sequential IDs: jobs 0 to J-1, workers J to J+W-1

2. **`fit()`**:
   - Runs nested stochastic block model using graph-tool
   - Uses degree-corrected model with bipartite constraint
   - Iterates over multiple random initializations to avoid local minima
   - Minimizes description length (MDL) criterion

3. **`export_blocks()`**:
   - Exports job block assignments to CSV
   - Exports worker block assignments to CSV
   - Creates hierarchical clustering (multiple levels)

### **SBM Estimation Process (in `do_all.py`)**:

```python
# Create bipartite edgelist
bipartite_edgelist = appended[(year>=2009) & (year<=2012)][['wid','jid']].drop_duplicates()

# Fit SBM
model = bisbm.bisbm()
model.create_graph(filename='bipartite_edgelist_file.p', min_workers_per_job=5)
model.fit(n_init=1)
model.export_blocks(joutput='model_jblocks.csv', woutput='model_wblocks.csv')
model.mcmc_sweeps(numiter=1000)  # MCMC refinement
```

**Outputs**:
- `./Data/derived/sbm_output/model_[modelname].p` - Fitted SBM object
- `./Data/derived/sbm_output/model_[modelname]_jblocks.csv` - Job block assignments (γ markets)
- `./Data/derived/sbm_output/model_[modelname]_wblocks.csv` - Worker block assignments (ι types)

**Key Results**:
- ~1,371 job markets (γ)
- ~446 worker types (ι)

## Stage 3: Earnings Panel Creation

### **File: `Code/Modules/create_earnings_panel.py`**

**Purpose**: Creates balanced earnings panel with SBM classifications

**Processing Steps**:

1. **Earnings Calculation**:
   - Converts to real wages using CPI deflator
   - Computes hourly wages: `real_hrly_wage_dec = (monthly_wage / (hours * 4.4)) / (cpi/100)`
   - Takes log of real hourly wages

2. **Sample Restrictions**:
   - December employment only (jobs lasting full month)
   - Monthly salary workers only (`tipo_salario==1`)
   - Positive earnings
   - Non-military occupations
   - Primary job per worker-year (highest hours, then wages)

3. **Balanced Panel Creation**:
   - Creates "spine" of all worker-year combinations
   - Age restrictions: 25+ at panel start, <55 at panel end
   - Merges employment data onto spine (missing = non-employment)

4. **SBM Block Assignment**:
   - Merges γ (job market) assignments from `model_[modelname]_jblocks.csv`
   - Merges ι (worker type) assignments from `model_[modelname]_wblocks.csv`
   - Handles missing values: non-employment = γ=0, unclassified = -1

5. **Traditional Classifications**:
   - Creates `occ2Xmeso` = 2-digit occupation × meso region
   - Various occupation and sector groupings for comparison

**Output**: `./Data/derived/earnings_panel/panel_[modelname]_level_0.csv`

## Stage 4: Prediction Data Preparation

### **File: `Code/predicting_flows_data_pull.py`**

**Purpose**: Prepares data specifically for the job transition prediction exercise

**Key Configuration**:
- **In-sample period**: 2009-2011 (for estimating transition probabilities)
- **Out-of-sample period**: 2012-2013 (for prediction validation)

**Processing Steps**:

1. **Raw Data Loading**:
   ```python
   ins_years = [2009, 2010, 2011]  # In-sample years
   oos_years = [2012, 2013]        # Out-of-sample years

   for year in ins_years + oos_years:
       raw = pull_one_year(year, state_codes=[31,33,35], age_lower=25, age_upper=55)
       # Merge SBM assignments
       raw = raw.merge(gammas, on='jid')  # Add γ market
       raw = raw.merge(iotas, on='wid')   # Add ι worker type
   ```

2. **Market Definition Creation**:
   - `gamma`: Network-based job markets from SBM
   - `occ2Xmeso`: 2-digit occupation × meso region (traditional classification)
   - Creates job-to-market crosswalks for each definition

### **File: `Code/Modules/create_df_trans.py`**

**Purpose**: Creates job-to-job transition dataframes

**Processing**:
```python
def create_df_trans(raw_dfs):
    df = pd.concat(raw_dfs)  # Stack multiple years
    df = df.sort_values(['wid', 'start_date'])

    # Create lagged variables (previous job info)
    df['jid_prev'] = df.groupby('wid')['jid'].shift(1)
    df['gamma_prev'] = df.groupby('wid')['gamma'].shift(1)
    df['occ2Xmeso_prev'] = df.groupby('wid')['occ2Xmeso'].shift(1)

    # Keep only actual job changes (jid != jid_prev)
    df_trans = df[(df['jid'] != df['jid_prev']) &
                  (all classifications non-missing)]

    return df_trans[['jid', 'jid_prev', 'wid', 'iota',
                     'gamma', 'gamma_prev', 'occ2Xmeso', 'occ2Xmeso_prev']]
```

**Outputs**:
- `pred_flows_df_trans_ins.p` - In-sample transitions (2009-2011)
- `pred_flows_df_trans_oos.p` - Out-of-sample transitions (2012-2013)

### **File: `Code/Modules/create_unipartite_adjacency_and_degrees.py`**

**Purpose**: Creates market-to-market adjacency matrices and degree counts

**Processing**:
```python
def create_unipartite_adjacency_and_degrees(mkt, df_trans):
    # Create undirected graph of market-to-market transitions
    g = gt.Graph(directed=False)
    g_vertices = g.add_edge_list(df_trans[[mkt+'_prev', mkt]].values, hashed=True)

    # Get adjacency matrix
    adjacency = gt.adjacency(g)

    # Compute degree counts per market
    mkt_degreecount = pd.DataFrame({
        mkt: g.vp[mkt].get_2d_array([0]).ravel(),
        mkt+'_degreecount': g.degree_property_map('total').a
    })

    return [adjacency, mkt_degreecount]
```

**Key Outputs**:
- `pred_flows_ag_ins.p` - γ-to-γ adjacency matrix (in-sample, 1155×1155)
- `pred_flows_ao_ins.p` - occ2Xmeso-to-occ2Xmeso adjacency matrix (in-sample, 1508×1508)
- `pred_flows_ajid_oos.p` - job-to-job adjacency matrix (out-of-sample, 3.4M×3.4M jobs)
- `pred_flows_gamma_degreecount_oos.p` - Market degree counts (out-of-sample)
- `pred_flows_occ2Xmeso_degreecount_oos.p` - Occ2×meso degree counts
- `pred_flows_jid_degreecount_oos.p` - Job-level degree counts with market assignments

4. **ι-γ Prediction Matrix**:
   - Uses MCMC-estimated bipartite SBM adjacency matrix
   - Computes `d_gg_tilde` = ι-mediated γ-to-γ transition probabilities
   - Theory: transitions between γ markets mediated by shared ι worker types

## Stage 5: Job Transition Prediction Analysis

### **File: `Code/parallel_jobtransitions_market_based_multiple_mkts.py`** (Main prediction script)

**Purpose**: Predicts job-to-job transitions for each of 3.4M+ jobs using different market definitions

**Parallelization**:
- Distributed across multiple CPU cores due to computational intensity
- Each core processes subset of jobs: `core_jobs = total_jobs // num_cores`

**Market Definitions Compared**:
1. **'gamma'** (g): Network-based markets from SBM clustering
2. **'iotagamma'** (i): Worker type-based, using ι transitions but γ structure
3. **'occ2Xmeso'** (o): Traditional 2-digit occupation × meso region

**Main Processing Loop**:
```python
# For each job j in assigned range
for j in range(first_job, last_job+1):
    for mkt in ['gamma', 'iotagamma', 'occ2Xmeso']:
        # Compute L1 and L2 prediction errors
        [error1, error2] = prediction_error_uni(j, cjid, cmkts[mkt], ajid_oos,
                                               amkts[mkt], D_insample, D_outsample, J, mkt)
    results.append([j, l1_g, l2_g, l1_i, l2_i, l1_o, l2_o])
```

**Output**: `job_transitions_results/results_core[X]_multiple_mkts.csv` (one per core)

### **File: `Code/Modules/prediction_error_uni.py`** (Core prediction algorithm)

**Purpose**: Implements the theoretical prediction method for a single job

**Prediction Algorithm** (for job j):

1. **Identify job's market**: `g = cjid[mkt][j]` (market containing job j)

2. **Extract market transition counts**:
   - Get row g from market-to-market adjacency matrix: `mm_count_temp[m] = amkt[g_index, :]`
   - This gives transitions from market g to all other markets

3. **Compute predicted transition probabilities**:
   ```python
   # Core prediction formula from theory
   cjid['pred_prob'] = (cjid['mm_count_temp'+m] / D_insample) *
                       cjid['djd'+m] * cjid['djd'+m][j]
   ```
   Where:
   - `mm_count_temp` = market-to-market transition counts (d_{mm'} in paper)
   - `D_insample` = total in-sample transitions (normalization)
   - `djd` = job degree / market size (job's employment share within market)

4. **Handle self-transitions**:
   - Model predicts j→j transitions (doesn't exist in real data)
   - Redistribute this probability mass to other jobs in same market

5. **Compute prediction errors**:
   ```python
   # Scale probabilities to match out-of-sample transition volume
   predicted_flows = pred_prob * D_outsample
   actual_flows = ajid_oos.getrow(j)  # Actual transitions from job j

   # L1 and L2 norms
   error1 = sum(|predicted - actual|)
   error2 = sum((predicted - actual)²)
   ```

**Mathematical Foundation**:
The prediction formula implements the theoretical model:
```
P(j'|j) = P(m'|m) × (d_j' / Σ_{k∈m'} d_k)
```
- First term: market-level transition probability
- Second term: job j's employment share within destination market m'

### **File: `Code/parallel_jobtransitions_stack_results.py`** (Results aggregation)

**Purpose**: Combines results from parallel processing and computes summary statistics

**Processing**:
1. **Combine parallel results**: Stack CSV files from all cores
2. **Performance comparison**:
   ```python
   # Compare network-based (gamma) vs traditional (occ2Xmeso)
   combined_df['diff_l1'] = combined_df['l1_g'] - combined_df['l1_o']
   combined_df['diff_l2'] = combined_df['l2_g'] - combined_df['l2_o']
   ```
3. **Robustness checks**:
   - Trim extreme outliers (top/bottom 10, 100, 1000 observations)
   - Random sampling (100 draws of 1% samples)
   - Generate distribution histograms

**Key Results**:
- Average L₁ errors: 15.5 (γ markets) vs 15.6 (occ2×meso)
- Average L₂ errors: 237.9 (γ markets) vs 317.7 (occ2×meso)
- Network-based markets achieve superior predictive performance

## Data Flow Summary

```
Raw RAIS Data (brasil[year].csv)
    ↓ [pull_one_year.py]
Individual Year Files (raw_data_sbm_[model]_[year].p)
    ↓ [do_all.py append section]
Appended Multi-Year Data (appended_sbm_[model].p)
    ↓ [bisbm.py: create_graph() → fit() → export_blocks()]
SBM Classifications (model_[model]_jblocks.csv, model_[model]_wblocks.csv)
    ↓ [create_earnings_panel.py]
Earnings Panel (panel_[model]_level_0.csv)
    ↓ [predicting_flows_data_pull.py]
Prediction Data:
  - Transition DataFrames (pred_flows_df_trans_[ins|oos].p)
  - Market Adjacency Matrices (pred_flows_a[g|o|jid]_[ins|oos].p)
  - Degree Count Data (pred_flows_[gamma|occ2Xmeso|jid]_degreecount_oos.p)
    ↓ [parallel_jobtransitions_market_based_multiple_mkts.py]
Individual Core Results (results_core[X]_multiple_mkts.csv)
    ↓ [parallel_jobtransitions_stack_results.py]
Final Analysis (combined_df with performance comparisons)
```

## Key Innovation

The pipeline demonstrates that **network-based labor market definitions** (derived from actual worker mobility patterns via SBM) **outperform traditional occupation-sector classifications** in predicting future job-to-job transitions. This validates the paper's core methodological contribution: labor markets should be defined based on revealed mobility patterns rather than administrative categories.

## Computational Challenges

1. **Scale**: 3.4+ million jobs requiring individual predictions
2. **Memory**: Sparse matrix representation for job-to-job adjacencies
3. **Parallelization**: Distributed processing across multiple CPU cores
4. **Self-loop correction**: Theoretical model predicts impossible self-transitions