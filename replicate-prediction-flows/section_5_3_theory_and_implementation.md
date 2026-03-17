# Section 5.3: Predicting Out-of-Sample Job-to-Job Flows - Theory and Code Explanation

## Theoretical Framework

### Overview
Section 5.3 of the paper tests the predictive power of different labor market definitions by evaluating their ability to predict future job-to-job transitions. The core idea is that a better market definition should capture the underlying structure of worker mobility patterns, allowing more accurate predictions of how workers will move between jobs in the future.

### The Prediction Method

Given a market definition **M**, the method works as follows:

1. **Compute Market-to-Market Transition Probabilities**:
   - Calculate empirical transition probabilities between markets: P_M[m'|m]
   - This tells us the probability that a worker in market m will move to market m'

2. **Predict Job-to-Job Transitions**:
   - For each job j in market m, compute the probability of transitioning to job j' in market m':
   ```
   Prob(j' ∈ m'|j ∈ m) := P_M[m'|m] × (d_j' / Σ_{k∈m'} d_k)
   ```
   - The first term is the market-level transition probability
   - The second term is job j's employment share within its destination market m'
   - This creates a predicted transitions matrix P̂_M[j'|j]

3. **Measure Prediction Error**:
   - Compare predicted transitions P̂_M[j'|j] to actual out-of-sample transitions P_oos[j'|j]
   - Use matrix norms to quantify prediction error:
   ```
   Ω_M := ||P̂_M[j'|j] - P_oos[j'|j]||
   ```

### Market Definitions Compared

The analysis compares three market definitions:

1. **Gamma Markets (γ)**: Network-based markets identified through the paper's clustering algorithm (1,371 markets)
2. **Iota-Gamma (ι)**: Worker type-based classification that uses the same transition structure as gamma markets but focuses on worker types (446 worker types)
3. **Occ2×Meso**: Traditional classification using 2-digit occupations × meso regions

### Key Results
- Network-based markets (γ) outperform traditional occupation-sector classifications
- Average L₁ prediction errors: 15.5 (markets) vs 15.6 (Occ2×Meso)
- Average L₂ prediction errors: 237.9 (markets) vs 317.7 (Occ2×Meso)
- The larger L₂ difference indicates traditional classifications generate more frequent large prediction errors

## Code Implementation

### File Structure

The analysis is implemented across several Python files:

1. **`parallel_jobtransitions_market_based_multiple_mkts.py`**: Main analysis script that runs predictions in parallel
2. **`parallel_jobtransitions_stack_results.py`**: Results aggregation and comparison
3. **`Code/Modules/prediction_error_uni.py`**: Core prediction function

### Data Inputs

The code loads several key data structures:

```python
# Adjacency matrices for market-to-market transitions (in-sample)
ag_ins    # gamma-to-gamma transitions (1155×1155)
ao_ins    # occ2Xmeso-to-occ2Xmeso transitions (1508×1508)
d_gg_tilde # iota-gamma transitions (reuses gamma structure)

# Out-of-sample job-to-job adjacency matrix
ajid_oos  # actual job transitions to predict (3,456,611 × 3,456,611)

# Degree count information
cmkts     # market-level degree counts for each market type
cjid      # job-level information including market assignments and degrees
```

### Main Analysis Loop (`parallel_jobtransitions_market_based_multiple_mkts.py`)

The script processes jobs in parallel across multiple CPU cores:

```python
# Parallelization setup
core = int(sys.argv[1])          # Current core number
total_cores = int(sys.argv[2])   # Total number of cores
workload_size = J // total_cores # Jobs per core

# Main prediction loop
for j in range(first_job, last_job+1):
    for mkt in ['gamma', 'iotagamma', 'occ2Xmeso']:
        m = mkt[0]  # Get market abbreviation (g, i, o)
        result_temp[m] = prediction_error_uni(j, cjid, cmkts[m], ajid_oos,
                                             amkts[m], D_insample, D_outsample, J, mkt)
```

### Core Prediction Function (`prediction_error_uni.py`)

For each job j, the function:

1. **Identifies the job's market**: `g = cjid[mkt][j]`

2. **Computes predicted transition probabilities**:
   ```python
   # Main prediction formula
   cjid['pred_prob'] = (cjid['mm_count_temp'+m] / D_insample) * cjid['djd'+m] * cjid['djd'+m][j]
   ```
   Where:
   - `mm_count_temp` = market-to-market transition counts (d_mm' in theory)
   - `D_insample` = total in-sample transitions
   - `djd` = job degree divided by market size (job's share within market)

3. **Handles self-loop correction**:
   - The model predicts non-zero self-transitions (j→j) which don't exist in reality
   - These probabilities are redistributed to other jobs in the same market

4. **Computes prediction errors**:
   ```python
   # L1 and L2 errors
   error1 = np.sum(np.abs((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
   error2 = np.sum(np.square((pred.sort_index()*D_outsample).values - ajid.getrow(j)))
   ```

### Results Aggregation (`parallel_jobtransitions_stack_results.py`)

This script:

1. **Combines results** from all parallel cores:
   ```python
   for core in range(1, 26):
       df = pd.read_csv('job_transitions_results/results_core' + str(core) + '_multiple_mkts.csv')
       dfs.append(df)
   combined_df = pd.concat(dfs, ignore_index=True)
   ```

2. **Computes performance differences**:
   ```python
   # Compare gamma vs occ2Xmeso performance
   combined_df['diff_l1'] = combined_df['l1_g'] - combined_df['l1_o']
   combined_df['diff_l2'] = combined_df['l2_g'] - combined_df['l2_o']
   ```

3. **Performs robustness checks**:
   - Trims outliers (top/bottom 10, 100, 1000 observations)
   - Computes statistics on random 1% samples
   - Generates histograms of prediction error differences

### Key Implementation Details

1. **Parallelization**: The computation is distributed across multiple cores because evaluating predictions for 3.4M+ jobs is computationally intensive

2. **Memory Management**: Uses sparse matrices for the large job-to-job adjacency matrix to handle the massive dimensionality

3. **Self-Loop Correction**: A key technical challenge is that the theoretical model predicts positive self-transition probabilities (job j to itself), but these don't exist in the empirical data. The code redistributes this probability mass to other jobs within the same market.

4. **Market Assignment**: Each job is assigned to exactly one market in each classification system, and predictions are based on the transition patterns between these markets.

## Interpretation

The superior performance of network-based markets suggests they capture fundamental economic relationships that traditional occupation-sector classifications miss. When workers move between jobs, they tend to follow pathways that respect the network-based market structure more than conventional categorical boundaries.

This validation exercise provides empirical support for the paper's main methodological contribution: that labor markets should be defined based on actual mobility patterns rather than administrative or subjective job classifications.