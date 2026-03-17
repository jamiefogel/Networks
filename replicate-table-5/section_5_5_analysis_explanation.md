# Section 5.5: Predicting General Equilibrium Effects of Rio de Janeiro Olympics

## Overview
This document explains the analysis in Section 5.5 of Fogel and Modenesi (2021) that creates **Table 5: Predicted Effect of Olympics on Wages: Network-Based vs. Standard Classifications**. This section tests the model's ability to predict real-world labor market changes by comparing network-based worker/market classifications to standard occupation/sector classifications.

## Theoretical Background

### The Core Research Question
The paper asks: Can a model using network-based classifications of workers and jobs better predict actual wage changes from economic shocks than traditional occupation/sector-based approaches?

### The Olympics "Shock"
- **Shock Period**: 2016 Rio de Janeiro Olympics
- **Timeline**: Olympics announced late 2009, construction peaked around 2014
- **Pre-shock**: 2009 (baseline year)
- **Post-shock**: 2014 (when Olympic construction demand peaked)
- **Nature**: Infrastructure investment and construction for Olympic venues created demand shock for construction and related workers
- **Primary Affected Sector**: Construction (Sector 5) - "Construction" in the 15-sector classification
- **Geographic Focus**: Rio de Janeiro metropolitan area (State 20 in the data)

### IBGE Sector Classification (15 Sectors)
The analysis uses the following sector breakdown:
1. Agriculture, livestock, forestry, fisheries and aquaculture
2. Extractive industries
3. Manufacturing industries
4. Electricity and gas, water, sewage, waste management and decontamination
5. **Construction** ← Primary Olympics impact
6. Trade and repair of motor vehicles and motorcycles
7. Transport, storage and mail
8. **Accommodation and food** ← Secondary Olympics impact
9. Information and communication
10. Financial, insurance and related services
11. Real estate activities
12. Professional, scientific and technical, administrative and complementary services
13. Public administration, defense, education and health and social security
14. Private health and education
15. Arts, culture, sports and recreation and other service activities

### The Prediction Exercise
The analysis follows this regression framework:
```
Δy = β₀ + β₁ Δŷ + ε
```
Where:
- `Δy` = Actual change in mean log earnings by worker type (2014 - 2009)
- `Δŷ` = Model-predicted change in mean log earnings by worker type
- `β₁` = Slope coefficient (ideal value = 1.0 for perfect prediction)
- `β₀` = Intercept (ideal value = 0.0 for unbiased prediction)

### Classification Approaches Compared
**Table 5 compares 5 different worker/market classification combinations:**

| Column | Worker Classification | Market Classification | Description |
|--------|----------------------|----------------------|-------------|
| 1 | ι (iota) | γ (gamma) | **Network-based** (authors' method) |
| 2 | Occ4 | Sector | Standard 4-digit occupation × IBGE sector |
| 3 | Occ4 | γ (gamma) | Hybrid: 4-digit occupation × network markets |
| 4 | k-means | Sector | O*NET-based occupation clusters × IBGE sector |
| 5 | k-means | γ (gamma) | O*NET-based occupation clusters × network markets |

## Methodology

### Step 1: Calibrate Demand Shocks
- Calibrate demand shifter vectors `a⃗^2009` and `a⃗^2014` to match observed sector-level output
- These capture the Olympics-induced changes in demand for different sectors

### Step 2: Solve General Equilibrium Model
- Feed demand shifters through the structural model
- Solve for equilibrium wages and employment allocations for each year
- Generate model-predicted earnings `ŷᵢ^2009` and `ŷᵢ^2014` for each worker type ι
- Compute predicted changes: `Δŷ = ŷᵢ^2014 - ŷᵢ^2009`

### Step 3: Compute Actual Changes
- Calculate actual mean earnings by worker type for 2009 and 2014
- Compute actual changes: `Δy = yᵢ^2014 - yᵢ^2009`

### Step 4: Regression Analysis
- Regress actual changes on predicted changes: `Δy = β₀ + β₁ Δŷ + ε`
- Weight by number of workers per classification
- Estimate for each of the 5 classification approaches

## Code Implementation

### Main Files
1. **`Code/model_fit.py`** - Main script that generates Table 5
2. **`Code/Modules/model_fit_func.py`** - Core analysis function
3. **`Results/model_fit_se.tex`** - Generated LaTeX table
4. **`Code/Modules/solve_model_functions.py`** - General equilibrium solver

### Code Structure

#### 1. Main Script (`model_fit.py`)
```python
# Loop over different classification combinations
classification_list = [('iota','gamma'), ('occ4_first_recode','sector_IBGE'),
                      ('occ4_first_recode','sector_IBGE'), ('occ4_first_recode','gamma'),
                      ('kmeans','sector_IBGE'), ('kmeans','gamma')]

fit_returnvals = []
for idx in classification_list:
    wtype_var = idx[0]  # Worker classification
    jtype_var = idx[1]  # Market/job classification

    # Run analysis for this classification combination
    rvals = model_fit(fit_mle_data_filename, fit_mle_data_sums,
                     fit_mle_estimates, fit_psi_hat, S, eta, level,
                     fit_b_gs, wtype_var, jtype_var, a_ts, p_ts,
                     2009, 2014, figuredir)
    fit_returnvals.append(rvals)
```

#### 2. Core Analysis Function (`model_fit_func.py`)
The `model_fit()` function:

**a) Load and Process Data**
```python
# Read earnings panel data
data_full = pd.read_csv(mle_data_filename)

# Create worker_type and job_type variables based on classification choice
if worker_type_var=='1':
    data_full['worker_type'] = 1  # Use iota
else:
    data_full['worker_type'] = data_full[worker_type_var]  # Use occ4, kmeans, etc.
```

**b) Calculate Actual Wage Changes**
```python
# Pre-shock period (2009)
data_pre = data_full[(data_full['year']==pre)]
ln_wage_pre = data_pre.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean()

# Post-shock period (2014)
data_post = data_full[(data_full['year']==post)]
ln_wage_post = data_post.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean()

# Actual change by worker type
actual_ln_wage_delta = ln_wage_post - ln_wage_pre
```

**c) Solve General Equilibrium Model**
```python
# Solve model for pre-shock period
equi_pre = solve_model(eta, I, G, S, a_s_pre, b_gs, m_i,
                      nu_hat, sigma_hat, xi_hat, xi_outopt,
                      phi_outopt, psi_hat, ...)

# Solve model for post-shock period
equi_post = solve_model(eta, I, G, S, a_s_post, b_gs, m_i,
                       nu_hat, sigma_hat, xi_hat, xi_outopt,
                       phi_outopt, psi_hat, ...)
```

**d) Calculate Model-Predicted Changes**
```python
# Expected wages pre/post shock
E_phi_pre = compute_expected_Phi(equi_pre['p_ig'], psi_hat,
                                equi_pre['w_g'], sigma_hat)
E_phi_post = compute_expected_Phi(equi_post['p_ig'], psi_hat,
                                 equi_post['w_g'], sigma_hat)

# Model-predicted change
E_phi_delta = E_phi_post - E_phi_pre
```

**e) Run Regression**
```python
# Regression: Actual change ~ Model-predicted change
X = sm.add_constant(E_phi_delta)  # Add intercept
result = sm.WLS(actual_ln_wage_delta, X, weights=worker_counts).fit()

return {'slope': result.params[1],
        'intercept': result.params[0],
        'mse': MSE,
        'reg_result': result}
```

#### 3. Table Generation
```python
# Extract results for each classification
regs_ig  = fit_returnvals[0]['reg_result']  # (iota, gamma)
regs_o4s = fit_returnvals[2]['reg_result']  # (occ4, sector)
regs_o4g = fit_returnvals[4]['reg_result']  # (occ4, gamma)
regs_ks  = fit_returnvals[3]['reg_result']  # (kmeans, sector)
regs_kg  = fit_returnvals[5]['reg_result']  # (kmeans, gamma)

# Write LaTeX table
table.write('Intercept')
for model in [regs_ig, regs_o4s, regs_o4g, regs_ks, regs_kg]:
    table.write(' & {:.3f}'.format(model.params[0]))

table.write('Model-implied Δ log earnings')
for model in [regs_ig, regs_o4s, regs_o4g, regs_ks, regs_kg]:
    table.write(' & {:.3f}'.format(model.params[1]))
```

## Data Pipeline

### Upstream Data Processing Chain

**1. Raw Data Sources:**
- **IBGE Production Data**: `Data/raw/IBGE/Conta_da_producao_2002_2017_xls/sectors_{state}.csv`
  - Contains sector-level production (`y_s`) and price (`p_s`) data by state and year
  - Covers Rio de Janeiro (state 20), Espírito Santo (22), São Paulo (23)
- **RAIS Earnings Panel**: Individual worker earnings and job characteristics (2009-2012)
- **O*NET Skills Data**: Occupation-based skill measures (for k-means clustering)

**2. Data Processing Steps:**
```python
# Step 1: Process IBGE sector data (process_ibge_data.py)
states = [20, 22, 23]  # Rio, Espírito Santo, São Paulo
sector_data = pd.concat([pd.read_csv(f"sectors_{state}.csv") for state in states])

# Step 2: Load model parameters (load_model_parameters.py)
y_ts = sector_data.pivot_table(index=['year'], columns=['state','s'], values=['y_s'])
p_ts = sector_data.pivot_table(index=['year'], columns=['state','s'], values=['p_s'])

# Step 3: Compute demand shifters a_s
# Normalize to base sector (sector 1, state 20) in pre-period
production_base = p_ts.loc[2009, ('p_s', 20, 1)]**(1/eta) * y_ts.loc[2009, ('y_s', 20, 1)]
a_ts = (p_ts.values**(1/eta) * y_ts.values) / production_base
```

**3. Model Estimation Pipeline:**
- **MLE Estimation**: Estimate structural parameters (ν̂, σ̂, ξ̂) and skill matrix Ψ̂
- **Worker/Market Classification**: Generate network-based types ι and markets γ
- **Comparison Classifications**: Create k-means occupation clusters from O*NET

### Key Data Sources
1. **`fit_mle_data_filename`** - Earnings panel data (2009-2014): `Data/derived/earnings_panel/{filename_stub}_level_0.csv`
2. **`fit_mle_estimates`** - Estimated model parameters (ν̂, σ̂, ξ̂): `Data/derived/MLE_estimates/{filename_stub}_mle_estimates_{suffix}.p`
3. **`fit_psi_hat`** - Estimated skill matrix Ψ̂: `Data/derived/MLE_estimates/{filename_stub}_psi_normalized_{suffix}_eta_{eta}.p`
4. **`a_ts, p_ts`** - Time series of demand shifters and prices by sector (from `load_model_parameters.py`)

### Worker/Market Classifications
- **ι (iota)**: Network-based worker types from estimated model
- **γ (gamma)**: Network-based markets from estimated model
- **occ4_first_recode**: 4-digit occupation codes
- **sector_IBGE**: Brazilian IBGE sector classifications
- **kmeans**: K-means clusters of occupations based on O*NET skills

### Required Model Parameters
- **η (eta)**: Elasticity of substitution between markets
- **S**: Number of sectors
- **I**: Number of worker types
- **G**: Number of markets
- **level**: Estimation level parameter

## Key Results Interpretation

### Table 5 Results
| Classification | Worker | Market | Intercept | Slope | MSE | N |
|---------------|--------|---------|-----------|--------|-----|---|
| **Network-based** | ι | γ | -0.003 | **0.982** | **0.021** | 290 |
| Standard | Occ4 | Sector | -0.001 | 0.148 | 0.025 | 306 |
| Hybrid | Occ4 | γ | -0.002 | 0.428 | 0.025 | 306 |
| K-means | k-means | Sector | -0.000 | 0.234 | 0.023 | 214 |
| K-means | k-means | γ | -0.002 | 0.560 | 0.023 | 214 |

### Key Findings
1. **Network-based approach performs best**: Slope of 0.982 ≈ 1.0 (perfect prediction)
2. **Standard approaches underperform**: All slopes well below 1.0
3. **Markets matter more than workers**: Using γ (network markets) improves prediction even with standard worker classifications
4. **Lowest prediction error**: Network-based approach has lowest MSE

### Economic Interpretation
- Network-based classifications better capture **which workers are exposed** to Olympic-related demand shocks
- Traditional occupation/sector categories are **too coarse** to identify shock transmission
- The model successfully predicts that Olympics infrastructure investment increased wages for construction-related worker types while having smaller effects on others

## How to Run the Analysis

### Prerequisites
Before running `model_fit.py`, you need:

1. **Completed MLE estimation** for all classification combinations in `classification_list`
2. **IBGE sector data** processed into state-specific files
3. **All required parameters defined** from the main `do_all.py` script

### Execution Order
```python
# 1. Set up environment (from do_all.py or similar)
root = "/path/to/project/"
figuredir = root + "Results/"
S = 15  # Number of sectors
eta = 2  # Elasticity parameter
level = 0  # Estimation level

# 2. Define classification combinations
classification_list = [('iota','gamma'), ('occ4_first_recode','sector_IBGE'),
                      ('occ4_first_recode','gamma'), ('kmeans','sector_IBGE'),
                      ('kmeans','gamma')]

# 3. Process IBGE data (if not already done)
sector_data_filepath = root + "Data/raw/IBGE/Conta_da_producao_2002_2017_xls"
exec(open(root + 'Code/process_ibge_data.py').read())

# 4. Load model parameters
exec(open(root + 'Code/load_model_parameters.py').read())

# 5. Run model fit analysis
exec(open(root + 'Code/model_fit.py').read())
```

### Required File Dependencies
```
Data/raw/IBGE/Conta_da_producao_2002_2017_xls/
├── sectors_20.csv  # Rio de Janeiro
├── sectors_22.csv  # Espírito Santo
└── sectors_23.csv  # São Paulo

Data/derived/earnings_panel/
└── {filename_stub}_level_0.csv

Data/derived/MLE_estimates/
├── {filename_stub}_mle_estimates_{wtype}_{jtype}_level_{level}.p
├── {filename_stub}_psi_normalized_{wtype}_{jtype}_level_{level}_eta_{eta}.p
└── {filename_stub}_alphas_{wtype}_{jtype}_level_{level}_eta_{eta}.p

Data/derived/mle_data_sums/
└── {filename_stub}_mle_data_sums_{wtype}_{jtype}_level_{level}.p

Code/
├── model_fit.py                    # Main script
├── Modules/model_fit_func.py       # Core analysis function
├── Modules/solve_model_functions.py  # General equilibrium solver
├── load_model_parameters.py        # Demand shifters & parameters
└── process_ibge_data.py            # IBGE data processing
```

### Output Files
- **`Results/model_fit_se.tex`** - Main results table (Table 5) with standard errors
- **`Results/model_fit.tex`** - Version without standard errors
- **Scatter plots**: `Results/model_fit_shock_E_Phi_norm_{wtype}_{jtype}_level_{level}_eta_{eta}_N.png`

## Summary

This analysis demonstrates the value of the network-based approach to understanding labor market dynamics and provides validation that the structural model can successfully predict real-world economic shocks. The key finding is that network-based worker types (ι) and markets (γ) significantly outperform traditional occupation/sector classifications in predicting the actual wage effects of the Rio Olympics infrastructure investment shock.

The success of the network approach suggests that:
1. **Traditional classifications are too coarse** to capture how economic shocks transmit through labor markets
2. **Network-based types better identify** which workers are exposed to specific demand shocks
3. **Structural models can successfully predict** real-world labor market adjustments when using appropriate classifications



Next prompt:  Suppose all I wanted to do was replicate section 5.5. Could I significantly pare down the    
input code? I want you to create a new folder called "replicate-table-5" and move the 
 .md file into that folder. Then I want you to write new code (building off of the old code)   
that goes all the way from the raw data to the results in section 5.5. It should basically be  
copies of the current code, but pared down to only what is strictly necessary. Do not edit any 
 existing code.     