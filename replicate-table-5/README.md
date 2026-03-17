# Replicate Table 5: Predicted Effect of Olympics on Wages

This folder contains a streamlined version of the code needed to replicate **Table 5** from Section 5.5 of Fogel and Modenesi (2021). This code has been pared down to include only the essential components required for this specific analysis.

## Overview

Table 5 compares the ability of network-based worker/market classifications versus standard occupation/sector classifications to predict actual wage changes from the 2016 Rio Olympics infrastructure investment shock.

## Files

### Core Scripts
- **`run_analysis.py`** - Simple interface to run the complete analysis pipeline
- **`replicate_table5.py`** - Main script that orchestrates the entire analysis
- **`solve_model_simplified.py`** - Simplified general equilibrium model solver
- **`check_data_availability.py`** - Validates that all required data files are present
- **`section_5_5_analysis_explanation.md`** - Detailed explanation of the methodology

### Output
- **`results/table5_predicted_olympics_wages.tex`** - Generated LaTeX table
- **`results/`** - Directory for all output files

## Required Data

Before running the analysis, you need the following data files in the main project directory:

### 1. IBGE Sector Data
```
Data/raw/IBGE/Conta_da_producao_2002_2017_xls/
├── sectors_20.csv  # Rio de Janeiro
├── sectors_22.csv  # Espírito Santo
└── sectors_23.csv  # São Paulo
```

### 2. MLE Estimates (for each classification)
```
Data/derived/earnings_panel/
└── panel_rio_2009_2012_level_0.csv

Data/derived/MLE_estimates/
├── panel_rio_2009_2012_mle_estimates_{wtype}_{jtype}_level_0.p
├── panel_rio_2009_2012_psi_normalized_{wtype}_{jtype}_level_0_eta_2.p
└── panel_rio_2009_2012_alphas_{wtype}_{jtype}_level_0_eta_2.p

Data/derived/mle_data_sums/
└── panel_rio_2009_2012_mle_data_sums_{wtype}_{jtype}_level_0.p
```

Where `{wtype}_{jtype}` combinations are:
- `iota_gamma` (network-based)
- `occ4_first_recode_sector_IBGE` (4-digit occupation × sector)
- `occ4_first_recode_gamma` (4-digit occupation × network markets)
- `kmeans_sector_IBGE` (k-means clusters × sector)
- `kmeans_gamma` (k-means clusters × network markets)

## Usage

### Prerequisites
```python
pip install pandas numpy torch statsmodels
```

### Run the Analysis

**Option 1: Complete Pipeline (Recommended)**
```bash
cd replicate-table-5/
python run_analysis.py
```
This will automatically check data availability and run the analysis if everything is ready.

**Option 2: Check Data First**
```bash
cd replicate-table-5/
python check_data_availability.py  # Check what's missing
python replicate_table5.py         # Run analysis if data is complete
```

**Option 3: Direct Analysis**
```bash
cd replicate-table-5/
python replicate_table5.py
```

### Expected Output
The script will:
1. Load IBGE sector data and compute demand shifters
2. Load MLE estimates for each classification
3. Solve the general equilibrium model for pre/post-shock periods
4. Run regressions comparing actual vs. predicted wage changes
5. Generate Table 5 in LaTeX format

## Key Results

The analysis should reproduce results showing:

| Classification | Worker | Market | Intercept | Slope | MSE | N |
|---------------|--------|---------|-----------|--------|-----|---|
| **Network-based** | ι | γ | -0.003 | **0.982** | **0.021** | 290 |
| Standard | Occ4 | Sector | -0.001 | 0.148 | 0.025 | 306 |
| Hybrid | Occ4 | γ | -0.002 | 0.428 | 0.025 | 306 |
| K-means | k-means | Sector | -0.000 | 0.234 | 0.023 | 214 |
| K-means | k-means | γ | -0.002 | 0.560 | 0.023 | 214 |

**Key Finding**: Network-based classifications (slope ≈ 1.0) significantly outperform standard approaches in predicting actual wage effects of the Olympics shock.

## Differences from Full Codebase

This streamlined version:
- **Focuses only on Table 5** - removes all other analysis
- **Simplified file structure** - consolidates functionality into fewer files
- **Minimal dependencies** - only essential packages and functions
- **Clear documentation** - everything needed to understand and run the analysis
- **Error handling** - graceful handling of missing data files

## Troubleshooting

### Missing MLE Estimates
If MLE estimates are missing for some classifications:
```
Warning: Missing files for (wtype, jtype):
  - Data/derived/MLE_estimates/panel_rio_2009_2012_mle_estimates_wtype_jtype_level_0.p
```
This means the full estimation pipeline needs to be run first for those classifications.

### IBGE Data Issues
If sector data files are missing:
```
Error loading sector data: No sector data files found
```
Ensure the IBGE production data files are in the correct location.

### Convergence Issues
If the general equilibrium solver doesn't converge:
- Try increasing `maxiter` parameter
- Adjust `factor` (dampening parameter)
- Check that all input data is valid

## Next Steps

To run the full analysis pipeline and generate all required MLE estimates:
1. Use the main `do_all.py` script in the root directory
2. Ensure all raw data files are available
3. Run the full estimation for all classification combinations

This streamlined version provides a focused pathway to reproduce Table 5 without the complexity of the full codebase.