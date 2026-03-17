#!/usr/bin/env python3
"""
Streamlined script to replicate Table 5: Predicted Effect of Olympics on Wages
From Section 5.5 of Fogel and Modenesi (2021)

This script contains only the essential code needed to generate Table 5.
It goes from raw data to final results with minimal dependencies.
"""

import os
import pandas as pd
import numpy as np
import torch
import pickle
import statsmodels.api as sm
from pathlib import Path
from solve_model_simplified import solve_model

# Set up paths
root = "/Users/jsf656/NetworksGit/"
results_dir = root + "replicate-table-5/results/"
os.makedirs(results_dir, exist_ok=True)

# Essential parameters for Table 5
S = 15          # Number of sectors
eta = 2         # Elasticity parameter
level = 0       # Estimation level
pre = 2009      # Pre-shock year
post = 2014     # Post-shock year

# Table 5 classification combinations only
# Note: The original code uses indices that don't match the table order
# This is the correct mapping based on the table headers
classification_list = [
    ('iota', 'gamma'),                      # Column 1: Network-based
    ('occ4_first_recode', 'sector_IBGE'),   # Column 2: Occ4 x Sector
    ('occ4_first_recode', 'gamma'),         # Column 3: Occ4 x γ
    ('kmeans', 'sector_IBGE'),              # Column 4: k-means x Sector
    ('kmeans', 'gamma')                     # Column 5: k-means x γ
]

print("="*60)
print("REPLICATING TABLE 5: PREDICTED EFFECT OF OLYMPICS ON WAGES")
print("="*60)
print(f"Pre-shock year: {pre}")
print(f"Post-shock year: {post}")
print(f"Number of classifications: {len(classification_list)}")
print()

def load_sector_data_and_compute_demand_shifters():
    """
    Load IBGE sector data and compute demand shifters a_ts, p_ts
    Simplified version of process_ibge_data.py + load_model_parameters.py
    """
    print("Loading IBGE sector data and computing demand shifters...")

    # Load sector data for Rio de Janeiro (20), Espírito Santo (22), São Paulo (23)
    states = [20, 22, 23]
    sector_data_filepath = root + "Data/raw/IBGE/Conta_da_producao_2002_2017_xls"

    dfs = []
    for state in states:
        filepath = f"{sector_data_filepath}/sectors_{state}.csv"
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            df['state'] = state
            dfs.append(df)
        else:
            print(f"Warning: {filepath} not found")

    if not dfs:
        raise FileNotFoundError("No sector data files found. Need sectors_20.csv, sectors_22.csv, sectors_23.csv")

    sector_data = pd.concat(dfs, ignore_index=True)
    sector_data = sector_data.loc[sector_data['year'] > 2002]

    # Create pivot tables
    y_ts = sector_data.pivot_table(index=['year'], columns=['state','s'], values=['y_s'])
    p_ts = sector_data.pivot_table(index=['year'], columns=['state','s'], values=['p_s'])

    if S != y_ts.shape[1]:
        raise ValueError(f'Number of sectors do not agree. Expected {S}, got {y_ts.shape[1]}')

    # Compute demand shifters a_ts
    # Normalize to base sector (sector 1, state 20) in pre-period
    sector_base = 1
    state_base = 20
    x_s = 0.65  # Labor share parameter

    try:
        production_base = (p_ts.loc[pre, ('p_s', state_base, sector_base)] ** (1/eta) *
                          y_ts.loc[pre, ('y_s', state_base, sector_base)])

        a_ts = (p_ts.values**(1/eta) * y_ts.values) / production_base

        print(f"Successfully computed demand shifters. Shape: {a_ts.shape}")
        print(f"Years covered: {y_ts.index.min()} to {y_ts.index.max()}")

        return torch.tensor(a_ts), p_ts, y_ts, x_s

    except KeyError as e:
        print(f"Error: Missing data for base normalization. {e}")
        print(f"Available years: {p_ts.index.tolist()}")
        print(f"Available states: {p_ts.columns.get_level_values(1).unique().tolist()}")
        raise

def load_mle_estimates_for_classification(wtype_var, jtype_var):
    """
    Load MLE estimates, data, and parameters for a specific classification
    """
    print(f"Loading MLE estimates for ({wtype_var}, {jtype_var})...")

    # File naming convention from original code
    filename_stub = "panel_rio_2009_2012"  # Adjust as needed
    suffix = f"{wtype_var}_{jtype_var}_level_{level}"

    # File paths
    mle_data_filename = root + f"Data/derived/earnings_panel/{filename_stub}_level_0.csv"
    mle_data_sums_filename = root + f"Data/derived/mle_data_sums/{filename_stub}_mle_data_sums_{suffix}.p"
    mle_estimates_filename = root + f"Data/derived/MLE_estimates/{filename_stub}_mle_estimates_{suffix}.p"
    psi_and_k_file = root + f"Data/derived/MLE_estimates/{filename_stub}_psi_normalized_{suffix}_eta_{eta}.p"
    alphas_file = root + f"Data/derived/MLE_estimates/{filename_stub}_alphas_{suffix}_eta_{eta}.p"

    # Check if files exist
    required_files = [mle_data_filename, mle_data_sums_filename, mle_estimates_filename, psi_and_k_file]
    missing_files = [f for f in required_files if not os.path.exists(f)]

    if missing_files:
        print(f"Warning: Missing files for ({wtype_var}, {jtype_var}):")
        for f in missing_files:
            print(f"  - {f}")
        return None

    try:
        # Load the data and estimates
        mle_data_sums = pickle.load(open(mle_data_sums_filename, "rb"), encoding='bytes')
        mle_estimates = pickle.load(open(mle_estimates_filename, "rb"), encoding='bytes')
        psi_hat = pickle.load(open(psi_and_k_file, "rb"), encoding='bytes')['psi_hat']

        # Load alphas if available
        if os.path.exists(alphas_file):
            with open(alphas_file, "rb") as f:
                alphags = pickle.load(f, encoding='bytes')
        else:
            print(f"Warning: Alphas file not found: {alphas_file}")
            alphags = None

        print(f"Successfully loaded MLE estimates for ({wtype_var}, {jtype_var})")

        return {
            'mle_data_filename': mle_data_filename,
            'mle_data_sums': mle_data_sums,
            'mle_estimates': mle_estimates,
            'psi_hat': psi_hat,
            'alphags': alphags
        }

    except Exception as e:
        print(f"Error loading MLE estimates for ({wtype_var}, {jtype_var}): {e}")
        return None

def compute_expected_Phi(p_ig, psi, w_g, sigma_hat):
    """Compute expected wages (Phi) for workers"""
    return torch.sum(p_ig * (psi * w_g * torch.exp(sigma_hat**2/2)), dim=1)


def run_model_fit_analysis(classification_data, a_ts_tensor, p_ts, x_s, wtype_var, jtype_var):
    """
    Run the model fit analysis for a single classification
    Simplified version of model_fit_func.py
    """
    print(f"Running model fit analysis for ({wtype_var}, {jtype_var})...")

    if classification_data is None:
        print(f"Skipping ({wtype_var}, {jtype_var}) - missing data")
        return None

    try:
        # Load earnings data
        data_full = pd.read_csv(classification_data['mle_data_filename'])
        data_full['ln_real_hrly_wage_dec'] = data_full['ln_real_hrly_wage_dec'].fillna(0)

        # Set up worker and job type variables
        if wtype_var == 'iota' or wtype_var == '1':
            data_full['worker_type'] = data_full.get('iota', 1)
            # Handle special case where iota == -1
            data_full['worker_type'].loc[data_full.get('iota', 1) == -1] = -1
        else:
            if wtype_var not in data_full.columns:
                print(f"Warning: Column {wtype_var} not found in data")
                return None
            data_full['worker_type'] = data_full[wtype_var]

        if jtype_var == 'gamma' or jtype_var == '1':
            data_full['job_type'] = data_full.get('gamma', 1)
            # Handle special cases for gamma
            data_full['job_type'].loc[data_full.get('gamma', 1) == 0] = 0
            data_full['job_type'].loc[data_full.get('gamma', 1) == -1] = -1
        else:
            if jtype_var not in data_full.columns:
                print(f"Warning: Column {jtype_var} not found in data")
                return None
            data_full['job_type'] = data_full[jtype_var]

        # Compute actual wage changes
        # Pre-shock period earnings by worker type
        data_pre = data_full[
            (data_full['gamma'] >= 0) &
            (data_full['iota'] != -1) &
            (data_full['job_type'] != -1) &
            (data_full['worker_type'] != -1) &
            (data_full['year'] == pre)
        ]

        data_post = data_full[
            (data_full['gamma'] >= 0) &
            (data_full['iota'] != -1) &
            (data_full['job_type'] != -1) &
            (data_full['worker_type'] != -1) &
            (data_full['year'] == post)
        ]

        if len(data_pre) == 0 or len(data_post) == 0:
            print(f"Warning: No data for ({wtype_var}, {jtype_var}) in {pre} or {post}")
            return None

        # Mean wages by worker type
        ln_wage_pre = data_pre.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean()
        ln_wage_post = data_post.groupby(['worker_type'])['ln_real_hrly_wage_dec'].mean()

        # Normalize by year's mean wage
        wage_level_pre = data_pre['ln_real_hrly_wage_dec'].mean()
        wage_level_post = data_post['ln_real_hrly_wage_dec'].mean()

        ln_wage_norm_pre = ln_wage_pre / wage_level_pre
        ln_wage_norm_post = ln_wage_post / wage_level_post

        # Actual change in normalized wages
        actual_wage_change = ln_wage_norm_post - ln_wage_norm_pre

        # Get model parameters
        mle_estimates = classification_data['mle_estimates']
        mle_data_sums = classification_data['mle_data_sums']
        psi_hat = classification_data['psi_hat']

        I = mle_data_sums['I']
        G = mle_data_sums['G']
        m_i = mle_data_sums['m_i']

        # Set up b_gs matrix
        if jtype_var == 'sector_IBGE':
            b_gs = torch.diag(x_s * torch.ones(S))
        else:
            alphags = classification_data['alphags']
            if alphags is not None:
                b_gs = alphags * x_s
            else:
                b_gs = torch.ones(G, S) * x_s / S  # Placeholder

        # Get demand shifters for pre and post periods
        # Find the row indices for pre and post years
        year_index_pre = list(p_ts.index).index(pre) if pre in p_ts.index else 0
        year_index_post = list(p_ts.index).index(post) if post in p_ts.index else -1

        a_s_pre = a_ts_tensor[year_index_pre:year_index_pre+1]
        a_s_post = a_ts_tensor[year_index_post:year_index_post+1]

        # Set up outside options (needed for solver)
        phi_outopt = torch.reshape(torch.tensor([0.0]*I, requires_grad=False), (I, 1))
        xi_outopt = torch.tensor([0.0], requires_grad=False)

        # Solve model for pre and post periods
        print(f"Solving model for pre-period ({pre})...")
        equi_pre = solve_model(
            eta, I, G, S, a_s_pre, b_gs, m_i,
            mle_estimates['nu_hat'], mle_estimates['sigma_hat'],
            mle_estimates['xi_hat'], xi_outopt, phi_outopt, psi_hat,
            maxiter=1000, factor=1e-2, tol=1e-3, silent=True
        )

        print(f"Solving model for post-period ({post})...")
        equi_post = solve_model(
            eta, I, G, S, a_s_post, b_gs, m_i,
            mle_estimates['nu_hat'], mle_estimates['sigma_hat'],
            mle_estimates['xi_hat'], xi_outopt, phi_outopt, psi_hat,
            maxiter=1000, factor=1e-2, tol=1e-3, silent=True
        )

        # Compute model-predicted wage changes
        E_phi_pre = compute_expected_Phi(
            equi_pre['p_ig'], psi_hat, equi_pre['w_g'], mle_estimates['sigma_hat']
        )
        E_phi_post = compute_expected_Phi(
            equi_post['p_ig'], psi_hat, equi_post['w_g'], mle_estimates['sigma_hat']
        )

        model_wage_change = E_phi_post - E_phi_pre

        # Align actual and predicted changes (match worker types)
        common_types = set(actual_wage_change.index) & set(range(len(model_wage_change)))
        if not common_types:
            print(f"Warning: No common worker types for regression")
            return None

        # Convert to arrays for regression
        actual_y = np.array([actual_wage_change.get(i, 0) for i in sorted(common_types)])
        predicted_x = np.array([model_wage_change[i].item() if i < len(model_wage_change) else 0
                              for i in sorted(common_types)])
        weights = np.array([m_i.flatten()[i].item() if i < len(m_i.flatten()) else 1
                           for i in sorted(common_types)])

        # Remove any NaN or inf values
        valid_mask = np.isfinite(actual_y) & np.isfinite(predicted_x) & (weights > 0)
        actual_y = actual_y[valid_mask]
        predicted_x = predicted_x[valid_mask]
        weights = weights[valid_mask]

        if len(actual_y) < 2:
            print(f"Warning: Insufficient data for regression ({len(actual_y)} observations)")
            return None

        # Run weighted regression: actual ~ predicted
        X = sm.add_constant(predicted_x)
        result = sm.WLS(actual_y, X, weights=weights).fit()

        # Compute MSE
        mse = np.average((actual_y - result.predict(X))**2, weights=weights)

        print(f"({wtype_var}, {jtype_var}): slope={result.params[1]:.3f}, intercept={result.params[0]:.3f}, MSE={mse:.3f}, N={len(actual_y)}")

        return {
            'slope': result.params[1],
            'intercept': result.params[0],
            'slope_se': result.bse[1],
            'intercept_se': result.bse[0],
            'mse': mse,
            'n': len(actual_y),
            'reg_result': result
        }

    except Exception as e:
        print(f"Error in analysis for ({wtype_var}, {jtype_var}): {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_table5(results):
    """Generate Table 5 LaTeX output"""
    print("\nGenerating Table 5...")

    # Filter out None results
    valid_results = [(i, r) for i, r in enumerate(results) if r is not None]

    if not valid_results:
        print("Error: No valid results to generate table")
        return

    # Table header mapping
    headers = [
        ("$\\iota$", "$\\gamma$"),
        ("Occ4", "Sector"),
        ("Occ4", "$\\gamma$"),
        ("k-means", "Sector"),
        ("k-means", "$\\gamma$")
    ]

    # Generate LaTeX table
    latex_content = []
    latex_content.append("\\begin{tabular}{lccccc}")
    latex_content.append("\\toprule ")

    # Header rows
    latex_content.append("Worker classification \t & " + " \t & ".join([h[0] for h in headers]) + " \\\\ ")
    latex_content.append("Market classification \t & " + " \t & ".join([h[1] for h in headers]) + " \\\\ ")
    latex_content.append("\\midrule ")

    # Intercept row
    intercept_row = "Intercept"
    for i, result in valid_results:
        intercept_row += f" & {result['intercept']:.3f}"
    latex_content.append(intercept_row + " \\\\ ")

    # Intercept standard errors
    intercept_se_row = ""
    for i, result in valid_results:
        intercept_se_row += f" & ( {result['intercept_se']:.3f} )"
    latex_content.append(intercept_se_row + " \\\\ ")

    # Slope row
    slope_row = "Model-implied $\\Delta$ log earnings"
    for i, result in valid_results:
        slope_row += f" & {result['slope']:.3f}"
    latex_content.append(slope_row + " \\\\ ")

    # Slope standard errors
    slope_se_row = ""
    for i, result in valid_results:
        slope_se_row += f" & ({result['slope_se']:.3f})"
    latex_content.append(slope_se_row + " \\\\ ")

    latex_content.append("\\midrule ")

    # MSE row
    mse_row = "MSE"
    for i, result in valid_results:
        mse_row += f" & {result['mse']:.3f}"
    latex_content.append(mse_row + " \\\\ ")

    # N row
    n_row = "Observations"
    for i, result in valid_results:
        n_row += f" & {result['n']}"
    latex_content.append(n_row + " \\\\ ")

    latex_content.append("\\bottomrule ")
    latex_content.append("\\end{tabular}")

    # Write to file
    output_file = results_dir + "table5_predicted_olympics_wages.tex"
    with open(output_file, 'w') as f:
        f.write('\n'.join(latex_content))

    print(f"Table 5 saved to: {output_file}")

    # Also print to console
    print("\nTABLE 5 RESULTS:")
    print("="*80)
    for line in latex_content:
        print(line)

    # Summary
    print("\nSUMMARY:")
    print("-"*40)
    for i, (idx, result) in enumerate(valid_results):
        wtype, jtype = classification_list[idx]
        print(f"{wtype:20s} × {jtype:15s}: slope={result['slope']:6.3f}, MSE={result['mse']:6.3f}, N={result['n']:3d}")

def main():
    """Main execution function"""

    # Step 1: Load sector data and compute demand shifters
    try:
        a_ts_tensor, p_ts, y_ts, x_s = load_sector_data_and_compute_demand_shifters()
    except Exception as e:
        print(f"Error loading sector data: {e}")
        print("Cannot proceed without sector data. Please ensure IBGE sector files are available.")
        return

    # Step 2: Load MLE estimates for all classifications
    print(f"\nLoading MLE estimates for {len(classification_list)} classifications...")
    classification_data = []
    for wtype_var, jtype_var in classification_list:
        data = load_mle_estimates_for_classification(wtype_var, jtype_var)
        classification_data.append(data)

    # Step 3: Run model fit analysis for each classification
    print(f"\nRunning model fit analysis...")
    results = []
    for i, ((wtype_var, jtype_var), data) in enumerate(zip(classification_list, classification_data)):
        print(f"\n--- Classification {i+1}/{len(classification_list)}: ({wtype_var}, {jtype_var}) ---")
        result = run_model_fit_analysis(data, a_ts_tensor, p_ts, x_s, wtype_var, jtype_var)
        results.append(result)

    # Step 4: Generate Table 5
    generate_table5(results)

    print("\n" + "="*60)
    print("TABLE 5 REPLICATION COMPLETE")
    print("="*60)

    # Report success/failure
    successful = sum(1 for r in results if r is not None)
    print(f"Successfully analyzed: {successful}/{len(classification_list)} classifications")

    if successful < len(classification_list):
        print("\nMissing classifications (likely due to missing MLE estimates):")
        for i, ((wtype, jtype), result) in enumerate(zip(classification_list, results)):
            if result is None:
                print(f"  - {wtype} × {jtype}")

if __name__ == "__main__":
    main()