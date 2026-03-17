#!/usr/bin/env python3
"""
Data Availability Checker for Table 5 Replication

This script checks if all required data files are present before running the analysis.
Run this first to identify any missing files.
"""

import os
from pathlib import Path

# Set up paths
root = "/Users/jsf656/NetworksGit/"

# Classification combinations for Table 5
classification_list = [
    ('iota', 'gamma'),
    ('occ4_first_recode', 'sector_IBGE'),
    ('occ4_first_recode', 'gamma'),
    ('kmeans', 'sector_IBGE'),
    ('kmeans', 'gamma')
]

def check_ibge_data():
    """Check if IBGE sector data files are available"""
    print("1. CHECKING IBGE SECTOR DATA")
    print("=" * 40)

    sector_data_filepath = root + "Data/raw/IBGE/Conta_da_producao_2002_2017_xls"
    states = [20, 22, 23]
    state_names = {20: "Rio de Janeiro", 22: "Espírito Santo", 23: "São Paulo"}

    missing_files = []
    found_files = []

    for state in states:
        filepath = f"{sector_data_filepath}/sectors_{state}.csv"
        if os.path.exists(filepath):
            found_files.append((state, state_names[state]))
            print(f"✓ Found: sectors_{state}.csv ({state_names[state]})")
        else:
            missing_files.append((state, state_names[state]))
            print(f"✗ Missing: sectors_{state}.csv ({state_names[state]})")

    print(f"\nIBGE Data Summary: {len(found_files)}/{len(states)} files found")

    if missing_files:
        print("\nMissing IBGE files:")
        for state, name in missing_files:
            print(f"  - {sector_data_filepath}/sectors_{state}.csv")
        return False
    return True

def check_earnings_panel_data():
    """Check if earnings panel data is available"""
    print("\n2. CHECKING EARNINGS PANEL DATA")
    print("=" * 40)

    filename_stub = "panel_rio_2009_2012"
    earnings_file = root + f"Data/derived/earnings_panel/{filename_stub}_level_0.csv"

    if os.path.exists(earnings_file):
        print(f"✓ Found: {earnings_file}")
        return True
    else:
        print(f"✗ Missing: {earnings_file}")
        return False

def check_mle_estimates():
    """Check if MLE estimates are available for each classification"""
    print("\n3. CHECKING MLE ESTIMATES")
    print("=" * 40)

    filename_stub = "panel_rio_2009_2012"
    level = 0
    eta = 2

    all_present = True

    for i, (wtype_var, jtype_var) in enumerate(classification_list):
        print(f"\nClassification {i+1}: ({wtype_var}, {jtype_var})")

        suffix = f"{wtype_var}_{jtype_var}_level_{level}"

        # Required files
        files_to_check = [
            (f"Data/derived/mle_data_sums/{filename_stub}_mle_data_sums_{suffix}.p", "MLE data sums"),
            (f"Data/derived/MLE_estimates/{filename_stub}_mle_estimates_{suffix}.p", "MLE estimates"),
            (f"Data/derived/MLE_estimates/{filename_stub}_psi_normalized_{suffix}_eta_{eta}.p", "Psi matrix"),
            (f"Data/derived/MLE_estimates/{filename_stub}_alphas_{suffix}_eta_{eta}.p", "Alphas (optional)")
        ]

        classification_complete = True
        for filepath, description in files_to_check:
            full_path = root + filepath
            if os.path.exists(full_path):
                print(f"  ✓ {description}")
            else:
                print(f"  ✗ {description}")
                if "optional" not in description.lower():
                    classification_complete = False
                    all_present = False

        if classification_complete:
            print(f"  → Classification {i+1} is READY")
        else:
            print(f"  → Classification {i+1} is INCOMPLETE")

    return all_present

def check_directory_structure():
    """Check if required directories exist"""
    print("\n4. CHECKING DIRECTORY STRUCTURE")
    print("=" * 40)

    required_dirs = [
        "Data/raw/IBGE/Conta_da_producao_2002_2017_xls",
        "Data/derived/earnings_panel",
        "Data/derived/MLE_estimates",
        "Data/derived/mle_data_sums",
        "replicate-table-5/results"
    ]

    all_dirs_exist = True

    for dir_path in required_dirs:
        full_path = root + dir_path
        if os.path.exists(full_path):
            print(f"✓ {dir_path}")
        else:
            print(f"✗ {dir_path}")
            all_dirs_exist = False

            # Try to create results directory
            if dir_path == "replicate-table-5/results":
                try:
                    os.makedirs(full_path, exist_ok=True)
                    print(f"  → Created: {dir_path}")
                    all_dirs_exist = True  # This one is OK now
                except:
                    pass

    return all_dirs_exist

def main():
    """Main data availability check"""
    print("DATA AVAILABILITY CHECK FOR TABLE 5 REPLICATION")
    print("=" * 60)
    print("Checking all required data files...\n")

    # Check each component
    ibge_ok = check_ibge_data()
    earnings_ok = check_earnings_panel_data()
    mle_ok = check_mle_estimates()
    dirs_ok = check_directory_structure()

    # Overall summary
    print("\n" + "=" * 60)
    print("OVERALL SUMMARY")
    print("=" * 60)

    components = [
        ("IBGE Sector Data", ibge_ok),
        ("Earnings Panel Data", earnings_ok),
        ("MLE Estimates", mle_ok),
        ("Directory Structure", dirs_ok)
    ]

    ready_count = sum(1 for _, ok in components if ok)

    for component, ok in components:
        status = "✓ READY" if ok else "✗ MISSING"
        print(f"{component:20s}: {status}")

    print(f"\nComponents ready: {ready_count}/{len(components)}")

    if ready_count == len(components):
        print("\n🎉 ALL DATA IS AVAILABLE! You can run 'python replicate_table5.py'")
        return True
    else:
        print(f"\n⚠️  MISSING DATA: {len(components) - ready_count} components need attention")
        print("\nTo get missing data files:")

        if not ibge_ok:
            print("  - IBGE data: Download from Brazilian national accounts")
        if not earnings_ok:
            print("  - Earnings panel: Process RAIS data using the main pipeline")
        if not mle_ok:
            print("  - MLE estimates: Run full estimation using do_all.py")
        if not dirs_ok:
            print("  - Directories: Some required directories are missing")

        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)