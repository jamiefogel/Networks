#!/usr/bin/env python3
"""
Simple Interface for Table 5 Replication

This script provides a simple way to run the complete Table 5 analysis
with appropriate data checking and error handling.
"""

import sys
import os
import subprocess
from pathlib import Path

def run_data_check():
    """Run the data availability check"""
    print("STEP 1: CHECKING DATA AVAILABILITY")
    print("=" * 50)

    try:
        result = subprocess.run([sys.executable, "check_data_availability.py"],
                              capture_output=True, text=True)

        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)

        return result.returncode == 0

    except Exception as e:
        print(f"Error running data check: {e}")
        return False

def run_table5_analysis():
    """Run the main Table 5 analysis"""
    print("\nSTEP 2: RUNNING TABLE 5 ANALYSIS")
    print("=" * 50)

    try:
        # Run the main analysis script
        result = subprocess.run([sys.executable, "replicate_table5.py"],
                              capture_output=False, text=True)

        return result.returncode == 0

    except Exception as e:
        print(f"Error running analysis: {e}")
        return False

def main():
    """Main execution function"""
    print("TABLE 5 REPLICATION PIPELINE")
    print("=" * 50)
    print("This script will:")
    print("1. Check if all required data files are available")
    print("2. Run the Table 5 analysis if data is complete")
    print("3. Generate results in the results/ directory")
    print()

    # Change to the correct directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)

    # Step 1: Check data availability
    data_ok = run_data_check()

    if not data_ok:
        print("\n❌ DATA CHECK FAILED")
        print("Please ensure all required data files are available before running the analysis.")
        print("See the data check output above for details on missing files.")
        return False

    # Step 2: Run the analysis
    print("\n✅ DATA CHECK PASSED - PROCEEDING WITH ANALYSIS")
    analysis_ok = run_table5_analysis()

    if analysis_ok:
        print("\n🎉 ANALYSIS COMPLETED SUCCESSFULLY!")
        print("\nResults saved to:")
        print("  - results/table5_predicted_olympics_wages.tex")
        print("\nYou can now use the generated LaTeX table in your document.")
    else:
        print("\n❌ ANALYSIS FAILED")
        print("Check the error messages above for details.")

    return analysis_ok

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)