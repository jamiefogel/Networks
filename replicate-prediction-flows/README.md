# Section 5.3 Replication: Predicting Out-of-Sample Job-to-Job Flows

This folder contains streamlined code to replicate Section 5.3 of the paper, which tests whether network-based labor market definitions predict job-to-job transitions better than traditional occupation×sector classifications.

## Overview

The analysis compares three market definitions:
- **Gamma (γ)**: Network-based markets from bipartite SBM clustering
- **Iota-Gamma (ι)**: Worker type-based classification
- **Occ2×Meso**: Traditional 2-digit occupation × meso region classification

The key finding: Network-based markets achieve lower prediction errors, validating the paper's methodological contribution.

## Requirements

### Data
- Raw Brazilian RAIS employment data (CSV files): `~/rais/RAIS/csv/brasil[YEAR].csv`
- Years needed: 2009, 2010, 2011 (in-sample), 2012, 2013 (out-of-sample)
- Region codes file: `./Data/raw/munic_microregion_rm.csv`

### Software
- Python 3.7+
- Required packages:
  - `graph-tool` (for SBM estimation)
  - `pandas`, `numpy`, `scipy` (data processing)
  - `matplotlib` (plotting)

**Note**: Installing graph-tool can be challenging. See: https://graph-tool.skewed.de/download

## Quick Start

### Option 1: Full Pipeline (Recommended)
Run everything from raw data to final results:

```bash
# 1. Run the main replication script (processes data and runs sample analysis)
python3 replicate_section_5_3.py

# 2. Run full prediction analysis (computationally intensive)
./run_parallel_analysis.sh    # Uses multiple CPU cores (recommended)
# OR
python3 run_full_prediction_analysis.py  # Single core (very slow)

# 3. Aggregate and analyze results
python3 aggregate_results.py
```

### Option 2: Step-by-Step

1. **Data Processing & Sample Analysis**:
```bash
python3 replicate_section_5_3.py
```
This script:
- Loads and processes raw RAIS data (2009-2013)
- Runs SBM to identify network markets (γ) and worker types (ι)
- Creates job transition matrices
- Runs prediction analysis on sample of 1,000 jobs
- Shows expected results format

2. **Full Prediction Analysis** (choose one):

**Option A: Parallel Processing** (recommended):
```bash
./run_parallel_analysis.sh
```
- Modify `TOTAL_CORES` in script based on your CPU
- Runs prediction analysis across all ~3.4M jobs
- Uses multiple CPU cores for efficiency
- Takes several hours depending on hardware

**Option B: Single Core** (slow):
```bash
python3 run_full_prediction_analysis.py
```
- Processes all jobs on single core
- Can take days to complete
- Only use if parallel processing not available

**Option C: Custom Parallel**:
```bash
# Run specific core manually
python3 run_full_prediction_analysis.py [core_number] [total_cores]
# Example: Run core 2 of 8 cores total
python3 run_full_prediction_analysis.py 2 8
```

3. **Results Aggregation**:
```bash
python3 aggregate_results.py
```
This script:
- Combines results from all CPU cores
- Computes summary statistics
- Runs robustness checks
- Creates summary plots
- Produces final comparison metrics

## File Structure

```
replicate-prediction-flows/
├── README.md                           # This file
├── complete_data_pipeline_documentation.md  # Full pipeline documentation
├── section_5_3_theory_and_implementation.md  # Educational theory walkthrough
├── replicate_section_5_3.py           # Main replication script
├── run_full_prediction_analysis.py    # Full prediction analysis
├── aggregate_results.py               # Results aggregation
├── run_parallel_analysis.sh           # Parallel execution script
├── modules/                           # Simplified modules
│   ├── pull_one_year_simple.py       # Raw data processing
│   └── bisbm_simple.py               # SBM estimation
├── data/                             # Generated data files
│   ├── section_5_3_replication_*.p   # Processed data
│   ├── bipartite_edgelist_*.p        # SBM input
│   └── *_blocks.csv                  # Market/type assignments
└── results/                          # Analysis results
    ├── core_*_final_results.csv      # Individual core results
    ├── *_all_results.csv             # Combined results
    ├── *_summary_statistics.csv      # Summary stats
    ├── *_final_summary.json          # Key findings
    └── *_summary_plots.png           # Visualization
```

## Expected Results

Based on the original paper, you should expect:

### Prediction Error Comparison
- **L₁ errors**: ~15.5 (γ markets) vs ~15.6 (Occ2×Meso)
- **L₂ errors**: ~237.9 (γ markets) vs ~317.7 (Occ2×Meso)

### Key Findings
- Network-based markets provide superior predictive performance
- ~5-25% improvement in L₂ prediction errors
- Majority of jobs show better predictions with network markets
- Robust across different samples and outlier treatments

## Computational Requirements

### Time Estimates
- **Main script**: ~2-4 hours (includes SBM estimation)
- **Full prediction**: ~6-24 hours (depending on CPU cores and hardware)
- **Results aggregation**: ~10 minutes

### Memory Requirements
- **Peak memory usage**: ~16-32 GB (for 3.4M×3.4M job adjacency matrix)
- **Storage space**: ~5-10 GB for intermediate files

### Parallelization
The most computationally intensive step (full prediction analysis) is parallelized across CPU cores:
- Each core processes a subset of the ~3.4M jobs
- Scales linearly with number of cores
- Recommended: 8-16 cores for reasonable runtime

## Customization

### Adjust Sample Size
For faster testing, modify the sample size in `replicate_section_5_3.py`:
```python
sample_jobs = min(1000, J)  # Change 1000 to desired sample size
```

### Change Geographic Scope
Modify state codes in configuration:
```python
state_codes = [31, 33, 35]  # Currently: MG, ES, RJ
```

### Adjust Parallel Processing
Modify core count in `run_parallel_analysis.sh`:
```bash
TOTAL_CORES=8  # Adjust based on your CPU
```

## Troubleshooting

### Common Issues

1. **Graph-tool installation problems**:
   - Use conda: `conda install -c conda-forge graph-tool`
   - Or follow OS-specific instructions: https://graph-tool.skewed.de/download

2. **Memory errors**:
   - Reduce sample size for testing
   - Ensure sufficient RAM (16GB+ recommended)
   - Close other applications

3. **Missing RAIS data**:
   - Ensure CSV files exist at `~/rais/RAIS/csv/brasil[YEAR].csv`
   - Check file permissions and accessibility
   - Verify years 2009-2013 are available

4. **Slow performance**:
   - Use parallel processing (`run_parallel_analysis.sh`)
   - Increase number of CPU cores
   - Consider running on high-performance computing cluster

5. **Incomplete results**:
   - Check log files in `results/` folder
   - Verify all cores completed successfully
   - Re-run failed cores individually

### Getting Help

If you encounter issues:
1. Check the log files in `./replicate-prediction-flows/results/`
2. Verify your Python environment has all required packages
3. Ensure you have sufficient computational resources
4. Review the full pipeline documentation: `complete_data_pipeline_documentation.md`

## Differences from Original Code

This streamlined replication focuses only on Section 5.3 and omits:
- Earnings panel creation and MLE estimation
- Model fitting and general equilibrium analysis
- Other sections of the paper
- Advanced MCMC refinements for bipartite SBM
- Extensive diagnostic and validation checks

For the complete pipeline, see the main codebase in `../Code/`.

## Citation

When using this replication code, please cite the original paper:
> Fogel, J. and Modenesi, B. (2021). "Networks in Labor Markets". [Paper details]