#!/bin/bash

# Parallel execution script for Section 5.3 job transition prediction analysis
#
# This script runs the prediction analysis across multiple CPU cores for efficiency.
# Each core processes a subset of the total jobs.

echo "=============================================================================="
echo "SECTION 5.3 PARALLEL PREDICTION ANALYSIS"
echo "=============================================================================="

# Configuration
TOTAL_CORES=8  # Adjust based on your system's CPU cores
SCRIPT_PATH="./replicate-prediction-flows/run_full_prediction_analysis.py"

# Check if the main script exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "ERROR: Prediction script not found at $SCRIPT_PATH"
    echo "Make sure you've run the main replication script first."
    exit 1
fi

# Create results directory if it doesn't exist
mkdir -p ./replicate-prediction-flows/results

# Check if Python environment has required packages
echo "Checking Python environment..."
python3 -c "import graph_tool, pandas, numpy, scipy" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: Required Python packages not available."
    echo "Make sure graph-tool, pandas, numpy, and scipy are installed."
    echo "For graph-tool installation, see: https://graph-tool.skewed.de/download"
    exit 1
fi

echo "Starting parallel analysis with $TOTAL_CORES cores..."
echo "Start time: $(date)"
echo ""

# Launch jobs in parallel
for core in $(seq 1 $TOTAL_CORES); do
    echo "Launching core $core..."
    python3 "$SCRIPT_PATH" $core $TOTAL_CORES > "./replicate-prediction-flows/results/core_${core}_output.log" 2>&1 &
done

echo ""
echo "All cores launched. Monitoring progress..."

# Function to check if all cores are complete
check_completion() {
    local completed=0
    for core in $(seq 1 $TOTAL_CORES); do
        if [ -f "./replicate-prediction-flows/results/core_${core}_final_results.csv" ]; then
            ((completed++))
        fi
    done
    echo $completed
}

# Monitor progress
while [ $(check_completion) -lt $TOTAL_CORES ]; do
    completed=$(check_completion)
    echo "Progress: $completed/$TOTAL_CORES cores completed ($(date))"

    # Show core status
    for core in $(seq 1 $TOTAL_CORES); do
        log_file="./replicate-prediction-flows/results/core_${core}_log.txt"
        if [ -f "$log_file" ]; then
            last_line=$(tail -n 1 "$log_file" | head -n 1)
            echo "  Core $core: $last_line"
        else
            echo "  Core $core: Starting..."
        fi
    done

    echo ""
    sleep 60  # Check every minute
done

# Wait for all background jobs to complete
wait

echo "=============================================================================="
echo "PARALLEL ANALYSIS COMPLETE"
echo "=============================================================================="
echo "End time: $(date)"
echo ""

# Check that all cores completed successfully
success_count=0
for core in $(seq 1 $TOTAL_CORES); do
    result_file="./replicate-prediction-flows/results/core_${core}_final_results.csv"
    if [ -f "$result_file" ]; then
        line_count=$(wc -l < "$result_file")
        echo "Core $core: $((line_count - 1)) jobs completed"  # Subtract 1 for header
        ((success_count++))
    else
        echo "Core $core: FAILED (no results file)"
    fi
done

if [ $success_count -eq $TOTAL_CORES ]; then
    echo ""
    echo "SUCCESS: All $TOTAL_CORES cores completed successfully"
    echo ""
    echo "Next step: Run the aggregation script to combine results"
    echo "  python3 ./replicate-prediction-flows/aggregate_results.py"
else
    echo ""
    echo "WARNING: Only $success_count/$TOTAL_CORES cores completed successfully"
    echo "Check the log files in ./replicate-prediction-flows/results/ for errors"
fi

echo "=============================================================================="