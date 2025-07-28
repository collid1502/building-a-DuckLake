#!/bin/bash
set -e

# Python path from your conda env
PYTHON="/opt/conda/envs/duck_etl/bin/python"

echo "Using Python from: $PYTHON"
echo "Running Retail DuckLake ETL ..."
echo

# Run scripts in parallel
$PYTHON local_development/ETL/Bronze_layer/source_customer_data.py &
$PYTHON local_development/ETL/Bronze_layer/source_transaction_data.py &

# Wait for both to finish
wait

echo
echo "All scripts completed using duck_etl environment."