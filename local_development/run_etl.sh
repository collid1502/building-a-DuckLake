#!/bin/bash
set -e

# Python path from your conda env
PYTHON="/opt/conda/envs/duck_etl/bin/python"

echo "Using Python from: $PYTHON"
echo

echo "Build Ducklake ..."
sh wipe_datalake_files.sh
echo "Existing datalake files wiped ..."

$PYTHON ETL/build_ducklake.py
echo "Retail Ducklake successfully built!"
echo

echo "Running Retail DuckLake ETL ..."
# Run scripts

# Bronze Layer
$PYTHON ETL/Bronze_layer/source_customer_data.py &
$PYTHON ETL/Bronze_layer/source_transaction_data.py &
$PYTHON ETL/Bronze_layer/source_store_data.py &
$PYTHON ETL/Bronze_layer/source_product_data.py &

wait 

echo "End of Bronze Layer Build"
echo "======================================================="

# Silver Layer

echo
echo "All scripts completed using duck_etl environment."