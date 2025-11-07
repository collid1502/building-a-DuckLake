#!/bin/bash
set -e

# Loop over a date range
start_date="2025-11-01"
end_date="2025-11-02"

# Python path from your conda env
PYTHON="/opt/conda/envs/duck_etl/bin/python"

echo "Using Python from: $PYTHON"
echo
echo "Running Retail DuckLake ETL ..."
# Run scripts

current_date="$start_date"

while [ "$current_date" != "$(date -I -d "$end_date + 1 day")" ]; do
    echo "Running ETL for extract-date: $current_date"

    $PYTHON ./Bronze_layer/source_customer_data.py --extract-date "$current_date" &
    $PYTHON ./Bronze_layer/source_transaction_data.py --extract-date "$current_date" &
    $PYTHON ./Bronze_layer/source_store_data.py --extract-date "$current_date" &
    $PYTHON ./Bronze_layer/source_product_data.py --extract-date "$current_date" &

    wait

    echo "-------------------------------------------------------"
    echo "End of Bronze Layer Build"
    echo "======================================================="

    # Silver Layer
    echo "Build silver layer ..."

    $PYTHON ./Silver_layer/dim_customer.py --extract-date "$current_date"

    echo "Finished ETL for $current_date"
    echo 
    echo "-------------------------------------------------------"
    echo "End of Silver Layer Build"
    echo "======================================================="

    echo
    echo "All scripts completed using duck_etl environment."

    current_date=$(date -I -d "$current_date + 1 day")
done

echo "End of mock ETL"
echo "======================================================="
