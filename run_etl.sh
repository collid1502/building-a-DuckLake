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

    $PYTHON -m src.Bronze_layer.source_data --extract-date "$current_date" --src "customers" &
    $PYTHON -m src.Bronze_layer.source_data --extract-date "$current_date" --src "products" &
    $PYTHON -m src.Bronze_layer.source_data --extract-date "$current_date" --src "stores" &
    $PYTHON -m src.Bronze_layer.source_data --extract-date "$current_date" --src "transactions" &

    wait

    echo "-------------------------------------------------------"
    echo "End of Bronze Layer Build"
    echo "======================================================="

    # Silver Layer
    echo "Build silver layer ..."

    $PYTHON -m src.Silver_layer.dim_customer --extract-date "$current_date"

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
