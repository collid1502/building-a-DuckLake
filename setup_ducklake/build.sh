#!/bin/bash
set -e

# Python path from your conda env
PYTHON="/opt/conda/envs/duck_etl/bin/python"

echo "Using Python from: $PYTHON"
echo

echo "Build Ducklake ..."
sh wipe_datalake_files.sh
echo "Existing datalake files wiped ..."

$PYTHON ./build_ducklake.py
echo "Retail Ducklake successfully built!"
echo
