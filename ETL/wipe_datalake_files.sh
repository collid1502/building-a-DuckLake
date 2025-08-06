#!/bin/bash

# Get the current directory name
current_dir=${PWD##*/}

# If we're not already in local_development, cd into it
if [ "$current_dir" != "ETL" ]; then
  cd ./ETL/ || { echo "Failed to cd into ETL"; exit 1; }
fi

# Reset the data directory
rm -rf ./data
mkdir data

# Optional: cd back to the original directory (only if you were outside)
if [ "$current_dir" != "ETL" ]; then
  cd ..
fi