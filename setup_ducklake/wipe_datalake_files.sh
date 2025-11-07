#!/bin/bash

# Get the current directory name
current_dir=${PWD##*/}

# If we're not already in local_development, cd into it
if [ "$current_dir" != "setup_ducklake" ]; then
  cd ./setup_ducklake/ || { echo "Failed to cd into setup_ducklake directory"; exit 1; }
fi

# Reset the data directory
rm -rf ./data
mkdir data

# Optional: cd back to the original directory (only if you were outside)
if [ "$current_dir" != "setup_ducklake" ]; then
  cd ..
fi