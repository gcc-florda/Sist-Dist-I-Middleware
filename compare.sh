#!/bin/bash

# Check for correct number of arguments
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <directory_1> <directory_2>"
  exit 1
fi

# Assign input directories to variables
DIR1="$1"
DIR2="$2"

# Ensure both directories exist
if [ ! -d "${DIR1}" ]; then
  echo "Directory ${DIR1} does not exist."
  exit 1
fi

if [ ! -d "${DIR2}" ]; then
  echo "Directory ${DIR2} does not exist."
  exit 1
fi

# Function to compare files in the two directories
compare_files() {
  local file1="$1"
  local file2="$2"
  local base_file=$(basename "${file1}")

  # Compare the files (ignoring order and whitespace differences)
  if diff -wB <(sort "${file1}" | tr -d '\r') <(sort "${file2}" | tr -d '\r') > /dev/null; then
    echo "File ${base_file} matches."
  else
    echo "File ${base_file} does NOT match. Showing differences:"
    diff -wB <(sort "${file1}" | tr -d '\r') <(sort "${file2}" | tr -d '\r')
  fi
}

# Iterate through files in the first directory
echo "Comparing files in ${DIR1} and ${DIR2}..."
for file1 in "${DIR1}"/*.csv; do
  base_file=$(basename "${file1}")
  file2="${DIR2}/${base_file}"

  # Check if the file exists in the second directory
  if [ ! -f "${file2}" ]; then
    echo "File ${base_file} is missing in ${DIR2}."
    continue
  fi

  # Compare the two files
  compare_files "${file1}" "${file2}"
done

# Check if DIR2 has extra files not in DIR1
for file2 in "${DIR2}"/*.csv; do
  base_file=$(basename "${file2}")
  file1="${DIR1}/${base_file}"

  if [ ! -f "${file1}" ]; then
    echo "File ${base_file} is missing in ${DIR1}."
  fi
done

echo "Comparison complete."