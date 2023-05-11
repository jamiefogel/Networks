#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Please provide the input filename as an argument."
  exit 1
fi

tex_file="$1"

# Extract the list of files
file_list=$(egrep -o '\\input\{[^}]+\}|\\includegraphics(?:\[[^]]+\])?\{[^}]+\}' "$tex_file" | sed -E 's/.*\{(.*)\}.*/\1/')

# Check if output file is provided
if [ $# -eq 2 ]; then
  output_file="$2"

  # Check if the output file already exists
  if [ -e "$output_file" ]; then
    read -p "The output file '$output_file' already exists. Do you want to overwrite it? (y/n): " overwrite_choice
    if [ "$overwrite_choice" != "y" ]; then
      echo "Operation cancelled."
      exit 0
    fi
  fi

  echo "$file_list" > "$output_file"
else
  echo "$file_list"
fi
