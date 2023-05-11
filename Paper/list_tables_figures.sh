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
  echo "$file_list" > "$output_file"
else
  echo "$file_list"
fi
