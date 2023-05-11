#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Please provide the input filename and output filename as arguments."
  exit 1
fi

tex_file="$1"
output_file="$2"

tex_file="$1"

# Extract the filepath from \def\figures{}
figures_path=$(grep -oE '\\def\\figures\{[^}]+\}' "$tex_file" | sed -E 's/.*\{(.*)\}.*/\1/')

# Extract the list of files and replace \figures with the actual filepath
file_list=$(egrep -o '\\input\{[^}]+\}|\\includegraphics(?:\[[^]]+\])?\{[^}]+\}' "$tex_file" | sed -E -e 's/.*\{(.*)\}.*/\1/')

# Write the file list to a text file
echo "$file_list" > "$output_file"


#-e '/^\\def\\figures\{.*\}/{s/.*\{(.*)\}.*/\1/; h;}' -e "/\\includegraphics/{s/\\includegraphics(\[.*\])?\{(.*)\}/\2/; x; G; s/(.*)\n(.*)/$figures_path\/\2/;"
