#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Please provide the filename of the TeX file as an argument."
  exit 1
fi

tex_file="$1"

# Extract the filepath from \def\figures{}
figures_path=$(grep -oE '\\def\\figures\{[^}]+\}' "$tex_file" | sed -E 's/.*\{(.*)\}.*/\1/')

# Extract the list of files and replace \figures with the actual filepath
file_list=$(egrep -o '\\input\{[^}]+\}|\\includegraphics(?:\[[^]]+\])?\{[^}]+\}' "$tex_file" | sed -E -e 's/.*\{(.*)\}.*/\1/')

# Write the file list to a text file
echo "$file_list" > file_list.txt



#-e '/^\\def\\figures\{.*\}/{s/.*\{(.*)\}.*/\1/; h;}' -e "/\\includegraphics/{s/\\includegraphics(\[.*\])?\{(.*)\}/\2/; x; G; s/(.*)\n(.*)/$figures_path\/\2/;"
