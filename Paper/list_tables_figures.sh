#!/bin/bash

if [ $# -eq 0 ]; then
  echo "Please provide the filename of the TeX file as an argument."
  exit 1
fi

tex_file="$1"

# Extract the filepath from \def\figures{}
figures_path=$(grep -oE '\\def\\figures\{[^}]+\}' "$tex_file" | sed -E 's/.*\{(.*)\}.*/\1/')

# Extract the list of files and replace \figures with the actual filepath
file_list=$(egrep -o '\\input\{[^}]+\}|\\includegraphics(?:\[[^]]+\])?\{[^}]+\}' "$tex_file" | awk -v figures_path="$figures_path" -F'[{]' '{ 
    if ($0 ~ /^\\def\\figures\{.*\}/) { 
        sub(/\\def\\figures\{/, "", $0); 
        sub(/\}/, "", $0); 
        figures_path=$0; 
    } 
    else { 
        sub(/.*[{]/, "", $0); 
        sub(/[}].*/, "", $0); 
        gsub(/\\figures/, figures_path, $0); 
        print $0; 
    } 
}')

# Write the file list to a text file
echo "$file_list" > file_list.txt
