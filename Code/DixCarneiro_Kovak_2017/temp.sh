#!/bin/bash

# Loop over all .do files in the directory and subdirectories
for file in $(find /Users/jfogel/NetworksGit/Code/DixCarneiro_Kovak_2017 -name "*.do")
do
    # Print the file
    echo "$file"
    # Use sed to replace the backslashes with forward slashes in each file
    sed -i '' 's|\\|/|g' "$file"
done




