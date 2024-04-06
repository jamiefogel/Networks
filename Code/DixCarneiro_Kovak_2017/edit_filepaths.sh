#!/bin/bash

for file in $(find /Users/jfogel/NetworksGit/Code/DixCarneiro_Kovak_2017 -type f -name "*.do")
do
    echo "Processing file: $file"
    sed -e 's|global root "C:\\Users\\rd123\\Dropbox\\DixCarneiroKovakRodriguez\\ReplicationFiles\\"|if "\`c(os)\`" == "MacOSX" {\n    global root "/Users/jfogel/NetworksGit/Code/DixCarneiro_Kovak_2017"\n} else if "\`c(os)\`" == "Windows" {\n    global root "\\\\storage6\\usuarios\\labormkt_rafaelpereira\\NetworksGit\\Code\\DixCarneiro_Kovak_2017\\\\"\n}|' \
        -e 's|global data "F:\\RAIS\\Data_Brazil\\RAIS_Stata2\\"|global data1 "${root}Data/"|' \
        -e 's|global data2 "\${root}Data_Other\\"|global data2 "${root}Data_Other/"|' \
        -e 's|global result "\${root}ProcessedData_RAIS\\Panel_1986_2010\\"|global output "${root}Results/MainEarnings/"\nglobal earnings "${root}ProcessedData_RAIS/RegionalEarnPremia/"|'
done