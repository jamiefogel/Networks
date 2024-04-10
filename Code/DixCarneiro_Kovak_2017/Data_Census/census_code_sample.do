******************************************************************************
* census_code_sample.do
* Dix-Carneiro and Kovak AER replication files
*
* Takes raw census files, extracts relevant variables, codes them for
* analysis, and restricts the sample.
******************************************************************************

set more off
capture log close
clear
clear matrix

cd "${root}Data_Census"
log using census_code_sample.txt, text replace

* Extract relevant variables, give intuitive names, code values as needed,
* and combine files across states to generate cenXX.dta for each year
do cen70code.do
do cen80code.do
do cen91code.do
do cen00code.do
do cen10code.do

* Generate variables for analysis and restrict sample
do code_sample_1970.do
do code_sample_1980.do
do code_sample.do

* Generate rent variables for Moretti price index analysis
do rent91code.do
do rent10code.do

log close
cd "${root}"
