******************************************************************************
* 1_Run_Census_Other.do
* Dix-Carneiro and Kovak AER replication files
*
* This do file is a wrapper that calls other files, which complete the
* tasks listed in comments below.  It performs analyses relating to 
* Census data and other auxiliary data sets
*
* The first line must point to the ReplicationFiles directory in which 
* this do file resides.
******************************************************************************

* Update this line to point to the /ReplicationFiles/ folder
local os "`c(os)'"
if "`os'" == "MacOSX" {
    global root "/Users/jfogel/NetworksGit/Code/DixCarneiro_Kovak_2017"
}
else if "`os'" == "Windows" {
    global root "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017"
}
else {
    di "Unsupported operating system"
    exit
}

cd "${root}"

***********
* Census data - extract and code variables and impose sample restrictions
do ./Data_Census/census_code_sample.do

***********
* Produce regional tariff shocks
do ./Codes_Other/rtc_kume_mmc1970.do

***********
* Generate Figure 1 - creates ./Codes_Other/figure1.pdf
do ./Codes_Other/figure_1.do

***********
* Generate data underlying Figure 2 - creates ./Codes_Other/figure2.csv
do ./Codes_Other/figure_2.do

***********
* Generate Table 3 - creates ./Codes_Census/table_3.xls
do ./Codes_Census/table_3.do

***********
* Generate Table 4 Panel A - creates ./Codes_Census/table_4_panel_A.xls
do ./Codes_Census/table_4_panel_A.do

***********
* Generate Table 4 Panel B - creates ./Codes_Census/table_4_panel_B.xls
do ./Codes_Census/table_4_panel_B.do

***********
* Generate Census portions of Table 1 - creates ./Codes_Census/table_1_census.txt
do ./Codes_Census/table_1_census.do

***********
* Create various auxiliary files used in RAIS analysis and robustness tests
* Results appear in ./Data/
* See individual do files for descriptions
do ./Codes_Other/rtc_trains.do
do ./Codes_Other/tariff_chg_kume_subsibge.do
do ./Codes_Other/rcs.do
do ./Codes_Other/dlnrent.do
do ./Codes_Other/frtc_kume.do
do ./Codes_Census/dln_employment.do
do ./Codes_Census/dln_earnings.do
do ./Codes_Other/delta_mmc.do
do ./Codes_Other/adao_shock_mmc.do
do ./Codes_Other/adh_shocks.do
do ./Codes_Other/colombia_worldmb_adh_shocks.do
do ./Codes_Other/la_worldmb_adh_shocks.do
do ./Codes_Other/mmc_rer.do


