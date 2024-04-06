********************************************************************************
* Dix-Carneiro and Kovak AER replication files
*
* RAIS Empirical Analyses
* 
* These codes conduct all of the empirical analyses using RAIS.
* They read regional level data that were generated starting with the 
* RAIS micro-data.
* They also read a variety of other data files used in the analyses, and found 
* in folder Data.
********************************************************************************

* Update this line to point to the \ReplicationFiles\ folder
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
global results "${root}Results\"

cap log close

log using ${results}Run_RAIS_Analysis_Log.log, replace

********************************************************************************
do ${root}\Codes_RAIS_Regressions\1_Main_Regressions_Earnings.do
* Generates results used in Figure 3 and Table 2
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\2_Main_Regressions_Employment.do
* Generates results used in Figure 4 and Table 2
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\3_Mechanisms_WorkerComp.do
* Generates results in Table 5
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\4_ADH_IV.do
* Generates Table 6 and Figure 5
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\5_ResidualAnalysis.do
* Generates Figure 6
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\6_NumberOfPlants.do
* Generates Results in Figure 7
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\7_PlantEntryExit.do
* Generates Figure 8
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\8_JobDestruction_JobCreation.do
* Generates Figure 9
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\9_TestAggEcon.do
* Generates results in Table 7
* Test for Agglomeration Economies
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\10_AgglomerationEconomies.do
* Generates Table 8
* Agglomeration Elasticity Estimates
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\11_MainRobustnessEarn.do
* Generates results reported in Tables B6 and B8
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\12_MainRobustnessEmp.do
* Generates results in Table B7
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\13_RobustnessCommodityBoom.do
* Generates Tables B9 and B11
********************************************************************************

********************************************************************************
do ${root}\Codes_RAIS_Regressions\14_DescriptiveStats.do
* Generates part of Table 1, with Descriptive Statistics
* Remaining rows (indexed by an "a" superscript) are generated using Census data
********************************************************************************

log close

