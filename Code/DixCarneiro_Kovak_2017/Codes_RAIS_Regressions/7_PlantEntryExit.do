********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates Figure 8
********************************************************************************

clear

* global root "C:/Users/rd123/Dropbox/DixCarneiroKovakRodriguez/ReplicationFiles/"

global data1 "${root}Data/"
global data2 "${root}Data_Other/"
global data3 "${root}ProcessedData_RAIS/JobDestruction_JobCreation/"
global result "${root}Results/JobDestruction_JobCreation/"
global earnings "${root}ProcessedData_RAIS/RegionalEarnPremia/"

********************************************************************************
********************************************************************************

* Make sure mmc variables across datasets previously generated are 
* transformed to string + rename variables whenever necessary

use ${data1}rtc_kume
tostring mmc, replace
sort mmc
save ${data1}rtc_kume, replace

use ${data2}mmc_1991_2010_to_c_mesoreg
tostring mmc, replace
rename c_mesoreg mesoreg
sort mmc
save ${data2}rais_mmc_to_mesoreg, replace

********************************************************************************
********************************************************************************

use ${data3}PlantEntryExit_Base1986

sort mmc
merge 1:1 mmc using ${data3}PlantEntryExit_Base1991
keep if _merge == 3
drop _merge

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main) 
drop _merge

sort mmc
merge 1:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

keep if coeff_rem_dez1990 ~= . & coeff_rem_dez1986 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

gen rtr_kume_main = -rtc_kume_main

gen state = substr(mmc,1,2)

********************************************************************************
********************************************************************************

******
* EXIT
******

set more off

cap drop exit

gen exit_86_90 = log(NEG1990)

gen exit = log(NEG1992)
xi: reg exit rtr_kume_main exit_86_90  i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main exit_86_90 using ${result}PlantExit, excel bdec(5) ctitle(1992) replace

forvalues year = 1993/2010{
replace exit = log(NEG`year')
xi: reg exit rtr_kume_main exit_86_90  i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main exit_86_90 using ${result}PlantExit, excel bdec(5) ctitle(`year') append
}

********************
* EXIT -- Pre-Trends
********************

replace exit = log(NEG1987)
xi: reg exit rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}PlantExit_PreTrends, excel bdec(5) ctitle(1992) replace

forvalues year = 1988/1991{
replace exit = log(NEG`year')
xi: reg exit rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}PlantExit_PreTrends, excel bdec(5) ctitle(`year') append
}


*******
* ENTRY
*******

set more off

cap drop entry

gen entry_86_90 = log(POS1990)

gen entry = log(POS1992)
xi: reg entry rtr_kume_main entry_86_90 i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main entry_86_90 using ${result}PlantEntry, excel bdec(5) ctitle(1992) replace

forvalues year = 1993/2010{
replace entry = log(POS`year')
xi: reg entry rtr_kume_main entry_86_90 i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main entry_86_90 using ${result}PlantEntry, excel bdec(5) ctitle(`year') append
}

*********************
* ENTRY -- Pre-Trends
*********************

replace entry = log(POS1987)
xi: reg entry rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}PlantEntry_PreTrends, excel bdec(5) ctitle(1992) replace

forvalues year = 1988/1991{
replace entry = log(POS`year')
xi: reg entry rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}PlantEntry_PreTrends, excel bdec(5) ctitle(`year') append
}

********************************************************************************
********************************************************************************

erase ${data2}rais_mmc_to_mesoreg.dta
