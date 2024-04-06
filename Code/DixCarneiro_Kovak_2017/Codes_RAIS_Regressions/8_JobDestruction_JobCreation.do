********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates Figure 9
********************************************************************************

clear

* global root "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\ReplicationFiles\"

global data1 "${root}Data\"
global data2 "${root}Data_Other\"
global data3 "${root}ProcessedData_RAIS\JobDestruction_JobCreation\"
global result "${root}Results\JobDestruction_JobCreation\"
global earnings "${root}ProcessedData_RAIS\RegionalEarnPremia\"

********************************************************************************
********************************************************************************

* Make sure mmc variables across datasets previously generated are 
* transformed to string + rename variables whenever necessary

use ${data1}rtc_kume
tostring mmc, replace
sort mmc
save ${data1}rtc_kume, replace

use ${data1}dlnrent
tostring mmc, replace
sort mmc
save ${data1}dlnrent, replace

use ${data2}mmc_1991_2010_to_c_mesoreg
tostring mmc, replace
rename c_mesoreg mesoreg
sort mmc
save ${data2}rais_mmc_to_mesoreg, replace

********************************************************************************
********************************************************************************

use ${data3}JobCreationDestruction_Base1986

sort mmc
merge 1:1 mmc using ${data3}JobCreationDestruction_Base1991
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

*****************
* Job Destruction
*****************

set more off

cap drop dest

gen dest_86_90 = log(NEG1990)

gen dest = log(NEG1992)
xi: reg dest rtr_kume_main dest_86_90  i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main dest_86_90 using ${result}JobDestruction, excel bdec(5) ctitle(1992) replace

forvalues year = 1993/2010{
replace dest = log(NEG`year')
xi: reg dest rtr_kume_main dest_86_90  i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main dest_86_90 using ${result}JobDestruction, excel bdec(5) ctitle(`year') append
}

*******************************
* Job Destruction -- Pre-Trends
*******************************

replace dest = log(NEG1987)
xi: reg dest rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}JobDestruction_PreTrends, excel bdec(5) ctitle(1992) replace

forvalues year = 1988/1991{
replace dest = log(NEG`year')
xi: reg dest rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}JobDestruction_PreTrends, excel bdec(5) ctitle(`year') append
}

**************
* Job Creation
**************

set more off

cap drop creat

gen creat_86_90 = log(POS1990)

gen creat = log(POS1992)
xi: reg creat rtr_kume_main creat_86_90 i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main creat_86_90 using ${result}JobCreation, excel bdec(5) ctitle(1992) replace

forvalues year = 1993/2010{
replace creat = log(POS`year')
xi: reg creat rtr_kume_main creat_86_90 i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main creat_86_90 using ${result}JobCreation, excel bdec(5) ctitle(`year') append
}

****************************
* Job Creation -- Pre-Trends
****************************

replace creat = log(POS1987)
xi: reg creat rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}JobCreation_PreTrends, excel bdec(5) ctitle(1992) replace

forvalues year = 1988/1991{
replace creat = log(POS`year')
xi: reg creat rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}JobCreation_PreTrends, excel bdec(5) ctitle(`year') append
}

********************************************************************************
********************************************************************************

erase ${data2}rais_mmc_to_mesoreg.dta

