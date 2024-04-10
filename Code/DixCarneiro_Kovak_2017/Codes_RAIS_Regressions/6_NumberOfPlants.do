********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates Results in Figure 7
********************************************************************************

clear

set more off

* global root "C:/Users/rd123/Dropbox/DixCarneiroKovakRodriguez/ReplicationFiles/"

global data1 "${root}Data/"
global data2 "${root}Data_Other/"
global data3 "${root}ProcessedData_RAIS/Plants/"
global result "${root}Results/NPlants/"
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

******************
* Number of Plants
******************

use ${data3}NumberPlants, clear

sort mmc 
merge m:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main)
keep if _merge == 3
drop _merge

sort mmc
merge m:1 mmc using ${data2}rais_mmc_to_mesoreg			 
drop _merge

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

gen state = substr(mmc,1,2)

set more off

* Keep same set of regions as those used for employment and earnings regressions
keep if coeff_rem_dez1986 ~= . & coeff_rem_dez1990 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

gen rtr_kume_main = -rtc_kume_main

gen delta_86_90 = log(nplants1990/nplants1986)

gen delta = log(nplants1992/nplants1991)
xi: reg delta rtr_kume_main delta_86_90 i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${result}NPLANTS, excel bdec(5) ctitle(1992) replace

forvalues year = 1993/2010{
replace delta = log(nplants`year'/nplants1991)
xi: reg delta rtr_kume_main delta_86_90 i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${result}NPLANTS, excel bdec(5) ctitle(`year') append
}

********************************
* Number of Plants -- Pre-Trends
********************************

replace delta = log(nplants1987/nplants1986)
xi: reg delta rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}NPLANTS_PreTrends, excel bdec(5) ctitle(1992) replace

forvalues year = 1988/1991{
replace delta = log(nplants`year'/nplants1986)
xi: reg delta rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}NPLANTS_PreTrends, excel bdec(5) ctitle(`year') append
}

****************
* Avg Plant Size
****************

use ${data3}PlantSize, clear

sort mmc 
merge m:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main)
keep if _merge == 3
drop _merge

sort mmc
merge m:1 mmc using ${data2}rais_mmc_to_mesoreg			 
drop _merge

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

gen state = substr(mmc,1,2)

set more off

* Keep same set of regions as those used for employment and earnings regressions
keep if coeff_rem_dez1986 ~= . & coeff_rem_dez1990 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

gen rtr_kume_main = -rtc_kume_main

gen delta2_86_90 = log(avg_plant_size1990/avg_plant_size1986)

gen delta2 = log(avg_plant_size1992/avg_plant_size1991)
xi: reg delta2 rtr_kume_main delta2_86_90 i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main delta2_86_90 using ${result}AVG_PLANT_SIZE, excel bdec(5) ctitle(1992) replace

forvalues year = 1993/2010{
replace delta2 = log(avg_plant_size`year'/avg_plant_size1991)
xi: reg delta2 rtr_kume_main delta2_86_90 i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main delta2_86_90 using ${result}AVG_PLANT_SIZE, excel bdec(5) ctitle(`year') append
}

*****************************
* Avg Plant Size -- PreTrends
*****************************

replace delta2 = log(avg_plant_size1987/avg_plant_size1986)
xi: reg delta2 rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}AVG_PLANT_SIZE_PreTrends, excel bdec(5) ctitle(1992) replace

forvalues year = 1988/1991{
replace delta2 = log(avg_plant_size`year'/avg_plant_size1986)
xi: reg delta2 rtr_kume_main i.state, vce(cluster mesoreg)
outreg2 rtr_kume_main using ${result}AVG_PLANT_SIZE_PreTrends, excel bdec(5) ctitle(`year') append
}

********************************************************************************
********************************************************************************

erase ${data2}rais_mmc_to_mesoreg.dta
