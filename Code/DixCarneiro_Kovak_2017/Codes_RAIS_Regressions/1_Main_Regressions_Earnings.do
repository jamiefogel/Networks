********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Main regressions
* Generates results used in Figure 3 and Table 2
* Update Dec 20 2017: Robustness Tests
********************************************************************************

clear

set more off

global root "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\AER_Final_Submission_Link\ReplicationFiles\"

global data1 "${root}Data\"
global data2 "${root}Data_Other\"
global output "${root}Results\MainEarnings\"
global earnings "${root}ProcessedData_RAIS\RegionalEarnPremia\"

********************************************************************************
********************************************************************************

* December 20 2017
* mmc'c that intersect the cerrado

insheet using ${data1}Cerrado_Intersection_Data.csv, clear

gen cerrado = (objectid == 2)

keep mmc cerrado
sort mmc
by mmc: egen cerrado_int = max(cerrado)

keep mmc cerrado_int
duplicates drop

tostring mmc, replace

sort mmc

save ${data1}mmc_cerrado, replace

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

use ${earnings}mmcEarnPremia_main_1986_2010, clear

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc

save ${earnings}mmcEarnPremia_main_1986_2010_wide, replace

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main) 
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

sort mmc
merge 1:1 mmc using ${data1}dlnrent
drop _merge

sort mmc
merge 1:1 mmc using ${data1}mmc_cerrado
drop _merge

* generate state fixed effects
gen state = substr(mmc,1,2)
qui tab state, gen(state)

****************************
* Regional Tariff Reductions
****************************

gen rtr_kume_main = -rtc_kume_main

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

* earnings pre-trends
gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986

* employment pre-trends
gen delta_emp_86_90 = log(obs_dez1990) - log(obs_dez1986)

keep if delta_86_90 ~= .

********************************************************************************
** EARNINGS ANALYSIS -- Figure 3 / Table 2 (Panel A) Results
********************************************************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* no state fixed effects / no pre-trends
reg d_ln_w rtr_kume_main [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Earnings_Main1, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / no pre-trends
reg d_ln_w rtr_kume_main state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Earnings_Main2, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / yes pre-trends
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${output}Earnings_Main3, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* no state fixed effects / no pre-trends
	reg d_ln_w rtr_kume_main [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Earnings_Main1, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_w rtr_kume_main state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Earnings_Main2, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_w rtr_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90 using ${output}Earnings_Main3, excel bdec(4) ctitle(`yr') append
}	

***********************
* Pre-Trends (Figure 3)
***********************

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1987 - coeff_rem_dez1986
gen weights = 1/sqrt(SE_rem_dez1987^2+ SE_rem_dez1986^2)
* yes state fixed effects / yes pre-trends
reg d_ln_w rtr_kume_main state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Earnings_Main3_PreTrends, excel bdec(4) ctitle(1992) replace

forvalues yr = 1988(1)1991{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1986
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1986^2)
	* yes state fixed effects / yes pre-trends
	reg d_ln_w rtr_kume_main state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Earnings_Main3_PreTrends, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

********************************************************************************
* Real Effects -- Table 2 (Panel B) Results
********************************************************************************

replace d_ln_w = coeff_rem_dez2010 - coeff_rem_dez1991
replace weights = 1/sqrt(SE_rem_dez2010^2+ SE_rem_dez1991^2)

gen d_ln_real_w = d_ln_w - 0.3022 * dln_real_rent

reg d_ln_real_w rtr_kume_main [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Earnings_Real, excel bdec(4) ctitle(1991-2010 -- PT No -- State FE No) replace

reg d_ln_real_w rtr_kume_main state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Earnings_Real, excel bdec(4) ctitle(1991-2010 -- PT No -- State FE Yes) append

reg d_ln_real_w rtr_kume_main delta_86_90  state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90  using ${output}Earnings_Real, excel bdec(4) ctitle(1991-2010 -- PT Yes -- State FE Yes) append

********************************************************************************
********************************************************************************

erase ${data2}rais_mmc_to_mesoreg.dta


********************************************************************************
* Robustness 1 -- remove mmc's that intersect the cerrado
* Dec 20 2017
********************************************************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* no state fixed effects / no pre-trends
reg d_ln_w rtr_kume_main if cerrado == 0 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Earnings_Main1_NoCerrado, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / no pre-trends
reg d_ln_w rtr_kume_main state2-state27 if cerrado == 0 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Earnings_Main2_NoCerrado, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / yes pre-trends
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 if cerrado == 0 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${output}Earnings_Main3_NoCerrado, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* no state fixed effects / no pre-trends
	reg d_ln_w rtr_kume_main if cerrado == 0 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Earnings_Main1_NoCerrado, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_w rtr_kume_main state2-state27 if cerrado == 0 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Earnings_Main2_NoCerrado, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_w rtr_kume_main delta_86_90 state2-state27 if cerrado == 0 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90 using ${output}Earnings_Main3_NoCerrado, excel bdec(4) ctitle(`yr') append
}	


********************************************************************************
* Robustness 2 -- remove Center-West, North and NorthEast regions
* Dec 20 2017
********************************************************************************

set more off

gen region = substr(mmc,1,1)

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* no state fixed effects / no pre-trends
reg d_ln_w rtr_kume_main if inlist(region,"3","4") [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Earnings_Main1_SouthSouthEast, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / no pre-trends
reg d_ln_w rtr_kume_main state2-state27 if inlist(region,"3","4") [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Earnings_Main2_SouthSouthEast, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / yes pre-trends
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 if inlist(region,"3","4") [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${output}Earnings_Main3_SouthSouthEast, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* no state fixed effects / no pre-trends
	reg d_ln_w rtr_kume_main if inlist(region,"3","4") [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Earnings_Main1_SouthSouthEast, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_w rtr_kume_main state2-state27 if inlist(region,"3","4") [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Earnings_Main2_SouthSouthEast, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_w rtr_kume_main delta_86_90 state2-state27 if inlist(region,"3","4") [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90 using ${output}Earnings_Main3_SouthSouthEast, excel bdec(4) ctitle(`yr') append
}	
