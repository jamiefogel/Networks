********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* This do file generates results reported on Table 5
********************************************************************************

clear

set more off

* global root "C:/Users/rd123/Dropbox/DixCarneiroKovakRodriguez/ReplicationFiles/"

global data1 "${root}Data/"
global data2 "${root}Data_Other/"
global data3 "${root}ProcessedData_RAIS/RegionalEarnPremia_WorkerFE/"
global output "${root}Results/Earnings_WorkerFE/"

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

* We first generate list of mmc's used in all specifications controlling for individual
* fixed effects in the first stage

use ${data3}RegionYearFE_all_est_withse, clear

keep mmc year RegionYearFE_nlhdfe RegionYearFE_nl_se

rename RegionYearFE_nlhdfe coeff_rem_dez
rename RegionYearFE_nl_se SE_rem_dez

reshape wide coeff_rem_dez SE_rem_dez, i(mmc) j(year)

forvalues yr = 1986(1)2010{
	keep if coeff_rem_dez`yr' ~= .
}

forvalues yr = 1986(1)2010{
	replace SE_rem_dez`yr' = 0 if SE_rem_dez`yr' == .
}

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

keep mmc
duplicates drop
save ${data3}IndivFixedEffectReg, replace

********************************************************************************
********************************************************************************

*************************************
* Fixed returns to unobserved ability
* Table 5 Panel B
*************************************

********************************************************************************
********************************************************************************

use ${data3}mmcEarnPremia_wrkrFE_1986_2010

replace feffse = 0 if feffse == . 
replace feffse = . if mmc_year_obs == 1

drop mmc_year_obs

rename feff coeff_rem_dez
rename feffse SE_rem_dez

reshape wide coeff_rem_dez SE_rem_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main)
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

sort mmc
merge 1:1 mmc using ${data3}IndivFixedEffectReg
keep if _merge == 3

gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986

set more off

capture drop d_ln_w 
capture drop weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_86_90  state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90  using ${output}Table5B, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop 
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main delta_86_90  state2-state27  [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90  using ${output}Table5B, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

********************************************
* Time-varying returns to unobserved ability
* Table 5 Panel C
********************************************

********************************************************************************
********************************************************************************

use ${data3}RegionYearFE_all_est_withse, clear

keep mmc year RegionYearFE_nlhdfe RegionYearFE_nl_se

rename RegionYearFE_nlhdfe coeff_rem_dez
rename RegionYearFE_nl_se SE_rem_dez

reshape wide coeff_rem_dez SE_rem_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main)
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

sort mmc
merge 1:1 mmc using ${data3}IndivFixedEffectReg
keep if _merge == 3

gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986
gen weights = 1/sqrt(SE_rem_dez1986^2+ SE_rem_dez1990^2)

********************************************************************************
********************************************************************************

set more off

capture drop d_ln_w 
capture drop weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_86_90  state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90  using ${output}Table5C, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop 
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main delta_86_90  state2-state27  [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90  using ${output}Table5C, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

erase ${data3}IndivFixedEffectReg.dta
erase ${data2}rais_mmc_to_mesoreg.dta
