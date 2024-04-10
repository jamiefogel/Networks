********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Main regressions
* Generates results used in Figure 4 and Table 2
* Updtaed Dec 20 2017 with Robustness Tests
********************************************************************************

clear

set more off

*global root "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Code/DixCarneiro_Kovak_2017"

global data1 "${root}Data/"
global data2 "${root}Data_Other/"
global output "${root}Results/MainEmployment/"
global empl "${root}ProcessedData_RAIS/RegionalEmployment/"

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

use ${empl}mmcEmployment_main_1986_2010, clear

keep mmc emp_dez19* emp_dez20*

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main) 
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
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

* employment pre-trends
gen delta_emp_86_90 = log(emp_dez1990) - log(emp_dez1986)

keep if delta_emp_86_90 ~= .

********************************************************************************
** EMPLOYMENT ANALYSIS -- Figure 4 / Table 2 (Panel C) Results
********************************************************************************

* The universe of formal employment is observed: no weights used in employment 
* regressions 
* Weighted regressions are estimated as a robustness exercise

set more off

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1992) - log(emp_dez1991)
* no state fixed effects / no pre-trends
reg d_ln_emp rtr_kume_main, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Employment_Main1, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / no pre-trends
reg d_ln_emp rtr_kume_main state2-state27, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Employment_Main2, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / yes pre-trends
reg d_ln_emp rtr_kume_main delta_emp_86_90 state2-state27, cluster(mesoreg)
outreg2 rtr_kume_main delta_emp_86_90 using ${output}Employment_Main3, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_emp
	gen d_ln_emp = log(emp_dez`yr') - log(emp_dez1991)
	* no state fixed effects / no pre-trends
	reg d_ln_emp rtr_kume_main, cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Employment_Main1, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_emp rtr_kume_main state2-state27, cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Employment_Main2, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_emp rtr_kume_main delta_emp_86_90 state2-state27, cluster(mesoreg)
	outreg2 rtr_kume_main delta_emp_86_90 using ${output}Employment_Main3, excel bdec(4) ctitle(`yr') append
}	

************
* Pre-Trends
************

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1987) - log(emp_dez1986)
* yes state fixed effects / yes pre-trends
reg d_ln_emp rtr_kume_main state2-state27, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Employment_Main3_PreTrends, excel bdec(4) ctitle(1992) replace

forvalues yr = 1988(1)1991{
	capture drop d_ln_emp
	gen d_ln_emp = log(emp_dez`yr') - log(emp_dez1986)
	* yes state fixed effects / yes pre-trends
	reg d_ln_emp rtr_kume_main state2-state27, cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Employment_Main3_PreTrends, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

erase ${data2}rais_mmc_to_mesoreg.dta


********************************************************************************
* Robustness 1 -- Remove Cerrado
* Dec 20 2017
********************************************************************************

set more off

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1992) - log(emp_dez1991)
* no state fixed effects / no pre-trends
reg d_ln_emp rtr_kume_main if cerrado == 0, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Employment_Main1_NoCerrado, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / no pre-trends
reg d_ln_emp rtr_kume_main state2-state27 if cerrado == 0, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Employment_Main2_NoCerrado, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / yes pre-trends
reg d_ln_emp rtr_kume_main delta_emp_86_90 state2-state27 if cerrado == 0, cluster(mesoreg)
outreg2 rtr_kume_main delta_emp_86_90 using ${output}Employment_Main3_NoCerrado, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_emp
	gen d_ln_emp = log(emp_dez`yr') - log(emp_dez1991)
	* no state fixed effects / no pre-trends
	reg d_ln_emp rtr_kume_main if cerrado == 0, cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Employment_Main1_NoCerrado, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_emp rtr_kume_main state2-state27 if cerrado == 0, cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Employment_Main2_NoCerrado, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_emp rtr_kume_main delta_emp_86_90 state2-state27 if cerrado == 0, cluster(mesoreg)
	outreg2 rtr_kume_main delta_emp_86_90 using ${output}Employment_Main3_NoCerrado, excel bdec(4) ctitle(`yr') append
}	


********************************************************************************
* Robustness 2 -- Remove North, NorthEast and CenterWest regions
* Dec 20 2017
********************************************************************************

set more off

gen region = substr(mmc,1,1)

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1992) - log(emp_dez1991)
* no state fixed effects / no pre-trends
reg d_ln_emp rtr_kume_main if inlist(region,"3","4"), cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Employment_Main1_SouthSouthEast, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / no pre-trends
reg d_ln_emp rtr_kume_main state2-state27 if inlist(region,"3","4"), cluster(mesoreg)
outreg2 rtr_kume_main using ${output}Employment_Main2_SouthSouthEast, excel bdec(4) ctitle(1992) replace
* yes state fixed effects / yes pre-trends
reg d_ln_emp rtr_kume_main delta_emp_86_90 state2-state27 if inlist(region,"3","4"), cluster(mesoreg)
outreg2 rtr_kume_main delta_emp_86_90 using ${output}Employment_Main3_SouthSouthEast, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_emp
	gen d_ln_emp = log(emp_dez`yr') - log(emp_dez1991)
	* no state fixed effects / no pre-trends
	reg d_ln_emp rtr_kume_main if inlist(region,"3","4"), cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Employment_Main1_SouthSouthEast, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_emp rtr_kume_main state2-state27 if inlist(region,"3","4"), cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}Employment_Main2_SouthSouthEast, excel bdec(4) ctitle(`yr') append
	* yes state fixed effects / yes pre-trends
	reg d_ln_emp rtr_kume_main delta_emp_86_90 state2-state27 if inlist(region,"3","4"), cluster(mesoreg)
	outreg2 rtr_kume_main delta_emp_86_90 using ${output}Employment_Main3_SouthSouthEast, excel bdec(4) ctitle(`yr') append
}
