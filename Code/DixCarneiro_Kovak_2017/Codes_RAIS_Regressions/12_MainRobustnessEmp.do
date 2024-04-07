********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates results in Table B7
********************************************************************************

clear

set more off

* global root "C:/Users/rd123/Dropbox/DixCarneiroKovakRodriguez/ReplicationFiles/"

global data1 "${root}Data/"
global data2 "${root}Data_Other/"
global output "${root}Results/RobustnessEmp/"
global employment "${root}ProcessedData_RAIS/RegionalEmployment/"

********************************************************************************
********************************************************************************

* Make sure mmc variables across datasets previously generated are 
* transformed to string + rename variables whenever necessary

use ${data1}rtc_kume
tostring mmc, replace
sort mmc
save ${data1}rtc_kume, replace

use ${data1}frtc_kume
tostring mmc, replace
sort mmc
save ${data1}frtc_kume, replace

use ${data2}mmc_1991_2010_to_c_mesoreg
tostring mmc, replace
rename c_mesoreg mesoreg
sort mmc
save ${data2}rais_mmc_to_mesoreg, replace

use ${data2}mmc1970_to_c_mesoreg1970
tostring mmc1970, replace
rename mmc1970 mmc
rename c_mesoreg1970 mesoreg
save ${data2}rais_mmc1970_to_mesoreg, replace

use ${data1}rtc_kume_mmc1970
tostring mmc1970, replace
rename mmc1970 mmc
sort mmc
save ${data1}rtc_kume_mmc1970_2, replace

use ${data1}dln_employment
tostring mmc1970, replace
rename mmc1970 mmc
sort mmc
save ${data1}dln_employment_2, replace

********************************************************************************
********************************************************************************

use ${employment}mmcEmployment_mmc1970_1986_2010, clear

sort mmc
merge 1:1 mmc using ${data1}rtc_kume_mmc1970_2
drop _merge

sort mmc
merge 1:1 mmc using  ${data2}rais_mmc1970_to_mesoreg
drop _merge

sort mmc
merge 1:1 mmc using  ${data1}dln_employment_2
drop _merge

* Drop Manaus
drop if mmc == "13901"
* Fernando de Noronha
drop if mmc == "26019"

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

* RAIS pre-trends
gen delta_emp_86_90 = log(emp_dez1990) -  log(emp_dez1986)
* Census pre-trends
gen delta_emp_70_80 = ln_all1980 - ln_all1970
gen delta_emp_80_91 = ln_prev_formemp1991 - ln_prev_formemp1980

keep if delta_emp_86_90 ~= .

****************************************
* Longer Pre-Trends -- Table B7, Panel B
****************************************

* Specification 1: OLS with delta_emp_70_80 and delta_emp_80_91

set more off

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1992) -  log(emp_dez1991) 
* regression with state fixed effects
reg d_ln_emp rtr_kume_main delta_emp_70_80 delta_emp_80_91 delta_emp_86_90 state2-state27 , cluster(mesoreg)
outreg2 rtr_kume_main delta_emp_70_80 delta_emp_80_91 delta_emp_86_90 using ${output}RobustnessEarn_LongPT1, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_emp 
	gen d_ln_emp = log(emp_dez`yr') -  log(emp_dez1991) 
	* regression with state fixed effects
	reg d_ln_emp rtr_kume_main delta_emp_70_80 delta_emp_80_91 delta_emp_86_90 state2-state27 , cluster(mesoreg)
	outreg2 rtr_kume_main delta_emp_70_80 delta_emp_80_91 delta_emp_86_90 using ${output}RobustnessEarn_LongPT1, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

****************************************
* Longer Pre-Trends -- Table B7, Panel C
****************************************

* Specification 2: OLS with delta_emp_70_80 and delta_emp_80_91, base year 1992

set more off

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1993) -  log(emp_dez1992) 
* regression with state fixed effects
reg d_ln_emp rtr_kume_main delta_emp_70_80 delta_emp_80_91 delta_emp_86_90 state2-state27, cluster(mesoreg)
outreg2 rtr_kume_main delta_emp_70_80 delta_emp_80_91 delta_emp_86_90 using ${output}RobustnessEarn_LongPT2, excel bdec(4) ctitle(1993) replace


forvalues yr = 1994(1)2010{
	capture drop d_ln_emp 
	gen d_ln_emp = log(emp_dez`yr') -  log(emp_dez1992) 
	reg d_ln_emp rtr_kume_main delta_emp_70_80 delta_emp_80_91 delta_emp_86_90 state2-state27, cluster(mesoreg)
	outreg2 rtr_kume_main delta_emp_70_80 delta_emp_80_91 delta_emp_86_90 using ${output}RobustnessEarn_LongPT2, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************


********************************************************************************
********************************************************************************

use ${employment}mmcEmployment_main_1986_2010, clear

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main rec_kume_main rtc_kume_nt_theta_1990_1995) 
drop _merge

sort mmc
merge 1:1 mmc using ${data1}frtc_kume
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main
gen frtr_kume_main = -frtc_kume_main
replace rec_kume_main = -rec_kume_main
gen rtr_kume_main_NT = -rtc_kume_nt_theta_1990_1995

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

gen delta_emp_86_90 = log(emp_dez1990) -  log(emp_dez1986)

keep if delta_emp_86_90 ~= .

*******************************************************************
* RTR using formal employment industry weights -- Table B7, Panel D
*******************************************************************

set more off

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1992) -  log(emp_dez1991) 
reg d_ln_emp frtr_kume_main delta_emp_86_90 state2-state27, cluster(mesoreg)
outreg2 frtr_kume_main delta_emp_86_90  using ${output}RobustnessEarn_FRTC, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_emp
	gen d_ln_emp = log(emp_dez`yr') -  log(emp_dez1991) 
	reg d_ln_emp frtr_kume_main delta_emp_86_90 state2-state27, cluster(mesoreg)
	outreg2 frtr_kume_main delta_emp_86_90  using ${output}RobustnessEarn_FRTC, excel bdec(4) ctitle(`yr') append
}

********************************************************************************
********************************************************************************

***************************************************************
*  RTR using effective rates of protection -- Table B7, Panel E
***************************************************************

set more off

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1992) -  log(emp_dez1991) 
reg d_ln_emp rec_kume_main delta_emp_86_90 state2-state27, cluster(mesoreg)
outreg2 rec_kume_main delta_emp_86_90  using ${output}RobustnessEarn_ERP, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_emp
	gen d_ln_emp = log(emp_dez`yr') -  log(emp_dez1991) 
	reg d_ln_emp rec_kume_main delta_emp_86_90 state2-state27, cluster(mesoreg)
	outreg2 rec_kume_main delta_emp_86_90  using ${output}RobustnessEarn_ERP, excel bdec(4) ctitle(`yr') append
}

********************************************************************************
********************************************************************************


*******************************************************************
*  RTR including zero nontradable price change -- Table B7, Panel F
*******************************************************************

set more off

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1992) -  log(emp_dez1991) 
reg d_ln_emp rtr_kume_main_NT delta_emp_86_90 state2-state27, cluster(mesoreg)
outreg2 rtr_kume_main_NT delta_emp_86_90  using ${output}RobustnessEarn_RTR_NT, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_emp
	gen d_ln_emp = log(emp_dez`yr') -  log(emp_dez1991) 
	reg d_ln_emp rtr_kume_main_NT delta_emp_86_90 state2-state27, cluster(mesoreg)
	outreg2 rtr_kume_main_NT delta_emp_86_90  using ${output}RobustnessEarn_RTR_NT, excel bdec(4) ctitle(`yr') append
}

********************************************************************************
********************************************************************************


*********************************************************
* Weighted by 1991 formal employment -- Table B7, Panel J
*********************************************************

set more off

capture drop weight
gen weights = emp_dez1991

capture drop d_ln_emp
gen d_ln_emp = log(emp_dez1992) -  log(emp_dez1991) 
reg d_ln_emp rtr_kume_main delta_emp_86_90 state2-state27 [aw = weights], cluster(mesoreg)
outreg2 rtr_kume_main delta_emp_86_90  using ${output}RobustnessEarn_WeightSize, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	capture drop d_ln_emp
	gen d_ln_emp = log(emp_dez`yr') -  log(emp_dez1991) 
	reg d_ln_emp rtr_kume_main delta_emp_86_90 state2-state27 [aw = weights], cluster(mesoreg)
	outreg2 rtr_kume_main delta_emp_86_90  using ${output}RobustnessEarn_WeightSize, excel bdec(4) ctitle(`yr') append
}

********************************************************************************
********************************************************************************

erase ${data2}rais_mmc_to_mesoreg.dta
erase ${data2}rais_mmc1970_to_mesoreg.dta
erase ${data1}rtc_kume_mmc1970_2.dta
erase ${data1}dln_employment_2.dta
