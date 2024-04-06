********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates Figure 6
********************************************************************************

clear

set more off

set matsize 10000

* global root "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\ReplicationFiles\"

global data1 "${root}Data\"
global data2 "${root}\Data_Other\"
global earnings "${root}\ProcessedData_RAIS\RegionalEarnPremia\"
global employment "${root}ProcessedData_RAIS\RegionalEmployment\"
global NPLANTS "${root}ProcessedData_RAIS\Plants\"
global AggEcon "${root}Results\AgglomerationEconomies\"

********************************************************************************
********************************************************************************

* Make sure mmc variables across datasets previously generated are 
* transformed to string + rename variables whenever necessary

use ${data1}delta_mmc
tostring mmc, replace
sort mmc
save ${data1}delta_mmc, replace

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

use ${earnings}mmcEarnPremia_main_1986_2010

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc 
merge m:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main)
keep if _merge == 3
drop _merge

sort mmc
merge 1:1 mmc using ${data1}delta_mmc
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

sort mmc
merge 1:1 mmc using ${employment}mmcEmployment_main_1986_2010
drop _merge

sort mmc
merge 1:1 mmc using ${NPLANTS}NumberPlants
drop _merge

drop if mmc == "." | mmc == ""

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986

drop if delta_86_90 == .

gen rtr_kume_main = -rtc_kume_main

* Average phi in the economy
local phi = 0.54436532

***********************************************
* Behavior of Residual -- "Inferred Adjustment"
***********************************************

set more off

* Pre-trends

gen pre_trend_Residual1 = (coeff_rem_dez1990 - coeff_rem_dez1986) + rtr_kume_main + `phi'*(log(emp_dez1990) -  log(emp_dez1986)) 

* Post-shock analysis

gen Residual1 = (coeff_rem_dez1992 - coeff_rem_dez1991) + rtr_kume_main + `phi'*(log(emp_dez1992) -  log(emp_dez1991)) 

gen weights = 1/(sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2))

reg Residual1 rtr_kume_main state2-state27 pre_trend_Residual1 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main using ${AggEcon}Residual1, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	
	capture drop Residual1
	capture drop weights
	
	gen Residual1 = (coeff_rem_dez`yr' - coeff_rem_dez1991) + rtr_kume_main + `phi'*(log(emp_dez`yr') -  log(emp_dez1991)) 
	
	gen weights = 1/(sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2))
	
	reg Residual1 rtr_kume_main state2-state27 pre_trend_Residual1 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main using ${AggEcon}Residual1, excel bdec(4) ctitle(`yr') append
	
}	



********************************************************
* Response of log-number of plants, weighted by (1-zeta)
********************************************************

**************
* zeta = 0.152
**************

set more off

cap drop pre_trend_nplants
cap drop delta_nplants

local zeta = 0.152

gen pre_trend_nplants = `phi'*(1-`zeta')*(log(nplants1990) - log(nplants1986))

gen delta_nplants = `phi'*(1-`zeta')*(log(nplants1992) - log(nplants1991))

reg delta_nplants rtr_kume_main state2-state27 pre_trend_nplants, cluster(mesoreg)
outreg2 rtr_kume_main using ${AggEcon}ResidualAnalysis_zeta1, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	
	capture drop delta_nplants
	
	gen delta_nplants = `phi'*(1-`zeta')*(log(nplants`yr') - log(nplants1991))
	
	reg delta_nplants rtr_kume_main state2-state27 pre_trend_nplants, cluster(mesoreg)
	outreg2 rtr_kume_main using ${AggEcon}ResidualAnalysis_zeta1, excel bdec(4) ctitle(`yr') append
	
}	

**************
* zeta = 0.545
**************

set more off

cap drop pre_trend_nplants
cap drop delta_nplants

local zeta = 0.545

gen pre_trend_nplants = `phi'*(1-`zeta')*(log(nplants1990) - log(nplants1986))

gen delta_nplants = `phi'*(1-`zeta')*(log(nplants1992) - log(nplants1991))

reg delta_nplants rtr_kume_main state2-state27 pre_trend_nplants, cluster(mesoreg)
outreg2 rtr_kume_main using ${AggEcon}ResidualAnalysis_zeta2, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	
	capture drop delta_nplants
	
	gen delta_nplants = `phi'*(1-`zeta')*(log(nplants`yr') - log(nplants1991))
	
	reg delta_nplants rtr_kume_main state2-state27 pre_trend_nplants, cluster(mesoreg)
	outreg2 rtr_kume_main using ${AggEcon}ResidualAnalysis_zeta2, excel bdec(4) ctitle(`yr') append
	
}



************************
* zeta = (0.152+0.545)/2
************************

set more off

cap drop pre_trend_nplants
cap drop delta_nplants

local zeta = (0.152+0.545)/2

gen pre_trend_nplants = `phi'*(1-`zeta')*(log(nplants1990) - log(nplants1986))

gen delta_nplants = `phi'*(1-`zeta')*(log(nplants1992) - log(nplants1991))

reg delta_nplants rtr_kume_main state2-state27 pre_trend_nplants, cluster(mesoreg)
outreg2 rtr_kume_main using ${AggEcon}ResidualAnalysis_zeta3, excel bdec(4) ctitle(1992) replace

forvalues yr = 1993(1)2010{
	
	capture drop delta_nplants
	
	gen delta_nplants = `phi'*(1-`zeta')*(log(nplants`yr') - log(nplants1991))
	
	reg delta_nplants rtr_kume_main state2-state27 pre_trend_nplants, cluster(mesoreg)
	outreg2 rtr_kume_main using ${AggEcon}ResidualAnalysis_zeta3, excel bdec(4) ctitle(`yr') append
	
}

erase ${data2}rais_mmc_to_mesoreg.dta
