********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates Table 8
* Agglomeration Elasticity Estimates
********************************************************************************

*************************************
* Wage-based Point Estimates of kappa
*************************************

clear

set matsize 10000

set more off

* global root "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\ReplicationFiles\"

global data1      "${root}Data\"
global data2      "${root}Data_Other\"
global output     "${root}Results\AgglomerationEconomies\"
global employment "${root}ProcessedData_RAIS\RegionalEmployment\"
global earnings   "${root}ProcessedData_RAIS\RegionalEarnPremia\"

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

use ${employment}mmcEmployment_main_1986_2010, clear

sort mmc 
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main rtc_kume_t_notheta_1990_1995)
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

sort mmc
merge 1:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

gen state = substr(mmc,1,2)

* This part assumes away heterogeneity in theta's across sectors
gen rtr_kume_main = -rtc_kume_t_notheta_1990_1995

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

gen delta_earn_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986

matrix eta_estimate = J(1,1,0)
matrix beta_estimate = J(1,1,0)
matrix kappa_estimates = J(3,1,0)

**********************************
* ELASTICITY OF LOCAL LABOR SUPPLY
**********************************

* To obtain the same sample of regions as in the main specification in the paper

forvalues year = 1986(1)2010{

	drop if coeff_rem_dez`year' == .

}

* List of regions kept -- we will restrict to those regions when we estimate equation (20)
preserve
	keep mmc 
	duplicates drop
	gen use = 1
	sort mmc
	save ${data1}mmc_use, replace
restore


qui tab state, gen(state)


gen delta_L = log(emp_dez2010) - log(emp_dez1991)
gen delta_w = coeff_rem_dez2010 - coeff_rem_dez1991


gen pre_trend_L =  log(emp_dez1990) - log(emp_dez1986)
gen pre_trend_w = coeff_rem_dez1990 - coeff_rem_dez1986


gen weight = 1 / sqrt(SE_rem_dez1991^2 + SE_rem_dez2010^2)

*******************************************
* Point Estimate of eta -- Table 8, Panel A
*******************************************

ivreg2 delta_L (delta_w = rtr_kume_main) pre_trend_L state2-state26, cluster(mesoreg)
outreg2 delta_w using ${output}eta, nocons excel bdec(4) adds(K-P rk LM statistic, `e(idstat)', p-value, `e(idp)', K-P rk Wald F statistic, `e(rkf)') replace
matrix coeff = e(b)
matrix VAR = e(V)
local eta = 1 / coeff[1,1]
matrix eta_estimate[1,1] = `eta'
display `eta'

reg delta_w rtr_kume_main pre_trend_w state2-state26 [aw = weight^2], cluster(mesoreg)
matrix coeff = e(b)
local beta = coeff[1,1]
matrix beta_estimate[1,1] = `beta'

* Average phi in the economy
local phi = 0.54436532

**********************************************
* Point Estimates of kappa -- Table 8, Panel B
* Wage-based
**********************************************

********************************************************************************
* zeta = 0.152
********************************************************************************

local zeta = 0.152

local kappa = `eta' / `beta' + `eta'*(1 - `phi'*(1-`zeta')) + `phi'*`zeta'
matrix kappa_estimates[1,1] = `kappa'

display `kappa'


********************************************************************************
* zeta = 0.545
********************************************************************************

local zeta = 0.545

local kappa = `eta' / `beta' + `eta'*(1 - `phi'*(1-`zeta')) + `phi'*`zeta'
matrix kappa_estimates[2,1] = `kappa'

display `kappa'


********************************************************************************
* zeta = (0.545 + 0.152)/2
********************************************************************************

local zeta = (0.545 + 0.152)/2

local kappa = `eta' / `beta' + `eta'*(1 - `phi'*(1-`zeta')) + `phi'*`zeta'
matrix kappa_estimates[3,1] = `kappa'

display `kappa'

save ${data1}bootstrap1, replace


********************************************************************************			 
** BOOTSTRAPPED STANDARD ERRORS -- Wage-based estimates of eta
********************************************************************************

set more off

local Nrep = 1000

matrix eta = J(`Nrep',1,0)
matrix beta = J(`Nrep',1,0)
matrix kappa = J(`Nrep',3,0)

forvalues nboot = 1(1)`Nrep'{

use ${data1}bootstrap1, clear

	gen weight_sample = 0
	* set seed
	local seed_value = 1000*`nboot' + 123 
	set seed `seed_value'
	bsample _N, weight(weight_sample)

	qui ivreg2 delta_L (delta_w = rtr_kume_main) pre_trend_L state2-state26 [fw = weight_sample], cluster(mesoreg)
	matrix coeff = e(b)
	matrix eta[`nboot',1] = 1/coeff[1,1]

	gen weight_reg = weight_sample*weight^2 
	
	qui reg delta_w rtr_kume_main pre_trend_w state2-state26 [aw = weight_reg], cluster(mesoreg)
	matrix coeff = e(b)
	matrix beta[`nboot',1] = coeff[1,1]
	
	local zeta = 0.152
	matrix kappa[`nboot',1] = eta[`nboot',1] / beta[`nboot',1] + eta[`nboot',1]*(1 - `phi'*(1-`zeta')) + `phi'*`zeta'
	
	local zeta = 0.545
	matrix kappa[`nboot',2] = eta[`nboot',1] / beta[`nboot',1] + eta[`nboot',1]*(1 - `phi'*(1-`zeta')) + `phi'*`zeta'
	
	local zeta = (0.545 + 0.152)/2
	matrix kappa[`nboot',3] = eta[`nboot',1] / beta[`nboot',1] + eta[`nboot',1]*(1 - `phi'*(1-`zeta')) + `phi'*`zeta'

}

clear

svmat eta
svmat beta
svmat kappa

matrix eta = J(6,1,0)
matrix beta = J(6,1,0)
matrix kappa = J(6,3,0)

sum eta1, d
matrix eta[1,1] = `r(mean)'
matrix eta[2,1] = `r(p50)'
sort eta1
matrix eta[3,1] = eta1[25]
matrix eta[4,1] = eta1[975]
matrix eta[5,1] = eta_estimate[1,1]
matrix eta[6,1] = `r(sd)'

sum beta1, d
matrix beta[1,1] = `r(mean)'
matrix beta[2,1] = `r(p50)'
sort beta1
matrix beta[3,1] = beta1[25]
matrix beta[4,1] = beta1[975]
matrix beta[5,1] = beta_estimate[1,1]
matrix beta[6,1] = `r(sd)'

sum kappa1, d
matrix kappa[1,1] = `r(mean)'
matrix kappa[2,1] = `r(p50)'
sort kappa1
matrix kappa[3,1] = kappa1[25]
matrix kappa[4,1] = kappa1[975]
matrix kappa[5,1] = kappa_estimates[1,1]
matrix kappa[6,1] = `r(sd)'

sum kappa2, d
matrix kappa[1,2] = `r(mean)'
matrix kappa[2,2] = `r(p50)'
sort kappa2
matrix kappa[3,2] = kappa2[25]
matrix kappa[4,2] = kappa2[975]
matrix kappa[5,2] = kappa_estimates[2,1]
matrix kappa[6,2] = `r(sd)'

sum kappa3, d
matrix kappa[1,3] = `r(mean)'
matrix kappa[2,3] = `r(p50)'
sort kappa3
matrix kappa[3,3] = kappa3[25]
matrix kappa[4,3] = kappa3[975]
matrix kappa[5,3] = kappa_estimates[3,1]
matrix kappa[6,3] = `r(sd)'

clear

svmat kappa 
svmat eta
svmat beta

* kappa1: Column (1) 
* kappa2: Column (3)
* kappa3: Column (2)

save ${output}eta_kappa_bootstrap1, replace

********************************************************************************
********************************************************************************




*******************************************
* Employment-Based Point Estimates of kappa
*******************************************

********************************************************************************
********************************************************************************

clear

set more off

set matsize 11000

global data1      "${root}Data\"
global data2      "${root}Data_Other\"
global output     "${root}Results\AgglomerationEconomies\"
global employment "${root}ProcessedData_RAIS\RegionalEmployment\"
global earnings   "${root}ProcessedData_RAIS\RegionalEarnPremia\"

********************************************************************************
********************************************************************************

* Make sure mmc variables across datasets previously generated are 
* transformed to string + rename variables whenever necessary

use ${data1}tariff_chg_kume_subsibge, clear
	sort subsibge
	merge 1:1 subsibge using ${data2}subsibge_to_subsibge_rais
	keep if _merge == 3
	keep subs_ibge dlnonetariff_1990_1995
	rename dlnonetariff_1990_1995 dln_price
	sort subs_ibge
save ${data1}dln_price, replace

********************************************************************************
********************************************************************************

use ${employment}mmcEmployment_bysector_1986_2010, clear

sort mmc subs_ibge year
reshape wide emp_dez, i(mmc subs_ibge) j(year)

sort mmc 
merge m:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_t_notheta_1990_1995)
drop _merge

sort mmc
merge m:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

sort subs_ibge
merge m:1 subs_ibge using ${data1}dln_price
drop _merge

gen state = substr(mmc,1,2)

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

gen rtr_kume_main = -rtc_kume_t_notheta_1990_1995

* List of regions kept 
sort mmc
merge m:1 mmc using ${data1}mmc_use
drop _merge
keep if use == 1

erase ${data1}mmc_use.dta

matrix eta_estimate = J(1,1,0)
matrix beta_estimates = J(3,1,0)
matrix kappa_estimates = J(3,1,0)

matrix eta_estimate[1,1] = `eta'

********************************************************************************
********************************************************************************

* Aggregate individual NT sectors into a single sector

* Manufacturing/Primary/NT jobs
gen Manuf = 0
replace Manuf = 1 if subs_ibge == "4506" | subs_ibge == "4507" | subs_ibge == "4508" | ///
					 subs_ibge == "4509" | subs_ibge == "4510" | subs_ibge == "4511" | ///
					 subs_ibge == "4512" | subs_ibge == "4513" | subs_ibge == "4514" | ///
					 subs_ibge == "4515" | subs_ibge == "4516" | subs_ibge == "4517"
* Primary jobs (Agriculture/Mining)
gen Primary = 0
replace Primary = 1 if subs_ibge == "1101" | subs_ibge == "4405" 
* Traded goods jobs (Manufacturing + Primary)
gen Traded = 0
replace Traded = 1 if Manuf == 1 | Primary == 1
gen NT = 1 - Traded	

sort mmc NT
by mmc NT: egen emp_T_NT1986 = total(emp_dez1986)
by mmc NT: egen emp_T_NT1990 = total(emp_dez1990)
by mmc NT: egen emp_T_NT1991 = total(emp_dez1991)
by mmc NT: egen emp_T_NT2010 = total(emp_dez2010)

replace emp_dez1986 = emp_T_NT1986 if NT == 1
replace emp_dez1990 = emp_T_NT1990 if NT == 1
replace emp_dez1991 = emp_T_NT1991 if NT == 1
replace emp_dez2010 = emp_T_NT2010 if NT == 1

replace subs_ibge = "NT" if NT == 1

keep mmc mesoreg emp_dez1986 emp_dez1990 emp_dez1991 emp_dez2010 rtr_kume_main subs_ibge state NT Manuf Primary dln_price
duplicates drop

********************************************************************************
********************************************************************************

tab subs_ibge, gen(indFE)

forvalues i = 1(1)15{
	replace indFE`i' = indFE`i'*(1-NT)
}

gen pre_trend = log(emp_dez1990) - log(emp_dez1986)
drop if pre_trend == .

replace dln_price = -rtr_kume_main if NT == 1

local phi = 0.54436532

qui tab state, gen(state)

***********************************************************
* Test for Agglomeration Economies / Estimation of phi*zeta
***********************************************************

cap drop LHS
gen LHS = log(emp_dez2010) - log(emp_dez1991)

* All

reg LHS dln_price rtr_kume_main pre_trend state2-state26, cluster(mesoreg)
outreg2 dln_price rtr_kume_main using ${output}AggEconTest, excel bdec(4) ctitle(All) replace

matrix coeff = e(b)

local zeta = 1 / (coeff[1,1]*`phi')

display `zeta'

* Traded only

reg LHS dln_price rtr_kume_main pre_trend state2-state26 if NT == 0, cluster(mesoreg)
outreg2 dln_price rtr_kume_main using ${output}AggEconTest, excel bdec(4) ctitle(Traded) append

matrix coeff = e(b)

local zeta = 1 / (coeff[1,1]*`phi')

display `zeta'

* Manuf only

reg LHS dln_price rtr_kume_main pre_trend state2-state26 if Manuf == 1, cluster(mesoreg)
outreg2 dln_price rtr_kume_main using ${output}AggEconTest, excel bdec(4) ctitle(Manuf) append

* Agr/Mining only

reg LHS dln_price rtr_kume_main pre_trend state2-state26 if Primary == 1, cluster(mesoreg)
outreg2 dln_price rtr_kume_main using ${output}AggEconTest, excel bdec(4) ctitle(Primary) append

* Traded -- Sector fixed effects

reg LHS indFE1-indFE15 rtr_kume_main pre_trend state2-state26 if NT == 0, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}AggEconTest, excel bdec(4) ctitle(Traded / FE) append

* Manuf -- Sector fixed effects

reg LHS indFE1-indFE15 rtr_kume_main pre_trend state2-state26 if Manuf == 1, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}AggEconTest, excel bdec(4) ctitle(Manuf / FE) append

* Agr/Mining -- Sector fixed effects

reg LHS indFE1-indFE15 rtr_kume_main pre_trend state2-state26 if Primary == 1, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}AggEconTest, excel bdec(4) ctitle(Primary / FE) append

save ${data1}bootstrap2, replace

********************************************************************************
* zeta = 0.152
********************************************************************************

local zeta = 0.152

cap drop LHS
gen LHS = `phi'*`zeta'*(log(emp_dez2010) - log(emp_dez1991)) - dln_price

* Including NT sector

reg LHS rtr_kume_main pre_trend state2-state26, cluster(mesoreg)
matrix coeff = e(b)
matrix VAR = e(V)
local beta = coeff[1,1]
matrix beta_estimates[1,1] = `beta'

local kappa = `eta'*(1 - `phi'*(1-`zeta')) - (`beta'/(1-`beta'))*`phi'*`zeta'
matrix kappa_estimates[1,1] = `kappa'

display `kappa'

* Not including NT sector

reg LHS rtr_kume_main pre_trend state2-state26 if NT == 0, cluster(mesoreg)
matrix coeff = e(b)
matrix VAR = e(V)
local beta = coeff[1,1]

local kappa = `eta'*(1 - `phi'*(1-`zeta')) - (`beta'/(1-`beta'))*`phi'*`zeta'

display `kappa'

* Not including NT sector -- industry fixed effects

reg LHS rtr_kume_main indFE1-indFE15 pre_trend state2-state26 if NT == 0, cluster(mesoreg)
matrix coeff = e(b)
matrix VAR = e(V)
local beta = coeff[1,1]

local kappa = `eta'*(1 - `phi'*(1-`zeta')) - (`beta'/(1-`beta'))*`phi'*`zeta'

display `kappa'


********************************************************************************
* zeta = 0.545
********************************************************************************

local zeta = 0.545

replace LHS = `phi'*`zeta'*(log(emp_dez2010) - log(emp_dez1991)) - dln_price

* Including NT sector

reg LHS rtr_kume_main pre_trend state2-state26, cluster(mesoreg)
matrix coeff = e(b)
matrix VAR = e(V)
local beta = coeff[1,1]
matrix beta_estimates[2,1] = `beta'

local kappa = `eta'*(1 - `phi'*(1-`zeta')) - (`beta'/(1-`beta'))*`phi'*`zeta'
matrix kappa_estimates[2,1] = `kappa'

display `kappa'

* Not including NT sector

reg LHS rtr_kume_main pre_trend state2-state26 if NT == 0, cluster(mesoreg)
matrix coeff = e(b)
matrix VAR = e(V)
local beta = coeff[1,1]

local kappa = `eta'*(1 - `phi'*(1-`zeta')) - (`beta'/(1-`beta'))*`phi'*`zeta'

display `kappa'

* Not including NT sector -- industry fixed effects

reg LHS rtr_kume_main indFE1-indFE15 pre_trend state2-state26 if NT == 0, cluster(mesoreg)
matrix coeff = e(b)
matrix VAR = e(V)
local beta = coeff[1,1]

local kappa = `eta'*(1 - `phi'*(1-`zeta')) - (`beta'/(1-`beta'))*`phi'*`zeta'

display `kappa'

********************************************************************************
* zeta = (0.152 + 0.545)/2
********************************************************************************

local zeta = (0.152 + 0.545)/2

replace LHS = `phi'*`zeta'*(log(emp_dez2010) - log(emp_dez1991)) - dln_price

* Including NT sector

reg LHS rtr_kume_main pre_trend state2-state26, cluster(mesoreg)
matrix coeff = e(b)
matrix VAR = e(V)
local beta = coeff[1,1]
matrix beta_estimates[3,1] = `beta'

local kappa = `eta'*(1 - `phi'*(1-`zeta')) - (`beta'/(1-`beta'))*`phi'*`zeta'
matrix kappa_estimates[3,1] = `kappa'

display `kappa'

* Not including NT sector

reg LHS rtr_kume_main pre_trend state2-state26 if NT == 0, cluster(mesoreg)
matrix coeff = e(b)
matrix VAR = e(V)
local beta = coeff[1,1]

local kappa = `eta'*(1 - `phi'*(1-`zeta')) - (`beta'/(1-`beta'))*`phi'*`zeta'

display `kappa'

* Not including NT sector -- industry fixed effects

reg LHS rtr_kume_main indFE1-indFE15 pre_trend state2-state26 if NT == 0, cluster(mesoreg)
matrix coeff = e(b)
matrix VAR = e(V)
local beta = coeff[1,1]

local kappa = `eta'*(1 - `phi'*(1-`zeta')) - (`beta'/(1-`beta'))*`phi'*`zeta'

display `kappa'

local Nrep = 1000

matrix eta = J(`Nrep',1,0)
matrix beta = J(`Nrep',3,0)
matrix kappa = J(`Nrep',3,0)

forvalues nboot = 1(1)`Nrep'{

	use ${data1}bootstrap1, clear

	cap drop weight_sample 
	gen weight_sample = 0
	* set seed
	local seed_value = 1000*`nboot' + 123 
	set seed `seed_value'
	bsample _N, weight(weight_sample)

	qui ivreg2 delta_L (delta_w = rtr_kume_main) pre_trend_L state2-state26 [fw = weight_sample], cluster(mesoreg)
	matrix coeff = e(b)
	matrix eta[`nboot',1] = 1/coeff[1,1]

	keep mmc weight_sample
	sort mmc
	save ${data1}weight_aux, replace

	use ${data1}bootstrap2

	sort mmc
	merge m:1 mmc using ${data1}weight_aux
	drop _merge
	
	gen pre_trend_L = log(emp_dez1990) - log(emp_dez1986)

	**************
	* zeta = 0.152
	**************
	
	local zeta = 0.152 
	
	cap drop LHS
	gen LHS = `phi'*`zeta'*(log(emp_dez2010) - log(emp_dez1991)) - dln_price
	
	qui reg LHS rtr_kume_main pre_trend_L state2-state26 [fw = weight_sample], cluster(mesoreg)
	matrix coeff = e(b)
	matrix beta[`nboot',1] = coeff[1,1]
	
	matrix kappa[`nboot',1] = eta[`nboot',1]*(1 - `phi'*(1-`zeta')) - (beta[`nboot',1]/(1-beta[`nboot',1]))*`phi'*`zeta'

	**************
	* zeta = 0.545
	**************
	
	local zeta = 0.545
	
	cap drop LHS
	gen LHS = `phi'*`zeta'*(log(emp_dez2010) - log(emp_dez1991)) - dln_price
	
	qui reg LHS rtr_kume_main pre_trend_L state2-state26 [fw = weight_sample], cluster(mesoreg)
	matrix coeff = e(b)
	matrix beta[`nboot',2] = coeff[1,1]
	
	matrix kappa[`nboot',2] = eta[`nboot',1]*(1 - `phi'*(1-`zeta')) - (beta[`nboot',2]/(1-beta[`nboot',2]))*`phi'*`zeta'
	
	**************************
	* zeta = (0.152 + 0.545)/2
	**************************
	
	local zeta = (0.152+0.545)/2
	
	cap drop LHS
	gen LHS = `phi'*`zeta'*(log(emp_dez2010) - log(emp_dez1991)) - dln_price
	
	qui reg LHS rtr_kume_main pre_trend_L state2-state26 [fw = weight_sample], cluster(mesoreg)
	matrix coeff = e(b)
	matrix beta[`nboot',3] = coeff[1,1]
	
	matrix kappa[`nboot',3] = eta[`nboot',1]*(1 - `phi'*(1-`zeta')) - (beta[`nboot',3]/(1-beta[`nboot',3]))*`phi'*`zeta'
	
}

clear

svmat eta
svmat beta
svmat kappa

matrix eta = J(6,1,0)
matrix beta = J(6,3,0)
matrix kappa = J(6,3,0)

sum eta1, d
matrix eta[1,1] = `r(mean)'
matrix eta[2,1] = `r(p50)'
sort eta1
matrix eta[3,1] = eta1[25]
matrix eta[4,1] = eta1[975]
matrix eta[5,1] = eta_estimate[1,1]
matrix eta[6,1] = `r(sd)'

sum beta1, d
matrix beta[1,1] = `r(mean)'
matrix beta[2,1] = `r(p50)'
sort beta1
matrix beta[3,1] = beta1[25]
matrix beta[4,1] = beta1[975]
matrix beta[5,1] = beta_estimates[1,1]
matrix beta[6,1] = `r(sd)'

sum beta2, d
matrix beta[1,2] = `r(mean)'
matrix beta[2,2] = `r(p50)'
sort beta2
matrix beta[3,2] = beta2[25]
matrix beta[4,2] = beta2[975]
matrix beta[5,2] = beta_estimates[2,1]
matrix beta[6,2] = `r(sd)'

sum beta3, d
matrix beta[1,3] = `r(mean)'
matrix beta[2,3] = `r(p50)'
sort beta3
matrix beta[3,3] = beta3[25]
matrix beta[4,3] = beta3[975]
matrix beta[5,3] = beta_estimates[3,1]
matrix beta[6,3] = `r(sd)'

sum kappa1, d
matrix kappa[1,1] = `r(mean)'
matrix kappa[2,1] = `r(p50)'
sort kappa1
matrix kappa[3,1] = kappa1[25]
matrix kappa[4,1] = kappa1[975]
matrix kappa[5,1] = kappa_estimates[1,1]
matrix kappa[6,1] = `r(sd)'

sum kappa2, d
matrix kappa[1,2] = `r(mean)'
matrix kappa[2,2] = `r(p50)'
sort kappa2
matrix kappa[3,2] = kappa2[25]
matrix kappa[4,2] = kappa2[975]
matrix kappa[5,2] = kappa_estimates[2,1]
matrix kappa[6,2] = `r(sd)'

sum kappa3, d
matrix kappa[1,3] = `r(mean)'
matrix kappa[2,3] = `r(p50)'
sort kappa3
matrix kappa[3,3] = kappa3[25]
matrix kappa[4,3] = kappa3[975]
matrix kappa[5,3] = kappa_estimates[3,1]
matrix kappa[6,3] = `r(sd)'

clear

svmat kappa 
svmat eta
svmat beta

* kappa1: Column (1) 
* kappa2: Column (3)
* kappa3: Column (2)

save ${output}eta_kappa_bootstrap2, replace

********************************************************************************
********************************************************************************

erase ${data1}bootstrap1.dta
erase ${data1}bootstrap2.dta
erase ${data1}weight_aux.dta
erase ${data1}dln_price.dta
erase ${data2}rais_mmc_to_mesoreg.dta
