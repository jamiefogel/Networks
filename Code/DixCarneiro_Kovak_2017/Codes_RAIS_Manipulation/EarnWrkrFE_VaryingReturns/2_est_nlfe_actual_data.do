
set more off
set trace off
capture log close

capture set mem 20g

global root "C:/Users/rd123/Dropbox/DixCarneiroKovakRodriguez/ReplicationFiles/"

global data1 "${root}ProcessedData_RAIS/Panel_1986_2010/"
global data2 "${root}Data_Other/"
global output "${root}ProcessedData_RAIS/RegionalEarnPremia_WorkerFE/Temp/"
global logs "${root}ProcessedData_RAIS/RegionalEarnPremia_WorkerFE/Temp/Logs/"

********************************************************************************
********************************************************************************

use ${data2}mmc_1991_2010_to_c_mesoreg, clear
tostring mmc, replace
rename c_mesoreg mesoreg
sort mmc
save ${data2}rais_mmc_to_mesoreg, replace

use ${data2}rais_codemun_to_mmc_1991_2010, clear
tostring mmc, replace
sort mmc
save ${data2}rais_codemun_to_mmc_1991_2010, replace

********************************************************************************
********************************************************************************

log using "${logs}2_est_nlfe_actual_data.log", replace

***************************
***                     ***
*** Spcifiy what to run ***
***                     ***
***************************

* How many iterations to try (max)?
scalar iter_limit = 500

* Tolerance for convergence?
scalar tol = 1e-4

* Which data file should be loaded to run the estimation?
local datafile "${data1}new_panel.dta"
* How to save the name with the predicted values, estimates and residuals?
* (required for the standard errors)
local predictfile "${output}new_panel_predicted.dta"


*********************************
***                        	  ***
*** Load and prepare the data ***
***                           ***
*********************************

use year pis age educ_25 gender subs_ibge cnpj codemun real_rem_dez using `datafile', clear
	
duplicates drop

*Drop all observations with a NONFORMAL sector	
drop if subs_ibge == "NONFORMAL"

*Keep indivuals with positive and non-missing real_rem_dez observation
keep if real_rem_dez > 0 & real_rem_dez ~= .

*Keep the highest paying job for each individual
gsort + pis + year - real_rem_dez
by pis year: gen obs = _n
keep if obs == 1
drop obs

display _N

preserve
	keep pis year
	duplicates drop
	display _N
restore	

gen log_rem_dez = log(real_rem_dez)

* Drop individuals that start in RAIS with age less than 16
sort pis
by pis: egen min_age = min(age)
drop if min_age < 16
* Only keep observations of working age individuals
keep if age >= 18 & age <= 64
drop if subs_ibge == "5719"
drop if subs_ibge == "9999"

preserve
	keep pis year
	duplicates drop
	sort pis year
	by pis: gen obs = _n
	by pis: egen max_obs = max(obs)
	keep if obs == 1
	drop obs
	tab max_obs
	sum max_obs, d
restore

*Merge with mmc converters
sort codemun
merge m:1 codemun using ${data2}rais_codemun_to_mmc_1991_2010
keep if _merge == 3
drop _merge

*Obtain mesoregion information
sort mmc
merge m:1 mmc using ${data2}rais_mmc_to_mesoreg
keep if _merge == 3
drop _merge

drop if trim(mmc) == "." | trim(mmc) == ""

egen indiv = group(pis)

*Generate mmc-year and industry-year categorical variables
egen mmc_year = group(mmc year)
qui sum mmc_year, meanonly
local max_mmc_year = r(max)
display `max_mmc_year' // number of groups

sort year subs_ibge 
egen subs_ibge_year = group(year subs_ibge)
qui sum subs_ibge_year, meanonly
local max_subs_ibge_year = r(max)
display `max_subs_ibge_year' // number of groups
qui tab subs_ibge_year, gen(subs_ibge_year)

gen     age_aux = 1 if age >= 18 & age <= 24
replace age_aux = 2 if age >= 25 & age <= 29
replace age_aux = 3 if age >= 30 & age <= 39
replace age_aux = 4 if age >= 40 & age <= 49
replace age_aux = 5 if age >= 50 & age <= 64

tab age_aux, gen(cat_age)

gen age_1 = (age-25)
gen age_2 = (age-25)^2

duplicates drop
display _N

******************************
***                        ***
*** NL iterative procedure ***
***                        ***
******************************s

* ---------------- *
* --- Programs --- *
* ---------------- *

* Renew the estimates of delta and PersonFE

capture program drop RenewDeltaPersonFE
program define RenewDeltaPersonFE
	* Arguments
	args eps delta0 PersonFE0 PersonID year first_year if_cond
	
	* Declare temporary variables
	tempvar PersonFE1 PersonFE1_num_aux PersonFE1_denom_aux PersonFE1_num PersonFE1_denom
	tempvar delta1 delta1_num_aux delta1_denom_aux delta1_num delta1_denom
	tempvar delta1_PersonFE1
	
	* Renew Person FE (using old deltas)
	qui gen `PersonFE1_num_aux' = `eps'*`delta0' `if_cond'
	qui gen `PersonFE1_denom_aux' = `delta0'^2 `if_cond'

	qui egen `PersonFE1_num' = sum(`PersonFE1_num_aux') `if_cond', by(`PersonID')
	qui egen `PersonFE1_denom' = sum(`PersonFE1_denom_aux') `if_cond', by(`PersonID')

	qui gen `PersonFE1' = `PersonFE1_num' / `PersonFE1_denom' `if_cond'

	* Renew delta (using new FE)
	qui gen `delta1_num_aux' = `eps'*`PersonFE1'  `if_cond'
	qui gen `delta1_denom_aux' = `PersonFE1'^2  `if_cond'

	qui egen `delta1_num' = sum(`delta1_num_aux')  `if_cond', by(`year')
	qui egen `delta1_denom' = sum(`delta1_denom_aux')  `if_cond', by(`year')

	qui gen `delta1' = `delta1_num' / `delta1_denom'  `if_cond'

	* Renew delta * PersonFE
	qui gen `delta1_PersonFE1' = `delta1'*`PersonFE1'  `if_cond'
	
	* Clean up if some variables existed before
	capture drop PersonFE1
	capture drop delta1
	capture drop delta1_PersonFE1
	
	* Normalize delta for the first year in the data to be 1
	* (otherwise things are not identified)	
	sum `delta1' if `year'==`first_year', meanonly
	local delta_fixed = `r(mean)'
	
	qui gen delta1 = `delta1'/`delta_fixed'  `if_cond'
	qui gen PersonFE1 = `PersonFE1'*`delta_fixed' `if_cond'
	qui gen delta1_PersonFE1 = `delta1_PersonFE1' `if_cond'	
end

* Drop singlton observations in the data

capture program drop DropSingletons
program define DropSingletons
	* Temporary variables
	tempvar more_than2_0 more_than2_1
	tempvar n_per_pers n_per_mmc_year n_per_ind_year

	local dist_N = 1
	gen `more_than2_1' = 1
	qui count if `more_than2_1'==1
	local n00 = `r(N)'
	qui gen `more_than2_0' = 1
	while (`dist_N'>0) {
		qui replace `more_than2_0' = `more_than2_1'
	
		capture drop `more_than2_1'
		capture drop `n_per_pers' `n_per_mmc_year' `n_per_ind_year'
	
		bysort indiv: egen `n_per_pers' = sum(`more_than2_0')
		bysort mmc_year: egen `n_per_mmc_year' = sum(`more_than2_0')
		bysort subs_ibge_year: egen `n_per_ind_year' = sum(`more_than2_0')

		qui gen `more_than2_1' = 1 if (`n_per_pers'>=2 & `n_per_mmc_year'>=2 & `n_per_ind_year'>=2)
	
		qui count if `more_than2_0'==1
		local n0 = `r(N)'
		qui count if `more_than2_1'==1
		local n1 = `r(N)'

		local dist_N = abs(`n0' - `n1')
	}
	
	local n_dropped = `n00' - `n1'
	di "Flagged `n1' singleton observations"
	qui gen insample = 1 if `more_than2_1'==1
end

* ------------------------- *
* --- Initialize values --- *
* ------------------------- *

	* Mark sample to be used
	DropSingletons
	local if_cond if insample==1

	* Inialize the estimates
		qui reghdfe log_rem_dez cat_age2 cat_age3 cat_age4 cat_age5 `if_cond', ///
				absorb(PersonFE_init = indiv RegYearFE_init = mmc_year IndYearFE_init = subs_ibge_year)
		gen delta_init = 1 `if_cond'

* initialize distance and iteration number
local min_dist = 1
local iter = 0
local RSS1 = 0

qui sum year 
local first_year = `r(min)'

* set up a file to track convergence
file open track_progress using "${logs}nlhdfe_conv_track.csv", write replace
file write track_progress "iter,RSS1,RSSdist,RegYearFEdist,IndYearFEdist,PersonFEdist,delta_dist,delta_PersonFE_dist,deltaPersonFE_coef,RegYearFE_coef,IndYearFE_coef" _n
file close track_progress

* ---------------------------- *
* --- Iterative procedure ---- *
* ---------------------------- *

while (`min_dist' > tol & `iter' < iter_limit) {

	* ---------------------------------------------- *
	* --- Renew the previous iteration estimates --- *
	* ---------------------------------------------- *
	capture replace PersonFE0 = PersonFE1 `if_cond'
	capture gen PersonFE0 = PersonFE_init `if_cond'
	
	capture replace delta0_PersonFE0 = delta1_PersonFE1 `if_cond'
	capture gen delta0_PersonFE0 = delta_init*PersonFE_init `if_cond'
	
	capture replace delta0 = delta1 `if_cond'
	capture gen delta0 = delta_init `if_cond'

	capture replace RegYearFE0 = RegYearFE1 `if_cond'
	capture gen RegYearFE0 = RegYearFE_init `if_cond'
	
	capture replace IndYearFE0 = IndYearFE1 `if_cond'
	capture gen IndYearFE0 = IndYearFE_init	`if_cond'

	local RSS0 = `RSS1'
	local iter = `iter' + 1

	* Drop the variables for the current iteration estimates
	capture drop RegYearFE1
	capture drop IndYearFE1
	capture drop delta1
	capture drop log_rem_dez_hat
	capture drop eps1
	capture drop PersonFE1
	capture drop delta1_PersonFE1
	capture drop delta1
	
	* ------------------------------------ *
	* --- New estimates for RegYear FE --- *
	* ------------------------------------ *
	

	qui reghdfe log_rem_dez delta0_PersonFE0 IndYearFE0 cat_age2 cat_age3 cat_age4 cat_age5 `if_cond', absorb(RegYearFE1 = mmc_year)
	
	* ------------------------------------ *
	* --- New estimates for IndYear FE --- *
	* ------------------------------------ *


	qui reghdfe log_rem_dez delta0_PersonFE0 RegYearFE1 cat_age2 cat_age3 cat_age4 cat_age5 `if_cond', absorb(IndYearFE1 = subs_ibge_year)
	
	* ------------------------------------------ *
	* --- New estimates for delta * PersonFE --- *
	* ------------------------------------------ *
	qui reg log_rem_dez delta0_PersonFE0 RegYearFE1 IndYearFE1 cat_age2 cat_age3 cat_age4 cat_age5 `if_cond'
	* Coef at delta*PersonFE (should be 1 when converged)
	matrix estimates = e(b)
	qui local coef_FE = estimates[1,1]
	qui local coef_RegFE = estimates[1,2]
	qui local coef_IndFE = estimates[1,3]

	* Calculate the new RSS
	local RSS1 = `e(rss)'
	* Predict FE and earnings	
	qui predict log_rem_dez_hat if e(sample), xb
	qui replace log_rem_dez_hat = log_rem_dez_hat - `coef_FE'*delta0_PersonFE0  `if_cond'
	qui gen eps1 = log_rem_dez - log_rem_dez_hat `if_cond'

	drop log_rem_dez_hat

	* ------------------------------------------ *
	* --- Renew estimates for delta * PersonFE --- *
	* ------------------------------------------ *
	
	RenewDeltaPersonFE eps1 delta0 PersonFE0 indiv year `first_year' "`if_cond'"

	* ---------------------------------------- *
	* --- Difference between the estimates --- *
	* ---------------------------------------- *

	* RSS Distance
	local RSSdist = abs(`RSS1' - `RSS0')
	
	qui gen PersonFE_dif = (PersonFE0 - PersonFE1)^2 `if_cond'
	qui gen IndYearFE_dif = (IndYearFE0 - IndYearFE1)^2 `if_cond'
	qui gen RegYearFE_dif = (RegYearFE0 - RegYearFE1)^2 `if_cond'		
	qui gen delta_dif = (delta0 - delta1)^2 `if_cond'
	qui gen delta_PersonFE_dif = (delta0_PersonFE0 - delta1_PersonFE1)^2 `if_cond'

	qui sum RegYearFE_dif
	local RegYearFEdist = `r(sum)'
	qui sum IndYearFE_dif
	local IndYearFEdist = `r(sum)'	
	qui sum PersonFE_dif
	local PersonFEdist = `r(sum)'
	qui sum delta_dif
	local delta_dist = `r(sum)'
	qui sum delta_PersonFE_dif
	local deltaPersonFE_dist = `r(sum)'
	
	local x_dist = max(`RegYearFEdist',`IndYearFEdist',`PersonFEdist',`delta_dist',`deltaPersonFE_dist')
	
	local min_dist = min(`x_dist',`RSSdist')
	
	drop IndYearFE_dif RegYearFE_dif PersonFE_dif delta_dif delta_PersonFE_dif
	
	file open track_progress using "${logs}nlhdfe_conv_track.csv", write append
	file write track_progress "`iter',`RSS1',`RSSdist',`RegYearFEdist',`IndYearFEdist',`PersonFEdist',`delta_dist',`deltaPersonFE_dist',`coef_FE',`coef_RegFE',`coef_IndFE'" _n
	file close track_progress	

}

* -------------------------------------- *
* --- Check that estimates converged --- *
* -------------------------------------- *

reg log_rem_dez delta1_PersonFE1 IndYearFE1 RegYearFE1 cat_age2 cat_age3 cat_age4 cat_age5 `if_cond'
regsave using "${output}estimates_nlhdfe.dta", replace

qui predict log_rem_dez_hat_nldhfe if e(sample), xb
qui gen theta_nlhdfe = log_rem_dez_hat_nldhfe - delta1_PersonFE1 - IndYearFE1 - RegYearFE1
qui gen resid_nldhfe = log_rem_dez - log_rem_dez_hat_nldhfe

rename PersonFE1 PersonFE_nlhdfe
rename RegYearFE1 RegionYearFE_nlhdfe
rename IndYearFE1 IndustryYearFE_nlhdfe
rename delta1 delta_nlhdfe
rename delta1_PersonFE1 delta_PersonFE_nlhdfe

* --------------------------- *
* --- Save the estimates ---- *
* --------------------------- *

preserve
	keep indiv PersonFE_nlhdfe
	drop if PersonFE_nlhdfe==.
	duplicates drop
	qui compress
	save "${output}IndFE_nlhdfe.dta", replace
restore

preserve
	keep mmc year mmc_year RegionYearFE_nlhdfe
	drop if RegionYearFE_nlhdfe==.
	duplicates drop
	qui compress
	save "${output}RegionYearFE_nlhdfe.dta", replace
restore

preserve
	keep subs_ibge year subs_ibge_year IndustryYearFE_nlhdfe
	drop if IndustryYearFE_nlhdfe==.
	duplicates drop
	qui compress
	save "${output}IndustryYearFE_nlhdfe.dta", replace
restore

preserve
	keep year delta_nlhdfe
	duplicates drop
	qui compress
	save "${output}delta_nlhdfe.dta", replace
restore

preserve
	keep cat_age* theta_nlhdfe
	duplicates drop
	qui compress
	save "${output}theta_nlhdfe.dta", replace
restore

* -------------------------------------------------------------- *
* --- Save the predicted values and residuals for bootstrap ---- *
* -------------------------------------------------------------- *

preserve
	keep indiv mmc year subs_ibge subs_ibge_year mmc_year  ///
		age_aux log_rem_dez_hat_nldhfe resid_nldhfe ///
		cat_age2 cat_age3 cat_age4 cat_age5 ///
		PersonFE_nlhdfe RegionYearFE_nlhdfe IndustryYearFE_nlhdfe delta_nlhdfe theta_nlhdfe
	save `predictfile', replace
restore 

erase ${data2}rais_mmc_to_mesoreg.dta

capture log close
di "Done"
