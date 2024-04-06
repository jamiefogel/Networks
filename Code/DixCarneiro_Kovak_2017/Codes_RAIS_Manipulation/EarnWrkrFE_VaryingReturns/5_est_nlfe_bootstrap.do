
args TaskID

set more off
set trace off
capture log close

capture set mem 20g

global root "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\ReplicationFiles\"

global data1  "${root}ProcessedData_RAIS\Panel_1986_2010\"
global data2  "${root}Data_Other\"
global output "${root}ProcessedData_RAIS\RegionalEarnPremia_WorkerFE\Temp\"
global boot   "${root}ProcessedData_RAIS\RegionalEarnPremia_WorkerFE\Temp\Bootstrap\"
global logs   "${root}ProcessedData_RAIS\RegionalEarnPremia_WorkerFE\Temp\Logs\"

********************************************************************************
********************************************************************************

local resid_mode  "asymmetric"

***************************
***                     ***
*** Spcifiy what to run ***
***                     ***
***************************

* How many iterations to try (max)?
scalar iter_limit = 200

* Tolerance for convergence?
scalar tol = 1e-4

* How to save the name with the predicted values, estimates and residuals?
* (required for the standard errors)
local predictfile "${output}new_panel_predicted.dta"
* Which file has fixed effects results?
local hdfe_predicted "${output}new_panel_hdfe"


**********************
***                ***
*** Load the data  ***
***                ***
**********************

use `predictfile', clear

************************
***                  ***
*** Bootstrap sample ***
***                  ***
************************

/* Note: Wild boostrap as recommended by Arcidiacono et al. 2012,
		 Implementation based on MacKinnon 2006 "Boostrap Methods in Economstrics" 
*/

* ------------------------------ *
* --- Draw the new residuals --- *
* ------------------------------ *

* set seed
local seed_value = 1000*`TaskID' + 123 
set seed `seed_value'

* gen b - bootstrap sample ID
gen b = `TaskID'

* generate v (multiplier for the actual residuals)
qui gen resid_draw = runiform()

* formula 15 in MacKinnon -- asymmetric distribution of residuals
if "`resid_mode'" == "asymmetric" {
	qui gen resid_v_star = cond(sign(resid_draw - (sqrt(5) + 1)/(2*sqrt(5))) == -1, - 0.5*(sqrt(5) - 1), 0.5*(sqrt(5) + 1))
}
* formula 14 in MacKinnon -- symmetric distribution of residuals
if "`resid_mode'" == "symmetric" {
	qui gen resid_v_star = sign(resid_draw - 0.5)
}

* ------------------------------ *
* --- New (bootrstap) sample --- *
* ------------------------------ *

* resampled residuals
gen resid_resampled = resid_nldhfe  * resid_v_star

* new y variable 
gen log_rem_dez = log_rem_dez_hat_nldhfe + resid_resampled

******************************
***                        ***
*** NL iterative procedure ***
***                        ***
******************************


/* Start from the estimates in the data instead to speed up the procedure! */

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

	* Normalize delta for the first year in the data to be 1
	* (otherwise things are not identified)
	qui sum year
	replace `delta0' = 1 if year == `r(min)'
	
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
	
	* Clean up
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

* Renormalize the estimates
capture program drop ReNormFE
program define ReNormFE
	args PersonFE1 IndustryYearFE1 RegionYearFE1 ref_IndustryYear ref_RegionYear group year cons2 delta1
	/* Description of the inputs
	Variables to be normalized:
		PersonFE1 - individual FE
		IndustryYearFE1 - Industry-year FE
		RegionYearFE2 - Region-year FE
	Variables determining reference groups in felsdvreg:
		ref_IndustryYear - ref group indicator for Industry-year FE
		ref_RegionYear - ref group indicator Region-year FE
		group - movers group
	*/
	
	tempvar add_ref_ind_min add_ref_ind_max add_ref_ind add_ref_ind_demeaned ///
			add_ref_reg_min add_ref_reg_max add_ref_reg ///
			add_ref_reg2_min add_ref_reg2_max add_ref_reg2 ///
			ref_reg_insample delta_PersonFE1
	
	* ------------------------ *
	* --- Industry-Year FE --- *
	* ------------------------ *	

	qui egen `add_ref_ind_min' = min(`IndustryYearFE1' * `ref_IndustryYear') if `IndustryYearFE1'<., by(`year')
	qui egen `add_ref_ind_max' = max(`IndustryYearFE1' * `ref_IndustryYear') if `IndustryYearFE1'<., by(`year')

	qui gen `add_ref_ind' = cond(`add_ref_ind_max' > 0, `add_ref_ind_max', `add_ref_ind_min')

	qui gen `IndustryYearFE1'_renorm = `IndustryYearFE1' - `add_ref_ind'

	qui sum `add_ref_ind' if IndustryYearFE1 < .
	local IndustryYearFE_ref_mean = `r(mean)'

	qui gen `add_ref_ind_demeaned' = `add_ref_ind' - `IndustryYearFE_ref_mean'

	* ------------------------ *
	* --- Region-Year FE --- -
	* ------------------------ *

	qui egen `add_ref_reg_min' = min(`RegionYearFE1' * `ref_RegionYear') if `RegionYearFE1'<., by(`group')
	qui egen `add_ref_reg_max' = max(`RegionYearFE1' * `ref_RegionYear') if `RegionYearFE1'<., by(`group')

	qui gen `add_ref_reg' = cond(`add_ref_reg_max' > 0, `add_ref_reg_max', `add_ref_reg_min')

	qui egen `add_ref_reg2_min' = min(`add_ref_ind_demeaned' * `ref_RegionYear') if `RegionYearFE1'<., by(`group')
	qui egen `add_ref_reg2_max' = max(`add_ref_ind_demeaned' * `ref_RegionYear') if `RegionYearFE1'<., by(`group')

	qui gen `add_ref_reg2' = cond(`add_ref_reg2_max' > 0, `add_ref_reg2_max', `add_ref_reg2_min')

	qui gen `RegionYearFE1'_renorm = `RegionYearFE1' - `add_ref_reg' + `add_ref_ind_demeaned' - `add_ref_reg2'

	* Set those for whom ref group is not in the sample to missing 
	qui egen `ref_reg_insample' = sum(`ref_RegionYear') if `RegionYearFE1'<., by(`group')
	replace `RegionYearFE1'_renorm = . if `ref_reg_insample'==0
	
	* ----------------- *
	* --- Person FE --- *
	* ----------------- *
	
	if "`delta1'"=="" {
		reg log_rem_dez cat_age2 cat_age3 cat_age4 cat_age5 `PersonFE1' `IndustryYearFE1' `RegionYearFE1'
		local cons = _b[_cons]

		gen `PersonFE1'_renorm = `PersonFE1' + `IndustryYearFE_ref_mean' + `add_ref_reg' + `add_ref_reg2' + `cons' - `cons2'
	}
	if "`delta1'"!="" {
		gen `delta_PersonFE1' = `delta1' * `PersonFE1'
		reg log_rem_dez cat_age2 cat_age3 cat_age4 cat_age5 `delta_PersonFE1' `IndustryYearFE1' `RegionYearFE1'
		local cons = _b[_cons]

		gen `PersonFE1'_renorm = `PersonFE1' + (`IndustryYearFE_ref_mean' + `add_ref_reg' + `add_ref_reg2' + `cons' - `cons2')/`delta1'
	}
	replace `PersonFE1'_renorm = . if `ref_reg_insample'==0	
end

* ------------------------- *
* --- Initialize values --- *
* ------------------------- *

gen insample = 1 if log_rem_dez_hat_nldhfe<.
local if_cond if insample==1

qui gen RegYearFE_init = RegionYearFE_nlhdfe
qui gen IndYearFE_init = IndustryYearFE_nlhdfe
qui gen PersonFE_init = PersonFE_nlhdfe
qui gen delta_init = delta_nlhdfe

local min_dist = 1
local iter = 0
local RSS1 = 0

qui sum year 
local first_year = `r(min)'	

* set up a file to track convergence
file open track_progress using "${boot}bootstrap_`resid_mode'_`TaskID'_nlhdfe_conv_track.csv", write replace
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
		
*	di _n "--- Iteration `iter' ---"
*	di _n "RSS0: `RSS0'"
*	di _n "RSS1: `RSS1'"
*	di _n "RSS distance: `RSSdist' "
*	di _n "Person FE distance: `PersonFEdist' "
*	di _n "RegYear FE distance: `RegYearFEdist' "
*	di _n "IndYear FE distance: `IndYearFEdist' "
*	di _n "delta distance: `delta_dist' "
*	di _n "delta*Person FE distance: `deltaPersonFE_dist' "
*	di _n "Coef at delta_PersonFE: `coef_FE'"
*	di _n "Coef at IndYearFE: `coef_IndFE'"
*	di _n "Coef at RegYearFE: `coef_RegFE'"
	
*	list PersonFE0 PersonFE1 delta0 delta1 delta0_PersonFE0 delta1_PersonFE1 in 1/5
	
	file open track_progress using "${boot}bootstrap_`resid_mode'_`TaskID'_nlhdfe_conv_track.csv", write append
	file write track_progress "`iter',`RSS1',`RSSdist',`RegYearFEdist',`IndYearFEdist',`PersonFEdist',`delta_dist',`deltaPersonFE_dist',`coef_FE',`coef_RegFE',`coef_IndFE'" _n
	file close track_progress	
	
}

* ----------------- *
* --- Estimates --- *
* ----------------- *

* -------------------------------------- *
* --- Check that estimates converged --- *
* -------------------------------------- *

reg log_rem_dez delta1_PersonFE1 IndYearFE1 RegYearFE1 cat_age2 cat_age3 cat_age4 cat_age5 `if_cond'

qui predict log_rem_dez_hat_b_nldhfe if e(sample), xb
qui gen theta_b_nlhdfe = log_rem_dez_hat_b_nldhfe - delta1_PersonFE1 - IndYearFE1 - RegYearFE1

rename PersonFE1 PersonFE_b_nlhdfe
rename RegYearFE1 RegionYearFE_b_nlhdfe
rename IndYearFE1 IndustryYearFE_b_nlhdfe
rename delta1 delta_b_nlhdfe
rename delta1_PersonFE1 delta_PersonFE_b_nlhdfe

* -------------------------------- *
* --- Renormalize the estimates --- *
* -------------------------------- *

merge 1:1 indiv mmc subs_ibge year using `hdfe_predicted'

reg log_rem_dez cat_age2-cat_age5 PersonFE2 IndustryYearFE2 RegionYearFE2 
local cons2 = _b[_cons]

gen ref_IndustryYear = 0
forvalues i=1(24)600 {
	qui replace ref_IndustryYear = 1 if subs_ibge_year == `i'
} 
qui gen ref_RegionYear = (RegionYearFE2 == 0)
count if ref_RegionYear==0

ReNormFE PersonFE_b_nlhdfe IndustryYearFE_b_nlhdfe RegionYearFE_b_nlhdfe ref_IndustryYear ref_RegionYear group year `cons2' delta_b_nlhdfe

gen delta_PersonFE_b_nlhdfe_renorm = delta_nlhdfe * PersonFE_b_nlhdfe_renorm
reg log_rem_dez delta_PersonFE_b_nlhdfe_renorm IndustryYearFE_b_nlhdfe_renorm RegionYearFE_b_nlhdfe_renorm cat_age2-cat_age5 


* --------------------------- *
* --- Save the estimates ---- *
* --------------------------- *

preserve
	keep b indiv PersonFE_b_nlhdfe PersonFE_b_nlhdfe_renorm
	duplicates drop
	qui compress
	save "${boot}bootstrap_`resid_mode'_`TaskID'_IndFE_nlhdfe.dta", replace
restore

preserve
	keep b mmc year mmc_year RegionYearFE_b_nlhdfe RegionYearFE_b_nlhdfe_renorm
	duplicates drop
	qui compress
	save "${boot}bootstrap_`resid_mode'_`TaskID'_RegionYearFE_nlhdfe.dta", replace
restore

preserve
	keep b subs_ibge year subs_ibge_year IndustryYearFE_b_nlhdfe IndustryYearFE_b_nlhdfe_renorm
	duplicates drop
	qui compress
	save "${boot}bootstrap_`resid_mode'_`TaskID'_IndustryYearFE_nlhdfe.dta", replace
restore

preserve
	keep b year delta_b_nlhdfe
	duplicates drop
	qui compress
	save "${boot}bootstrap_`resid_mode'_`TaskID'_delta_nlhdfe.dta", replace
restore

preserve
	keep b cat_age* theta_b_nlhdfe
	duplicates drop
	qui compress
	save "${boot}bootstrap_`resid_mode'_`TaskID'_theta_nlhdfe.dta", replace
restore

capture log close
di "Done"
