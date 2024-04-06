
set more off
set trace off
capture log close

global root "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\ReplicationFiles\"

global data1 "${root}ProcessedData_RAIS\Panel_1986_2010\"
global data2 "${root}Data_Other\"
global output "${root}ProcessedData_RAIS\RegionalEarnPremia_WorkerFE\Temp\"
global logs "${root}ProcessedData_RAIS\RegionalEarnPremia_WorkerFE\Temp\Logs\"

********************************************************************************
********************************************************************************

log using "${logs}3_est_fe_actual_data_cp_renormalize.log", replace


**********************************
***                        	   ***
*** Load all the estimates     ***
*** (Merge ones with varying   ***
*** return to ability as well) ***
***                            ***
**********************************

use ${output}new_panel_hdfe, clear

* Merge estimates with varying return to ability	
merge 1:1 indiv mmc subs_ibge year using ${output}new_panel_predicted, ///
	keepusing(PersonFE_nlhdfe RegionYearFE_nlhdfe ///
		IndustryYearFE_nlhdfe delta_nlhdfe theta_nlhdfe ///
		cat_age2 cat_age3 cat_age4 cat_age5)
drop _merge
			

********************************
***                        	 ***
*** Renormalization programs ***
***                          ***
********************************

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
		replace `PersonFE1'_renorm = . if `ref_reg_insample'==0	
	}
	if "`delta1'"!="" {
		gen `delta_PersonFE1' = `delta1' * `PersonFE1'
		reg log_rem_dez cat_age2 cat_age3 cat_age4 cat_age5 `delta_PersonFE1' `IndustryYearFE1' `RegionYearFE1'
		local cons = _b[_cons]

		gen `PersonFE1'_renorm = `PersonFE1' + (`IndustryYearFE_ref_mean' + `add_ref_reg' + `add_ref_reg2' + `cons' - `cons2')/`delta1'
		replace `PersonFE1'_renorm = . if `ref_reg_insample'==0	
		egen `PersonFE1'_renorm_1 = mean(`PersonFE1'_renorm), by(indiv)
		replace `PersonFE1'_renorm = `PersonFE1'_renorm_1
		drop `PersonFE1'_renorm_1
	}
end

******************************
***                        ***
*** RegHDfe                ***
***                        ***
******************************

display _n "Started normalizing REGHDFE at: $S_TIME  $S_DATE"

* Create indicator for reference groups from flsdvreg
gen ref_IndustryYear = 0
forvalues i=1(24)600 {
	qui replace ref_IndustryYear = 1 if subs_ibge_year == `i'
} 
gen ref_RegionYear = (RegionYearFE2 == 0)
count if ref_RegionYear==0

reg log_rem_dez cat_age2-cat_age5 PersonFE2 IndustryYearFE2 RegionYearFE2 
local cons2 = _b[_cons]

ReNormFE PersonFE1 IndustryYearFE1 RegionYearFE1 ref_IndustryYear ref_RegionYear group year `cons2'

* Cp regression results (should be exactly the same)
reg log_rem_dez cat_age2-cat_age5 PersonFE1 IndustryYearFE1 RegionYearFE1 
reg log_rem_dez cat_age2-cat_age5 PersonFE1_renorm IndustryYearFE1_renorm RegionYearFE1_renorm 

* Check that normalization is correct
assert abs(IndustryYearFE1_renorm - IndustryYearFE2)<0.001 if IndustryYearFE1_renorm<.
assert abs(RegionYearFE1_renorm - RegionYearFE2)<0.001 if RegionYearFE1_renorm<.

assert abs(PersonFE1_renorm - PersonFE2)<0.01 if PersonFE1_renorm<.

count if PersonFE1_renorm<.
count if PersonFE2<.
			
**********************************
***                            ***
*** Varying returns to ability ***
***                            ***
**********************************

display _n "Started normalizing model with varying return to ability at: $S_TIME  $S_DATE"

replace PersonFE_nlhdfe = . if PersonFE_nlhdfe==0

* Create indicator for reference groups from flsdvreg

ReNormFE PersonFE_nlhdfe IndustryYearFE_nlhdfe RegionYearFE_nlhdfe ref_IndustryYear ref_RegionYear group year `cons2' delta_nlhdfe

gen delta_PersonFE_nlhdfe = delta_nlhdfe * PersonFE_nlhdfe
gen delta_PersonFE_nlhdfe_renorm = delta_nlhdfe * PersonFE_nlhdfe_renorm

* Cp regression results (should be exactly the same)
reg log_rem_dez cat_age2-cat_age5 delta_PersonFE_nlhdfe IndustryYearFE_nlhdfe RegionYearFE_nlhdfe
reg log_rem_dez cat_age2-cat_age5 delta_PersonFE_nlhdfe_renorm IndustryYearFE_nlhdfe_renorm RegionYearFE_nlhdfe_renorm 

* ---------------------------- *
* --- Label the estimates ---- *
* ---------------------------- *

label var PersonFE1 "Individual FE est. with reghdfe"
label var PersonFE2 "Individual FE est. with felsdvreg"
label var PersonFE1_renorm "Individual FE est. with reghdfe and normalized to be comparable with felsdvreg"
label var PersonFE_nlhdfe "Individual FE est. with iterative procedure"
label var PersonFE_nlhdfe_renorm "Individual FE est. with iterative procedure and normalized to be comparable with felsdvreg"	

label var RegionYearFE1 "Region-year FE est. with reghdfe"
label var RegionYearFE2 "Region-year FE est. with felsdvreg"
label var RegionYearFE1_renorm "Region-year FE est. with reghdfe and normalized to be comparable with felsdvreg"
label var RegionYearFE_nlhdfe "Region-year FE est. with iterative procedure"
label var RegionYearFE_nlhdfe_renorm "Region-year FE est. with iterative procedure and normalized to be comparable with felsdvreg"	

label var IndustryYearFE1 "Industry-Year FE est. with reghdfe"
label var IndustryYearFE2 "Industry-Year FE est. with felsdvreg"
label var IndustryYearFE1_renorm "Industry-Year FE est. with reghdfe and normalized to be comparable with felsdvreg"
label var IndustryYearFE_nlhdfe "Industry-Year FE est. with iterative procedure"
label var IndustryYearFE_nlhdfe_renorm "Industry-Year FE est. with iterative procedure and normalized to be comparable with felsdvreg"	

label var group "Mobility group constructed by felsdvreg"
label var ref_IndustryYear "Reference industry-year group (felsdvreg), 1 for each year"
label var ref_RegionYear "Reference mmc-year group (felsdvreg), 1 for each mobility group"

* --------------------------- *
* --- Save the estimates ---- *
* --------------------------- *

preserve
keep indiv mmc year mmc_year subs_ibge subs_ibge_year ///
	 PersonFE1 PersonFE2 PersonFE1_renorm PersonFE_nlhdfe PersonFE_nlhdfe_renorm ///
	 RegionYearFE1 RegionYearFE2 RegionYearFE_nlhdfe RegionYearFE_nlhdfe_renorm ///
	 IndustryYearFE1 IndustryYearFE2 IndustryYearFE_nlhdfe IndustryYearFE_nlhdfe_renorm ///
	 group ref_IndustryYear ref_RegionYear log_rem_dez
save ${output}new_panel_all, replace	 
restore

preserve
	keep indiv PersonFE1 PersonFE2 PersonFE1_renorm PersonFE_nlhdfe PersonFE_nlhdfe_renorm
	duplicates drop
	bysort indiv: gen dupl = cond(_N>1,_n,0)
	keep if dupl <= 1
	drop dupl
	isid indiv
	label var PersonFE1 "Individual FE est. with reghdfe"
	label var PersonFE2 "Individual FE est. with felsdvreg"
	label var PersonFE1_renorm "Individual FE est. with reghdfe and normalized to be comparable with felsdvreg"
	label var PersonFE_nlhdfe "Individual FE est. with iterative procedure"
	label var PersonFE_nlhdfe_renorm "Individual FE est. with iterative procedure and normalized to be comparable with felsdvreg"	
	qui compress
	save ${output}IndFE_all_est, replace
restore

preserve
	keep mmc year mmc_year RegionYearFE1 RegionYearFE1_renorm RegionYearFE2 RegionYearFE_nlhdfe RegionYearFE_nlhdfe_renorm
	collapse (min) RegionYearFE1 RegionYearFE1_renorm RegionYearFE2 RegionYearFE_nlhdfe RegionYearFE_nlhdfe_renorm ///
			 (max) m_RegionYearFE1 = RegionYearFE1 m_RegionYearFE1_renorm = RegionYearFE1_renorm ///
			 	   m_RegionYearFE2 = RegionYearFE2 m_RegionYearFE_nlhdfe = RegionYearFE_nlhdfe ///
			 	   m_RegionYearFE_nlhdfe_renorm = RegionYearFE_nlhdfe_renorm mmc_year, by(mmc year) fast	
	assert RegionYearFE1 == m_RegionYearFE1
	assert RegionYearFE1_renorm == m_RegionYearFE1_renorm 
	assert RegionYearFE2 == m_RegionYearFE2
	assert RegionYearFE_nlhdfe == m_RegionYearFE_nlhdfe
	assert RegionYearFE_nlhdfe_renorm == m_RegionYearFE_nlhdfe_renorm
	drop m_RegionYear*
	isid mmc year
	label var RegionYearFE1 "Region-year FE est. with reghdfe"
	label var RegionYearFE2 "Region-year FE est. with felsdvreg"
	label var RegionYearFE1_renorm "Region-year FE est. with reghdfe and normalized to be comparable with felsdvreg"
	label var RegionYearFE_nlhdfe "Region-year FE est. with iterative procedure"
	label var RegionYearFE_nlhdfe_renorm "Region-year FE est. with iterative procedure and normalized to be comparable with felsdvreg"	
	qui compress
	save ${output}RegionYearFE_all_est, replace
restore

preserve
	keep subs_ibge year subs_ibge_year IndustryYearFE1 IndustryYearFE1_renorm IndustryYearFE2 IndustryYearFE_nlhdfe IndustryYearFE_nlhdfe_renorm
	collapse (min) IndustryYearFE1 IndustryYearFE1_renorm IndustryYearFE2 IndustryYearFE_nlhdfe IndustryYearFE_nlhdfe_renorm ///
			 (max) m_IndustryYearFE1 = IndustryYearFE1 m_IndustryYearFE1_renorm = IndustryYearFE1_renorm /// 
			 	   m_IndustryYearFE2 = IndustryYearFE2 m_IndustryYearFE_nlhdfe = IndustryYearFE_nlhdfe ///
			 	   m_IndustryYearFE_nlhdfe_renorm = IndustryYearFE_nlhdfe_renorm subs_ibge_year, by(subs_ibge year) fast
	drop if subs_ibge == ""		 	   		 	   
	assert abs(IndustryYearFE1 -  m_IndustryYearFE1) < 0.001 if IndustryYearFE1 < . & m_IndustryYearFE1 < .
	assert abs(IndustryYearFE1_renorm - m_IndustryYearFE1_renorm) < 0.001 if IndustryYearFE1_renorm < . & m_IndustryYearFE1_renorm < .
	assert abs(IndustryYearFE2 -  m_IndustryYearFE2) < 0.001 if IndustryYearFE2 < . & m_IndustryYearFE2 < .
	assert abs(IndustryYearFE_nlhdfe -  m_IndustryYearFE_nlhdfe) < 0.001 if IndustryYearFE_nlhdfe < . & m_IndustryYearFE_nlhdfe < .
	assert abs(IndustryYearFE_nlhdfe_renorm -  m_IndustryYearFE_nlhdfe_renorm) < 0.001 if IndustryYearFE_nlhdfe_renorm < . & m_IndustryYearFE_nlhdfe_renorm < .
	drop m_IndustryYear*
	isid subs_ibge year
	qui compress
	save ${output}IndustryYearFE_all_est, replace
restore

capture log close
di "Done"
