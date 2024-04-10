
set more off
set trace off
capture log close

global root "C:/Users/rd123/Dropbox/DixCarneiroKovakRodriguez/ReplicationFiles/"

global data1  "${root}ProcessedData_RAIS/Panel_1986_2010/"
global data2  "${root}Data_Other/"
global output "${root}ProcessedData_RAIS/RegionalEarnPremia_WorkerFE/Temp/"
global boot   "${root}ProcessedData_RAIS/RegionalEarnPremia_WorkerFE/Temp/Bootstrap/"
global logs   "${root}ProcessedData_RAIS/RegionalEarnPremia_WorkerFE/Temp/Logs/"
global out_f  "${root}ProcessedData_RAIS/RegionalEarnPremia_WorkerFE/"

********************************************************************************
********************************************************************************

log using "${logs}6_est_nlfe_bootstrap_collect.log", replace


***************************
***                     ***
*** Spcifiy what to run ***
***                     ***
***************************

* How many boostrap iterations?
* local B = 5
local B = 500

* Stub for files with the saved estimates
local stub_nlhdfe ""
local stub_estimates "all_est"

* What was the residual mode used?
* symmetric or asymmetric?
global resid_mode "asymmetric"


****************
***          ***
*** Programs ***
***          ***
****************

* Load all the bootstrap results
capture program drop LoadBootstrap
program define LoadBootstrap
	args EstFileName EstName GroupName B stub
	* Arguments:
	* EstFileName - names of the estimates (PersonFE, IndFE etc.)
	* Estimate variable - 
	* B - No. of boostrap iterations
	* stub - stub with which boostrap files were saved
	tempfile AllBootstrap
	
	* Load bootstrap
	local b_iter = 0
	di "No bootstrap iter: `B'"
	forvalues b=1/`B' {
		capture confirm file "${boot}bootstrap_${resid_mode}_`b'_`EstFileName'_nlhdfe`stub'.dta"
		if _rc!=0 {
			display _n "Bootstrap sample `b' not found!"
		}
		else {
			qui use "${boot}bootstrap_${resid_mode}_`b'_`EstFileName'_nlhdfe`stub'.dta", clear
			di _n "Loaded bootstrap sample `b'"
			local b_iter = `b_iter' + 1			
			qui drop if `EstName'_b_nlhdfe==0

			capture append using `AllBootstrap'
			qui save `AllBootstrap', replace

		}		
	} 
	
	* Effective B (taking into account samples which failed to save etc.)
	gen B_effective = `b_iter'
		
	* Standard errors
	egen `EstName'_nl_se = sd(`EstName'_b_nlhdfe), by(`GroupName')	
	capture confirm variable `EstName'_b_nlhdfe_renorm
	if !_rc {
		egen `EstName'_nl_renorm_se = sd(`EstName'_b_nlhdfe_renorm), by(`GroupName')	
		collapse (max) B_effective `EstName'_nl_se `EstName'_nl_renorm_se, by(`GroupName')
	}
	else {
		collapse (max) B_effective `EstName'_nl_se, by(`GroupName')
	}
end

**************************************************
***                                            ***
*** Estimates for delta_t (returns to ability) ***
***                	                           ***
**************************************************

LoadBootstrap "delta" "delta" "year" `B' "`stub_nlhdfe'"

save "${output}SE_delta_nlhdfe_v2.dta", replace
	
use "${output}delta_nlhdfe`stub_nlhdfe'.dta", clear
drop if delta_nlhdfe==.
qui merge 1:1 year using "${output}SE_delta_nlhdfe_v2.dta"
drop _merge
	
label var B_effective "No of bootstrap iterations"
label var delta_nl_se "Bootstrap s.e.: returns to ability"

qui compress
save "${output}delta_nlhdfe`stub_estimates'_withse_v2.dta", replace
	
**************************************
***                                ***
*** Estimates for Industry-Year FE ***
***                	               ***
**************************************

LoadBootstrap "IndustryYearFE" "IndustryYearFE" "subs_ibge_year" `B' "`stub_nlhdfe'"

save "${output}SE_IndustryYearFE_v2.dta", replace

use "${output}IndustryYearFE_`stub_estimates'.dta", clear
qui merge 1:1 subs_ibge_year using "${output}SE_IndustryYearFE_v2.dta"
drop _merge

label var B_effective "No of bootstrap iterations"
label var IndustryYearFE_nl_se "Bootstrap s.e.: Industry-Year FE est. with iterative procedure"
label var IndustryYearFE_nl_renorm_se "Bootstrap s.e.: Industry-Year FE est. with iterative procedure and normalized to be comparable with felsdvreg"	

qui compress
save "${output}IndustryYearFE_`stub_estimates'_withse_v2.dta", replace
		


************************************
***                              ***
*** Estimates for Region-Year FE ***
***                	             ***
************************************

LoadBootstrap "RegionYearFE" "RegionYearFE" "mmc_year" `B' "`stub_nlhdfe'"

save "${output}SE_RegionYearFE.dta", replace
		
use "${output}RegionYearFE_`stub_estimates'.dta", clear
qui merge 1:1 mmc_year using "${output}SE_RegionYearFE.dta"
drop _merge
	
label var B_effective "No of bootstrap iterations"
label var RegionYearFE_nl_se "Bootstrap s.e.: Region-Year FE est. with iterative procedure"
label var RegionYearFE_nl_renorm_se "Bootstrap s.e.: Region-Year FE est. with iterative procedure and normalized to be comparable with felsdvreg"	
	
qui compress
save "${output}RegionYearFE_`stub_estimates'_withse.dta", replace


**************************************
* Save final dataset in "final" folder
**************************************

save "${out_f}RegionYearFE_`stub_estimates'_withse.dta", replace

capture log close
di "Done"
