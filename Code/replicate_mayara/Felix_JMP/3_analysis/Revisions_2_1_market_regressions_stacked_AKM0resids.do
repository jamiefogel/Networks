/*
	1) Save residuals to correct SEs a la AKM (2021)

	2) Add new outcomes: employment by surviving status
		emp_exit_llm
		emp_exit_cbo942d
		emp_exit_mmc
		emp_exit_all
*/
version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154

* ssc install vcemway

* Gtools work on server
if c(username)=="mfelix"{
	local tool "g"
}
else{
	local tool ""
}

* Mayara mounting on server
if c(username)=="mfelix"{
	global dictionaries		"/proj/patkin/raisdictionaries/harmonized"
	global deIDrais			"/proj/patkin/raisdeidentified"
	global monopsonies		"/proj/patkin/projects/monopsonies"
	global public			"/proj/patkin/publicdata"
}
* Mayara mounting locally on Mac
else if c(username)=="mayara"{
	global dictionaries		"/Volumes/proj_atkin/raisdictionaries/harmonized"
	global deIDrais			"/Volumes/proj_atkin/raisdeidentified"
	global monopsonies		"/Volumes/proj_atkin/projects/monopsonies"
	global public			"/Volumes/proj_atkin/publicdata"
	
}
* Mayara mounting locally on Windows
else if c(username)=="Mayara"{
	global dictionaries		"M:/raisdictionaries/harmonized"
	global deIDrais			"M:/raisdeidentified"
	global monopsonies		"M:/projects/monopsonies"
	global public			"M:/publicdata"
}


global minyear = 1986
global maxyear = 2000
local minyear = ${minyear}
local maxyear = ${maxyear}
local baseyear = 1991

local regressions	= 1
local premiadate	= 20210802
local regsdate		= 20220623

local allice "iceH iceW iceE iceWErp"
*local allice "iceH"
local icemain "iceH"

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`regsdate'"
cap mkdir "${monopsonies}/eps/`regsdate'"
********************************************************************************
******** LLM-level regressions on earnings, employment and concentration *******
********************************************************************************

foreach group in cbo942d none{

	u "${monopsonies}/sas/regsfile_mmc_`group'.dta", clear
		drop if year==1985
		
		cap drop tot_* wavg* ewavg* wavg*
		drop *top* *bot* *gt10* *lt10*
		cap gen none = 1
		
		keep if inrange(year,${minyear},${maxyear})
		
		* Drop manaus and other mmcs dropped by DK (2017)
		drop if inlist(mmc,13007)
		
		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
		drop if mmc_drop==1
		
		cap drop if inlist(cbo942d,31,22,37)
		
		* Bring in mesoregion for SEs
		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_1991_2010_to_c_mesoreg.dta", keep(3) nogen
		ren c_mesoreg mesoreg

		* Merge in earnings premia
		if "`group'"=="none"{
			merge 1:1 mmc year using "${monopsonies}/sas/rais_lnearn_premia_mmc_`premiadate'.dta", keep(1 3) nogen 
			*gen double mmc_main =mmc
			*merge 1:1 mmc_main year using "${monopsonies}/sas/rais_lnearn_premia_wSE_mmc_`premiadate'.dta", keep(1 3) nogen 
			*drop mmc_main
		}
		else{
			merge 1:1 mmc `group' year using "${monopsonies}/sas/rais_lnearn_premia_mmc_`group'_`premiadate'.dta", keep(1 3) nogen 
		}
		
		* Merge in wage premia Herfindahl
		if "`group'"=="none"{
			merge 1:1 mmc year using "${monopsonies}/sas/earnings_premia_Herfindhal_mmc_`group'.dta", keep(1 3) nogen
		}
		else{
			merge 1:1 mmc `group' year using "${monopsonies}/sas/earnings_premia_Herfindhal_mmc_`group'.dta", keep(1 3) nogen
		}

		* Bring in employment by different exit status for non-exporting tradables
		if "`group'"=="cbo942d"{
			merge 1:1 year mmc cbo942d using "${monopsonies}/sas/nonexpT_emp_byexit_mmc_cbo942d.dta", keepusing(emp_*) keep(1 3) nogen
		}
		else if "`group'"=="none"{
			preserve
				u "${monopsonies}/sas/nonexpT_emp_byexit_mmc_cbo942d.dta", clear
				collapse (sum) emp_*, by(mmc year)
				tempfile empexit
				sa `empexit'
			restore

			merge 1:1 year mmc using `empexit', keep(1 3) nogen
		}
		
		* Keep balanced panel
		*keep if mkt_emp>=10
		bys mmc `group': `tool'egen totyears = count(year)
		egen double maxy = max(totyears)

		keep if totyears==maxy
		drop maxy totyears
		
		* Baseline employment as weight
		gen double weight0  = 1
		local baseyear = 1991
		`tool'egen weight1 		= mean(cond(year==`baseyear',mkt_emp,.)), by(mmc `group')
		
		* Non-tradables employment
		gen double mkt_ntemp = mkt_emp - mkt_temp
		
		* Non-exporter tradables employment
		gen double mkt_nexplibTemp = mkt_temp - explib_emp
		
		* All except exporter employment
		gen double mkt_nexplibemp = mkt_emp - explib_emp	
		
		********* Shares *********
		gen double expshare 		= explib_emp/mkt_emp
		gen double nexpshare 		= 1-expshare
		
		gen double Tnexpshare		= mkt_nexplibTemp/mkt_emp
		gen double NTshare			= mkt_ntemp/mkt_emp
		
		* Reverse sign of ICE so can more easily interpret, if no ICE (no tradables) replace with zero
		foreach var of varlist ice*{
			replace `var' = 0 if missing(`var')
			replace `var' = - `var'
		}
		
		* Log
		foreach var of varlist mkt_avgdearn *emp* *bill* *firms*{
				if regexm("`var'","hf_")>0 {
				
					*  Scale Herfindahl before making inverse hyp sign
					qui gen `var'scaled = `var'*100
					gen double ln`var' = ln(`var'scaled + sqrt(`var'scaled^2 +1))
					drop `var'scaled
				}
				else{
					qui replace `var' = ln(`var'+ sqrt(`var'^2 +1))
					ren `var' ln`var'
				}
		} 

		ren lnemp_exit_llm lnempEXIllm
		ren lnemp_exit_mmc lnempEXImmc
		ren lnemp_exit_cbo942d lnempEXIcbo
		ren lnemp_exit_all lnempEXIall
			
		ds lnmkt_avgdearn lnmkt_emp davg* dprem* ///
			hf_wdbill hf_pdbill hf_emp  ///
			lnhf_wdbill lnhf_pdbill lnhf_emp ///
			lnmkt_ntemp lnmkt_temp lnmkt_firms lnexplib_firms ///
			lnexplib_emp lnmkt_nexplibTemp lnmkt_nexplibemp ///
			expshare nexpshare Tnexpshare NTshare ///
			lnempEXIllm lnempEXImmc lnempEXIcbo lnempEXIall 
		
		local regvars "`r(varlist)'"
		keep mmc mesoreg `group' year ice* weight* `regvars' 
		
		* Long differences
		foreach var of varlist `regvars'{
			bys mmc `group': egen double `var'`baseyear' = max(cond(year==`baseyear',`var',.))
			
			gen double D`var' = (`var' - `var'`baseyear')
			replace D`var' = -D`var' if year<`baseyear'
			drop `var'`baseyear'
		}
		
		ren ice_bdwtrains		icebW
		ren ice_btrains			icebE
		ren icet_dwtrains 		iceWt
		ren ice_dwtrains 		iceW
		ren ice_dwerptrains 	iceWErp
		ren ice_trains  		iceE
		ren ice_dwtrains_hf 	iceH
		
		ren mesoreg meso
		
		tempfile regsfile
		sa `regsfile'
		
		************************************
		foreach w in 0 1{
		foreach ice in `allice'{
			
			u `regsfile', clear

			* Interactions with year
			forvalues y=1986/2000{
			
				gen double fe_`y' = (year==`y')
				gen double ICE_`y' = `ice'*(year==`y')
			}
			
			* Ommit base year
			renvars ICE_`baseyear' fe_`baseyear', pref("om_")
			
			egen double llm = group(mmc `group')
			
			sum weight1, detail
			gen double ICEX = `ice'*(year>1994)
			gen double ICEtrend =`ice'*(`baseyear' - year)
			
			foreach x in `regvars'{
			
				* De-trending
				reghdfe D`x' ICEtrend [w=weight`w'] if year<=1989, absorb(llm year)

				gen double nres`x' = D`x'
				replace nres`x' = D`x' - _b[ICEtrend]*`ice'*(`baseyear' - year) if year>1989

				foreach var in i_llm i_year fe_llm fe_year res pred{
					cap drop `var'
				}
				
				* Annual specification in stacked mode (spec y)
					* Levels
						reghdfe D`x' fe_* [w=weight`w'], absorb(llm) resid
					
						preserve
							predict double resid_mt, resid
							keep if !missing(resid_mt)

							keep year mmc `group' resid_mt

							gen outcome = "`x'"
							gen weight = "`w'"
							gen ice = "`ice'"
							gen spec = "main"

							tempfile res`x'`w'`ice'
							sa `res`x'`w'`ice''
						restore
						
					* De-trended (Spec ny)
						* Adjust dof to account for estimated coefficient used
						reghdfe nres`x' fe_* [w=weight`w'], absorb(llm) resid

						preserve
							predict double resid_mt, resid
							keep if !missing(resid_mt)
							
							keep year mmc `group' resid_mt

							gen outcome = "`x'"
							gen weight = "`w'"
							gen ice = "`ice'"
							gen spec = "detrended"

							tempfile nres`x'`w'`ice'
							sa `nres`x'`w'`ice''
						restore
						
			
			} /* Close regvars */
		} /* Close ice */
		} /* Close weight */
	
	u `reslnmkt_emp1`icemain'', clear
	foreach w in 0 1{
		foreach x in `regvars'{
			foreach ice in `allice'{
				append using `res`x'`w'`ice''
				append using `nres`x'`w'`ice''
			}
		}
	}
	order outcome weight ice spec
	gduplicates drop
	compress
	saveold "${monopsonies}/dta/coeffs/`regsdate'/DD_stacked_annual_mktregs_reg_AKM0_residuals_`group'.dta", replace

} /* Close group */
		
