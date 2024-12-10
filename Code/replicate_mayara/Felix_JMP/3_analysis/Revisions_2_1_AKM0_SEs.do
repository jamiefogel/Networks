/*
	Calculate standard errors using AKM (2019) formula

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

local inputs		= 0
local SEs			= 1
local premiadate	= 20210802
local regsdate		= 20220623

*local allice "iceH iceW iceE iceWErp"
local allice "iceH"
local icemain "iceH"

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`regsdate'"
cap mkdir "${monopsonies}/eps/`regsdate'"
********************************************************************************
******** LLM-level regressions on earnings, employment and concentration *******
********************************************************************************

if `inputs'==1{

	* CNAE-level stat needed for AKM (2021) standard errors
		u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
		keep cnae10 TRAINS year
		reshape wide TRAINS, i(cnae10) j(year)
		gen double Dlnt_s = ln(1+TRAINS1994/100) - ln(1+TRAINS1990/100)
		
		keep cnae10 Dlnt_s
		ren cnae10 cnae95
		gen double A_s2 = Dlnt_s^2

		tempfile A_s2
		sa `A_s2'

	foreach group in cbo942d none {

		* Firm-market dataset
		u "${monopsonies}/sas/rais_collapsed_firm_mmc_`group'.dta", clear
		des
		keep if year==1991
		drop if inlist(mmc,13007)
		cap drop if inlist(cbo942d,31,22,37)

		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
		drop if mmc_drop==1

		keep fakeid_firm mmc `group' year cnae95 totdecearnmw

		* Payroll shares -- to be used later for kappa weights
		bys mmc `group': egen double mkttot = sum(totdecearnmw)
		gen double s2 = (totdecearnmw/mkttot)^2

		* Merge A_s2
		merge m:1 cnae95 using `A_s2', keep(3) nogen

		* Compute kappa weights of each firm
		bys mmc `group': egen double sum_s2 = sum(s2)
		gen double Kappa_zm = s2/sum_s2

		* Check that kappas sum to one : "kappa shares" are complete
		bys mmc `group': egen double checksum = sum(Kappa_zm)
		sum checksum

		* Sum firm Kappa weights by sector
		collapse (sum) Kappa_sm=Kappa_zm, by(cnae95 A_s2 mmc `group')

		keep cnae95 mmc `group' A_s2 Kappa_sm

		compress
		saveold "${monopsonies}/dta/coeffs/`regsdate'/AKM_SE_firm_inputs_mmc_`group'.dta", replace

		des
		unique mmc `group'
		sum A_s2, detail
		sum Kappa_sm, detail

	} /* close group */
}

if `SEs'==1{
	foreach group in cbo942d none {

		* Set of markets included in regression
		u "${monopsonies}/dta/coeffs/`regsdate'/DD_stacked_annual_mktregs_reg_AKM0_residuals_`group'.dta", clear
		keep mmc `group'
		gduplicates drop
		unique mmc `group'

		tempfile mkts
		sa `mkts'

		* Denominator : common across all markets
		u "${monopsonies}/sas/regsfile_mmc_`group'.dta", clear
		ren ice_dwtrains_hf iceH
		keep mmc `group' iceH
		duplicates drop
		merge 1:1 mmc `group' using `mkts', keep(3) nogen
		sum iceH, detail

		gen double ice2 = iceH^2
		collapse (sum) ice2

		sum ice2
		local denom = `r(mean)'

		u "${monopsonies}/dta/coeffs/`regsdate'/DD_stacked_annual_mktregs_reg_AKM0_residuals_`group'.dta", clear
		des
		drop if year==1991
		tempfile fall
		sa `fall'

		preserve
			keep if year==1995
			levelsof outcome, local(outcomes)
			levelsof weight, local(weights)
			levelsof ice, local(ices)
			levelsof spec, local(specs)
		restore
		
		/* Do levelsof in subsample so it's faster
		local outcomes "hf_pdbill lnmkt_emp lnmkt_ntemp lnmkt_temp"
		local weights = "0"
		local ices "iceH"
		local specs "main"	
		*/

		local i = 1
		foreach outcome in `outcomes'{
			foreach weight in `weights'{
				foreach ice in `ices'{
					foreach spec in `specs'{

						u `fall', clear

						di "keep if outcome==`outcome' & weight==`weight' & ice==`ice' & spec==`spec'"

						keep if outcome=="`outcome'" & weight=="`weight'" & ice=="`ice'" & spec=="`spec'"
						isid mmc `group' year

						reshape wide resid_mt, i(mmc `group') j(year)

						* Merge with cnae-level data
						merge 1:m mmc `group' using "${monopsonies}/dta/coeffs/`regsdate'/AKM_SE_firm_inputs_mmc_`group'.dta", keep(3) nogen

						* B_z for each year
						forvalues y=1986(1)2000{
							cap gen double B_s`y' = Kappa_sm*resid_mt`y'
						}
						collapse (sum) B_s*, by(cnae95 A_s2)

						reshape long B_s, i(cnae95 A_s2) j(year)
						order year

						gen double A2B2_s = A_s2*(B_s)^2

						collapse (sum) sum_A2B2_s= A2B2_s, by(year)

						gen double sum_ice2 = `denom'
						gen double se_akm = sqrt(sum_A2B2_s)/sum_ice2

						gen outcome = "`outcome'"
						gen weight = "`weight'"
						gen ice = "`ice'"
						gen spec = "`spec'"

						keep outcome weight ice spec year se_akm sum_A2B2_s sum_ice2
						order outcome weight ice spec year se_akm sum_A2B2_s sum_ice2 

						tempfile f`i'
						sa `f`i''

						local i=`i'+1
					}
				}
			}
		}
		local j = `i'-1
		u `f1', clear
		forvalues k = 2/`j'{
			append using `f`k''
		}
		
		outsheet using "${monopsonies}/csv/`regsdate'/AKM0_SEs_mmc_`group'.csv", comma replace
	}
}

