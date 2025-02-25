/*
	Market-level effect on weighted average wage markdown
	
*/
version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154

*ssc install vcemway

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

local premiadate	= 20210802
local etadate		= 20210802
local thetadate		= 20210802

local outdate	= 20210802

local mkdowns				= 1		/* Compute average markdowns */

local etaweight		= "w0"
local thetaweight 	= "all"
local iceshock		"ice_dwtrains_hf"
	
* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"

******************************************************************************

************************
*** Get eta estimate ***
************************
insheet using "${monopsonies}/csv/`etadate'/eta_change_regressions.csv", clear
keep if samp=="up91mkt" & model=="l" & wagevar=="lndp" & year==1997 & clust=="cnae95" & spec=="l" & tariff=="lnT" & weight=="`etaweight'"
levelsof iv_b, local(eta_inverse_b)
levelsof iv_se, local(eta_inverse_se)

**************************
*** Get theta estimate ***
**************************
insheet using "${monopsonies}/csv/`thetadate'/theta_change_regressions.csv", clear
keep if esamp=="u91m" & tsamp=="u91m" & model=="l" & wagevar=="lndp" & year==1997 & thetaclust=="fe_ro" & deltatype == "delta_ro" & spec=="m" & weight=="`thetaweight'" & chng_lrotype=="chng_Lro" & etaweight=="`etaweight'"
levelsof iv_b, local(diff_b)
levelsof iv_se, local(diff_se)
levelsof theta_inverse_b, local(theta_inverse_b)
levelsof theta_inverse_se, local(theta_inverse_se)

	
if `mkdowns'==1{

	u "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", clear
	keep mmc cbo942d year mkt_emp
	
	merge 1:1 mmc cbo942d year using "${monopsonies}/sas/earnings_premia_Herfindhal_mmc_cbo942d.dta", keep(3) nogen
	
	sum hf_pdbill
	assert hf_pdbill<=1
	
	gen double avg_inv_epsilon = `theta_inverse_b'*hf_pdbill + (1-hf_pdbill)*`eta_inverse_b'
	
	* Manual SE using variance of individual coefficients
	gen double avg_inv_epsilon_se = sqrt((`theta_inverse_se'*hf_pdbill)^2 + ((1-hf_pdbill)*`eta_inverse_se')^2)
	
	gen double avg_takehome 		= 1/(1+avg_inv_epsilon)
	gen double avg_takehome_se_app	= avg_inv_epsilon_se		/* Assume SE is ~ same as SE on epsilon */
	
	* Bootstrap single SE per year using data across all markets
	program bootse, rclass
		version 14
		cap drop mu mu2 E2 avg_takehome2
		egen double mu = mean(avg_takehome)
		gen double mu2 = mu^2
		
		gen double avg_takehome2 = avg_takehome^2
		egen double E2 = mean(avg_takehome2)
		
		sum mu2
		local mu2 = `r(mean)'
		sum E2
		local E2 = `r(mean)'
		
		drop mu mu2 E2 avg_takehome2
		return scalar SEboot = sqrt(`E2' - `mu2')
	end
	
	tempfile all
	sa `all'
	
	forvalues i=1986(1)2000{
		preserve
			keep if year==`i'
			if `i'==1991{
				di "Do nothing"
			}
			else{
				qui bootstrap r(SEboot), reps(1000): bootse 
				mat list r(table)
				local bootSE`i' = r(table)[1,1]
			}
		restore
	}
	
	u `all', clear
	gen double avg_takehome_se_boot=.
	forvalues y=1986(1)2000{
		cap replace avg_takehome_se_boot=`bootSE`y'' if year==`y'
	}
	
	saveold "${monopsonies}/sas/average_markdowns_mmc_cbo942d.dta", replace
	
}
