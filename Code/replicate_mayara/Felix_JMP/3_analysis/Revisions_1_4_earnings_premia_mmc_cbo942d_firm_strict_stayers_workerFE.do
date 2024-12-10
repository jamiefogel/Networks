/*
	Compute earnings premia as year-by-year FEs	
	Keep singletons to have a premium point estimate for all 

	Specification pooling 1991 and 1997 and
	including worker fixed effect

*/
version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154

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


* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"

local premia 		= 1

local outdate 	= 20220623
local yearfirst = 1991
local yearlast  = 1997


if `premia'==1{
	
		u "${monopsonies}/sas/rais_for_earnings_premia1991.dta", clear
		* Restrictions
			drop if mmc==13007 | mmc==23014

			* Keep if education category is well-defined
			keep if !missing(educ)
			
			* Keep only onbs with non-zero dec earnings
			keep if earningsdecmw>0

		* Keep strict stayers for 1991-1997
		ren earningsdecmw earningsdecmw1991
		ren agegroup agegroup1991
		merge 1:1 fakeid_worker fakeid_firm cbo942d mmc ///
		using "${monopsonies}/sas/rais_for_earnings_premia1997.dta", keep(3) keepusing(agegroup earningsdecmw) nogen

		ren earningsdecmw earningsdecmw1997
		ren agegroup agegroup1997
		reshape long earningsdecmw agegroup, i(fakeid_worker) j(year)
		
		/* Use same control and ranges as DK */
		gen age1 = (agegroup==3)
		gen age2 = (agegroup==4)
		gen age3 = (agegroup==5)
		gen age4 = (agegroup==6)
		gen age5 = (agegroup==7)
		
		gen double lndecearn = ln(earningsdecmw)
		
		*Firm-market-year group
		gegen double fe_zrot = group(fakeid_firm mmc cbo942d year)
		
		*Time-varying characteristics (age)
		foreach var of varlist age2 age3 age4 age5 {
			gen double `var't91 = `var'==1 & year==1991
			gen double `var't97 = `var'==1 & year==1997
		}

		****************** Premia ****************
		reghdfe lndecearn *t91 *t97, absorb(dprem_zrot=fe_zrot fakeid_worker)
		
		keep if !missing(dprem_zrot) 
		keep fakeid_firm year mmc cbo942d dprem_zrot
		gduplicates drop

		compress
	
	saveold "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_strict_stayers_workerFE`outdate'.dta", replace
}

