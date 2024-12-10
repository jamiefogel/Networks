/*
	Compute earnings premia as year-by-year FEs	
	Keep singletons to have a premium point estimate for all 
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

local compress  		= 1
local balanced_sample 	= 1
local premia 			= 1
local erasefiles 		= 0

local outdate = 20210802

local yearfirst = 1985
local yearlast  = 2000

cap mkdir "${monopsonies}/eps/`outdate'/"


if `compress'==1{
	forvalues y= `yearfirst'(1)`yearlast'{	
		u "${monopsonies}/sas/rais_for_earnings_premia`y'.dta", clear
		compress
		saveold "${monopsonies}/sas/rais_for_earnings_premia`y'.dta", replace
	}
}

/* Get balanced sample of MMCs */
if `balanced_sample'==1{

	forvalues y=`yearfirst'(1)`yearlast'{
		u mmc using "${monopsonies}/sas/rais_for_earnings_premia`y'.dta", clear
		gduplicates drop
		
		tempfile mmc`y'
		sa `mmc`y''
	}

	u `mmc1985', clear
	forvalues y=`yearfirst'(1)`yearlast'{
		merge 1:1 mmc using `mmc`y'', keep(3) nogen
	}
	
	compress
	saveold "${monopsonies}/sas/balanced_mmcs.dta", replace
}


if `premia'==1{
	
	forvalues y=`yearfirst'(1)`yearlast'{
		u "${monopsonies}/sas/rais_for_earnings_premia`y'.dta", clear
		drop if mmc==13007 | mmc==23014
		
		* Keep if education category is well-defined
		keep if !missing(educ)
		
		* Keep only onbs with non-zero dec earnings
		keep if earningsdecmw>0
		
		/* Use same control and ranges as DK */
		gen age1 = (agegroup==3)
		gen age2 = (agegroup==4)
		gen age3 = (agegroup==5)
		gen age4 = (agegroup==6)
		gen age5 = (agegroup==7)
		
		gen double lndecearn = ln(earningsdecmw)
		
		* Use same categories as DK
		forvalues i=1/8{
			gen educ`i' = (educ == `i')
		}
		gen educ9 = (educ >=9)
	
		****************** MMC premia (incl. occup) ****************
		reghdfe lndecearn female age2-age5 educ2-educ9 , absorb(dprems_r=mmc ibgesubsector) noconstant
		
		****************** MMC premia (incl. occup) ****************
		reghdfe lndecearn female age2-age5 educ2-educ9 , absorb(dpremos_r=mmc cbo942d ibgesubsector) noconstant
		
		****************** MMC average wage ****************
		reghdfe lndecearn, absorb(davgw_r=mmc) noconstant
		
		****************** MMC premia (excl. sector) ****************
		reghdfe lndecearn female age2-age5 educ2-educ9 , absorb(dprem_r=mmc) noconstant
		
		keep if !missing(dprems_r) 
		keep mmc  dprems_r dpremos_r davgw_r dprem_r
		gduplicates drop

		compress
		tempfile mmc`y'
		sa `mmc`y''
		
	} /* Close year */
	
	*********** Append across years *******
	local ynext = `yearfirst'+1
	
	u `mmc1985', clear
	gen year = 1985
	forvalues y=`ynext'(1)`yearlast'{
		append using `mmc`y''
		replace year = `y' if missing(year)
	}
	gduplicates drop
	compress
	saveold "${monopsonies}/sas/rais_lnearn_premia_mmc_`outdate'.dta", replace
	
}


if `erasefiles'==1{
	cd "${monopsonies}/sas/"
	shell rm rais_for_earnings_premia*
}
