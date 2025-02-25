/*
	Compute earnings premia as year-by-year FEs	

	Specification pooling across all years and including worker FE

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


if `premia'==1{
	
		use fakeid_firm fakeid_worker mmc cbo942d agegroup educ earningsdecmw using "${monopsonies}/sas/rais_for_earnings_premia1986.dta", clear
		drop if mmc==13007 | mmc==23014
		drop if inlist(cbo942d,31,22,37)
		keep if !missing(educ) & earningsdecmw>0

		gen double year =1986
		forvalues i=1987/2000{
			append using "${monopsonies}/sas/rais_for_earnings_premia`i'.dta", keep(fakeid_firm fakeid_worker mmc cbo942d agegroup educ earningsdecmw)
			drop if mmc==13007 | mmc==23014
			drop if inlist(cbo942d,31,22,37)
			keep if !missing(educ) & earningsdecmw>0

			replace year = `i' if missing(year)
		}

		/*
		/* Use same control and ranges as DK */
		gen age1 = (agegroup==3)
		gen age2 = (agegroup==4)
		gen age3 = (agegroup==5)
		gen age4 = (agegroup==6)
		gen age5 = (agegroup==7)

		* Use same categories as DK
		forvalues i=1/8{
			gen educ`i' = (educ == `i')
		}
		gen educ9 = (educ >=9)
		*/
		*/

		gen double lndecearn = ln(earningsdecmw)
		
		*Firm-market-year group
		gegen double fe_zrot = group(fakeid_firm mmc cbo942d year)
		
		/*
		*Type-year interactions omitting base year and one type
		foreach var of varlist age2-age5 educ2-educ9 {
			forvalues i=1987(1)1989{
				gen double `var'X`i' = (`var'==1 & year==`i')
			}
		}
		*/

		* Worker type - year group
		gegen double fe_type_year = group(agegroup educ year)

		****************** Premia ****************
		reghdfe lndecearn, absorb(dprem_zrot=fe_zrot fe_worker=fakeid_worker fe_type_year)
		
		preserve
			keep if !missing(fe_worker)
			keep fakeid_worker fe_worker
			gduplicates drop
			compress
			saveold "${monopsonies}/sas/workerFEs`outdate'.dta", replace
		restore

		keep if !missing(dprem_zrot) 
		keep fakeid_firm year mmc cbo942d dprem_zrot
		gduplicates drop

		compress
	
	saveold "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_workerFE`outdate'.dta", replace
}

