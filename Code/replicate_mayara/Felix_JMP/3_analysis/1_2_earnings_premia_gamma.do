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

else if c(username)=="p13861161" & c(os)=="Windows" {
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara"
	global dictionaries		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara"
	global dictionaries		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdictionaries/harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies"
	global public			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/replicate_mayara\publicdata"
}

do "${encrypted}/Felix_JMP/3_analysis/specs_config.do"
args spec
di "`spec'"
if "`spec'"=="" local spec "3states_gamma"
di "`spec'"

if "`spec'" == "" {
    display as error "Error: No spec provided. Please pass a spec (e.g., gamma, original, gamma_2)."
    exit 1
}


cap log close
local date = subinstr("`c(current_date)'", " ", "_", .)
local time = subinstr("`c(current_time)'", ":", "_", .)
log using "${encrypted}/logs/1_2_earnings_premia_gamma_`spec'_`date'_`time'.log", replace


// Retrieve the market variables and file suffix based on the spec
local mkt "${s_`spec'_mv}"
local path "${s_`spec'_fs}"
local _3states "${s_`spec'_3s}"

display "Using market variables: `mkt'"
display "Using path suffix: `path'"



* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"

local premia 			= 1
local balanced_sample 	= 1
local erasefiles 		= 0

local outdate = 20210802

*XX local yearfirst = 1985
local yearfirst = 1986
local yearlast  = 2000

cap mkdir "${monopsonies}/eps/`outdate'/"


/* Get balanced sample of gammas */
if `balanced_sample'==1{

	forvalues y=`yearfirst'(1)`yearlast'{
		* Note that the _gamma fileworks for both market definitions cuz havent done any collapses at this stage
		u `mkt' using "${monopsonies}/sas/rais_for_earnings_premia`y'_gamma`_3states'.dta", clear
		gduplicates drop
		
		tempfile temp`y'
		sa `temp`y''
	}

	u `temp`yearfirst'', clear
	forvalues y=`yearfirst'(1)`yearlast'{
		merge 1:1 `mkt' using `temp`y'', keep(3) nogen
	}
	
	compress
	saveold "${monopsonies}/sas/balanced_`mkt's.dta", replace
}

if `premia'==1{
	forvalues y=`yearfirst'(1)`yearlast'{
		di "`y'"
		u "${monopsonies}/sas/rais_for_earnings_premia`y'_gamma`_3states'.dta", clear
		isid fakeid_worker
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
		
		gegen double fe_ro = group(`mkt')
		
		****************** MMC premia (incl. industry) ****************
		reghdfe lndecearn female age2-age5 educ2-educ9, absorb(dprems_ro=fe_ro ibgesubsector) noconstant
		
		****************** MMC average wage ****************
		reghdfe lndecearn, absorb(davgw_ro=fe_ro) noconstant
		
		****************** MMC premia (excl. industry) ****************
		reghdfe lndecearn female age2-age5 educ2-educ9, absorb(dprem_ro=fe_ro) noconstant
			


			
		keep if  !missing(dprems_ro) 
		keep `mkt' dprems_ro dprem_ro davgw_ro 
		gduplicates drop

		compress
		tempfile market`y'
		sa `market`y''
		
	} /* Close year */
	
	*********** Append across years *******
	local ynext = `yearfirst'+1
	
	u `market`yearfirst'', clear
	gen year = `yearfirst'
	forvalues y=`ynext'(1)`yearlast'{
		append using `market`y''
		replace year = `y' if missing(year)
	}
	gduplicates drop
	compress
	saveold "${monopsonies}/sas/rais_lnearn_premia_`path'_`outdate'.dta", replace
	
}
if `erasefiles'==1{
	cd "${monopsonies}/sas/"
	shell rm rais_for_earnings_premia*
}

log close
