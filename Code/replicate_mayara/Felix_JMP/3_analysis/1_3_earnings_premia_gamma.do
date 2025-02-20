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
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}


capture log close 
log using "${encrypted}/logs/1_3_earnings_premia_gamma.log", replace

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"

local premia 		= 1
local erasefiles 	= 0

local outdate 	= 20210802
local yearfirst = 1986
local yearlast  = 2000

foreach version in original gamma {

	if "`version'"=="original"{
		local mkt "mmc cbo942d"
		local path "mmc_occ942d"
	}
	if "`version'"=="gamma"{
		local mkt "gamma"
		local path "gamma"
	}
	
	if `premia'==1{
		
		forvalues y=`yearfirst'(1)`yearlast'{		
			u "${monopsonies}/sas/rais_for_earnings_premia`y'_gamma.dta", clear
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
			
			gegen double fe_zro = group(fakeid_firm `mkt')
			
			****************** MMC average wage ****************
			reghdfe lndecearn, absorb(davgw_zro=fe_zro) noconstant keepsingletons
			
			****************** MMC premia (excl. industry) ****************
			reghdfe lndecearn female age2-age5 educ2-educ9 , absorb(dprem_zro=fe_zro) noconstant keepsingletons
				
			keep if !missing(dprem_zro) 
			keep fakeid_firm `mkt' dprem_zro davgw_zro
			gduplicates drop

			compress
			tempfile firm`y'
			sa `firm`y''
		} /* Close year */
		
		*********** Append across years *******
		local ynext = `yearfirst'+1
		
		u `firm1986', clear
		gen year = 1986
		forvalues y=`ynext'(1)`yearlast'{
			append using `firm`y''
			replace year = `y' if missing(year)
		}
		gduplicates drop
		compress
		saveold "${monopsonies}/sas/rais_lnearn_premia_`path'_`outdate'.dta", replace
	}
}

if `erasefiles'==1{
	cd "${monopsonies}/sas/"
	shell rm rais_for_earnings_premia*
}
