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

local premiadate 	= 20210802
local outdate		= 20210802

local baseyear 	= 1991
local baseyear_o1 	= `baseyear'+3
local baseyear_o2 	= `baseyear'+6

local baseyear_n = 91

local regressions   	= 1

local regvars 		"Exlnemp Exdprem_zro"
local allsamples	"all"
local iceshock		"ice_dwtrains_hf"

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"


local usesample = 0		/* Use 10% random sample of firms (for de-bugging) */

***************************************************
******************* Regressions *******************
***************************************************

if `regressions'==1{
	
		u "${monopsonies}/sas/firm_DD_regsfile`usesample'.dta", clear
		
		preserve
			keep if year<1991
			tempfile pre
			sa `pre'
		restore
		
		keep if year>=1991
		
		egen double fe_zro = group(fakeid_firm mmc cbo942d)
		
		* Fill forward to incorporate for firms that will exit
		preserve
			keep fe_zro fakeid_firm cnae95 explib up91mkt chng_lnT T mmc cbo942d
			gduplicates drop
			tempfile fills
			sa `fills'
		restore
		
		xtset fe_zro year
		tsfill, full
		
		merge m:1 fe_zro using `fills', keep(3 4 5) update nogen
		drop fe_zro
		
		* Append pre-period
		append using `pre'
		
		* Firms who exit the market
		gen Exemp = emp
		replace Exemp = 0 if missing(emp)
			
		* Inverse hyp sign
		gen double Exlnemp = ln(Exemp + sqrt(Exemp^2 + 1))
		
		* Merge in earnings premia
		merge 1:1 fakeid_firm mmc cbo942d year using "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_`premiadate'.dta", keep(1 3) nogen
		
		* Assume firms who exit were paying the smallest wage premium in the region for the year
		bys mmc cbo942d year: egen double minllm= min(dprem_zro)
		bys mmc year: egen double minmmc= min(dprem_zro)
		bys year: egen double min= min(dprem_zro)
		
		gen double Exdprem_zro = dprem_zro
		replace Exdprem_zro=minllm if missing(dprem_zro)
		replace Exdprem_zro=minmmc if missing(dprem_zro)
		replace Exdprem_zro=min if missing(dprem_zro)
		
		preserve
			u "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", clear
			keep mmc cbo942d `iceshock'
			gduplicates drop
			tempfile dwice
			sa `dwice'
		restore
		merge m:1 mmc cbo942d using `dwice', keep(3) keepusing(`iceshock') nogen
		
		ren `iceshock'		ice
		ren lnTRAINS 			lnT
		ren lnErpTRAINS 		lnE
		ren chng_lnTRAINS 		chng_lnT
		ren chng_lnErpTRAINS 	chng_lnE
		
		
		****** Invert the sign so it's easier to interpret ******
		replace ice 			= - ice
		replace chng_lnT 		= - chng_lnT
		replace chng_lnE 		= - chng_lnE	
		
		drop ice_* *Erp* lnE
		
		drop lnT* up91 in91mkt in91 bTeshare bTwshare beshare bwshare Twshare Teshare bexp chng_lnE
		
		local ivars "fakeid_firm mmc cbo942d cnae95 chng_lnT explib ice T up91mkt "
		
		ds `ivars' year, not v(32)
		local widevars "`r(varlist)'"	
		
		reshape wide `widevars', i(`ivars') j(year)
		order `ivars'
		
		****************************************
		****** DD year by year variables  ******
		****************************************
		
		foreach x in `widevars'{
		
			*** Differences From base year back to y ***
			local pre = `baseyear' - 1
			forvalues y=1986/`pre'{
				qui gen double D`x'`y' = `x'`baseyear' - `x'`y'
			}
			
			*** Differences to Base Year ***
			local next = `baseyear' +1
			forvalues y=`next'/2000{
				qui gen double D`x'`y' =  `x'`y' -`x'`baseyear'
			}
		}
		
		**************************************************************************
		****** Interactions with exporter at baseline and relative size at baseline  ******
		**************************************************************************
		
		* Size variables
		gen double gt10 = (emp`baseyear'> 10)
		gen double gt20 =  (emp`baseyear'> 20)
		gen double gt100 = (emp`baseyear'> 100)
		sum emp`baseyear', detail
		gen double top90 = (emp`baseyear'>`r(p90)')
		
		local sizevars "gt10 gt20 gt100 top90"
		
		gen double chng_lnT_explib  = chng_lnT*explib
		foreach var in `sizevars'{
			gen double chng_lnT_`var'   = chng_lnT*`var'
			gen double chng_lnT_int2`var' 	= chng_lnT*explib*`var'
			gen double int2`var' = explib*`var'
		}
		
		********************************************************************
		****************** Firm-market level Diff in Diff ******************
		********************************************************************
		
		keep `ivars' chng_lnT* ice* D* `sizevars' int2*
		cap gen all = 1
		egen llm = group(cbo942d mmc)
			
		foreach samp in `allsamples'{
		foreach x in `regvars'{
		foreach sizev in `sizevars'{
		foreach y in 1997{
			if `y'==1991{
				di "1991 is base year - no reg"
			}
			else{
			areg D`x'`y' chng_lnT chng_lnT_explib chng_lnT_`sizev' chng_lnT_int2`sizev'  explib `sizev' int2`sizev'  if `samp'==1, absorb(llm) cluster(fakeid_firm)
			count if e(sample)
			local obs = `r(N)'
			unique fakeid_firm
			local firms = `r(unique)'
			unique llm
			local mkts = `r(unique)'
			preserve
				clear
				set obs 1
				gen var = "`x'"
				gen sizevar = "`sizev'"
				gen year = `y'
				gen samp = "`samp'"	
				gen obs	 = `obs'
				gen firms = `firms'
				gen mkts = `mkts'
				gen b 					= _b[chng_lnT]
				gen se 					= _se[chng_lnT]
				gen b_int_explib 		= _b[chng_lnT_explib]
				gen se_int_explib 		= _se[chng_lnT_explib]
				gen b_int_sizev			= _b[chng_lnT_`sizev']
				gen se_int_sizev 		= _se[chng_lnT_`sizev']
				gen b_int_int2sizev 	= _b[chng_lnT_int2`sizev']
				gen se_int_int2sizev 	= _se[chng_lnT_int2`sizev']
				gen b_explib 			= _b[explib]
				gen se_explib 			= _se[explib]
				gen b_sizev 			= _b[`sizev']
				gen se_sizev 			= _se[`sizev']
				gen b_int2sizev 		= _b[int2`sizev']
				gen se_int2sizev 		= _se[int2`sizev'] 
				
				di "Temp saving m`x'`y'`samp'`sizev'"
				tempfile m`x'`y'`samp'`sizev'
				sa `m`x'`y'`samp'`sizev''
			restore
			}
		}
		}
		}
		}
		
		* Append all
		u `mExlnemp1997up91mktgt10', clear
		foreach samp in `allsamples'{
		foreach x in `regvars'{
		foreach sizev in `sizevars'{
			foreach y in 1997{
				cap append using `m`x'`y'`samp'`sizev''
			}
		}
		}
		}
		duplicates drop
		
		outsheet using "${monopsonies}/csv/`outdate'/firm_DD_interactions.csv", comma replace
		
}

	
