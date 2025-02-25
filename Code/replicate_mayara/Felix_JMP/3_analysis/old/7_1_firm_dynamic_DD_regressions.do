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

local setup  			= 0
local regressions   	= 1
local graphs 			= 1

*local regvars 		"lnemp dprem_zro wshare eshare"
local regvars 		"lnemp dprem_zro"

*local intvarsall 	"explib ice"
*local intvarsT 	"explib ice"
local intvarsall 	"explib"
local intvarsT 		"explib"

*local allsamples 	"all up91mkt"
local allsamples 	"all"

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"


local usesample = 0		/* Use 10% random sample of firms (for de-bugging) */


***********************
****** Setup **********
***********************

if `setup'==1{
	
	u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
	ren cnae10 cnae95
	keep year cnae95 ibgesubsector TRAINS ErpTRAINS
	* Note: ErpTRAINS only available for 1989 - 1998
	
	keep if !missing(TRAINS)
	foreach var of varlist TRAINS ErpTRAINS{
		replace `var' = `var'
		gen double ln`var' = ln(1+`var'/100)
		bys cnae95: egen double `var'1990 = max(cond(year==1990,`var',.))
		gen double ln`var'1990 = ln(`var'1990)
		drop `var'1990
		gsort cnae95 year
	}
	
	keep year cnae95 ibgesubsector lnTRAINS lnErpTRAINS lnTRAINS1990 lnErpTRAINS1990
	tempfile tariffs
	sa `tariffs'
	
	u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
	ren cnae10 cnae95
	keep if year==`baseyear' | year==1994
	keep if !missing(TRAINS) & !missing(ErpTRAINS)
	keep year cnae95 TRAINS ErpTRAINS
	sort cnae95 year
	foreach var of varlist TRAINS ErpTRAINS{
		replace `var' = `var'/100
		bys cnae95: gen double chng_ln`var' = ln(1+`var') - ln(1+`var'[_n-1])
	}
	keep if year==1994
	keep cnae95 chng*
	gduplicates drop
	
	tempfile t_change
	sa `t_change'
	
	u "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", clear
	keep mmc cbo942d ice_dwerp ice_dwtrains
	gduplicates drop
	tempfile market
	sa `market'
	
	u "${monopsonies}/dta/fakeid_importers_exporters_allyears_20191213.dta", clear
	bys fakeid_firm: gegen explib = max(cond(inrange(year,1990,1994),exporter,.))
	bys fakeid_firm: gegen bexp = max(cond(inlist(year,1990,1991),exporter,.))
	
	keep fakeid_firm explib bexp 
	gduplicates drop
	tempfile exporters
	sa `exporters'
	
	foreach sampfile in 1 0 {
		u "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta" if inrange(year, 1986,2000), clear
		
		* Drop occupations that are not private sector
		drop if mmc==13007 
		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
		drop if mmc_drop==1
		
		******* Sample restrictions *******
		if `sampfile'==1{
			preserve
				keep fakeid_firm
				gduplicates drop
				sample 10
				tempfile samp
				sa `samp'
			restore
			merge m:1 fakeid_firm using `samp', keep(3) nogen
		}
		
		* Merge in tariffs so can also restrict to tradables
		merge m:1 cnae95 year using `tariffs', keep(1 3) nogen
		replace lnTRAINS = lnTRAINS1990 if year<1990
		replace lnErpTRAINS = lnErpTRAINS1990 if year<1990
		
		* Long change in tariffs
		merge m:1 cnae95  using `t_change', keep(1 3) nogen		
		
		* Tradable sector dummies (ibgesub only included in TRAINS data at this point)
		gegen ibge = max(ibgesubsector), by(fakeid_firm)
		gen T = (ibge<14 | ibge==25)
		drop ibgesubsector
		ren ibge ibgesubsector
		drop ibgesubsector
		
		/* For untradables set change in tariffs to zero */
		replace chng_lnTRAINS 		= 0 if T==0
		replace chng_lnErpTRAINS 	= 0 if T==0
		
		keep if !missing(chng_lnTRAINS)
		
		* Compute firm baseline share
		preserve
			 gegen double sumearn = sum(totdecearn), by(mmc cbo942d year)
			 gen double wshare = totdecearn/sumearn
			 gegen double sumemp = sum(emp), by(mmc cbo942d year)
			 gen double eshare = emp/sumemp
			 
			 gegen double Tsumearn = sum(totdecearn) if T==1, by(mmc cbo942d year)
			 gen double Twshare = totdecearn/Tsumearn if T==1
			 gegen double Tsumemp = sum(emp) if T==1, by(mmc cbo942d year)
			 gen double Teshare = emp/Tsumemp if T==1
			 
			 keep fakeid_firm mmc year cbo942d eshare Teshare wshare Twshare
			 
			 tempfile shares
			 sa `shares'
		restore
		
		* Merge in base shares, replacing with zero if firm was not there
		merge m:1 fakeid_firm mmc year cbo942d using `shares', keep(1 3) nogen
		
		foreach var of varlist eshare Teshare wshare Twshare{
			gegen b`var' = max(cond(year==`baseyear',`var',.)), by(fakeid_firm mmc cbo942d)
		}
		
		* Dummy for whether firm-market pair existed in 1990
		gen double in`baseyear_n'mkt = !missing(beshare)
		
		* Dummy for whether firm existed in 1990
		gegen in`baseyear_n' = max(in`baseyear_n'mkt), by(fakeid_firm)
		
		foreach var of varlist beshare  bTeshare bwshare bTwshare{
			replace `var' = 0 if missing(`var')
		}
		
		* Keep if firm-market pair was in there in base year
		*keep if in`baseyear_n'==1
		keep if in`baseyear_n'mkt==1
		
		************************************
		* Flag Unique producers among tradable sector firms
		preserve
			keep if year==`baseyear'
			keep fakeid_firm cnae95 mmc cbo942d
			bys cnae95 mmc cbo942d: gegen producers = count(fakeid_firm)
			keep if producers==1
			keep fakeid_firm mmc cbo942d
			
			gen up`baseyear_n' = 1
			tempfile unique 
			sa `unique'
		restore	
		merge m:1 fakeid_firm mmc cbo942d using `unique', keep(1 3) nogen
		replace up`baseyear_n' = 0 if missing(up`baseyear_n')
		
		* Market that has unique producer 
		bys mmc cbo942d: gegen up`baseyear_n'mkt = max(up`baseyear_n')
		
		* Merge in market shocks
		merge m:1 mmc cbo942d using `market', keep(3) nogen
		
		* Merge in exporter dummies
		merge m:1 fakeid_firm using `exporters', keep(1 3) nogen
		foreach var of varlist explib bexp {
			replace `var' = 0 if missing(`var')
		}
		
		* Log employment
		gen double lnemp = ln(emp)
		
		compress
		saveold "${monopsonies}/sas/firm_DD_regsfile`sampfile'.dta", replace
		
	} /* End sampfile */
} 	/* End setup */

***************************************************
******************* Regressions *******************
***************************************************

if `regressions'==1{
	
	
		u "${monopsonies}/sas/firm_DD_regsfile`usesample'.dta", clear
		
		* Merge in earnings premia
		merge 1:1 fakeid_firm mmc cbo942d year using "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_`premiadate'.dta", keep(3) nogen
		preserve
			u "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", clear
			keep mmc cbo942d ice_dwtrains
			gduplicates drop
			tempfile dwice
			sa `dwice'
		restore
		merge m:1 mmc cbo942d using `dwice', keep(3) keepusing(ice_dwtrains) nogen
		
		ren ice_dwtrains		ice
		ren lnTRAINS 			lnT
		ren lnErpTRAINS 		lnE
		ren chng_lnTRAINS 		chng_lnT
		ren chng_lnErpTRAINS 	chng_lnE
		
		
		****** Invert the sign so it's easier to interpret ******
		replace ice 			= - ice
		replace chng_lnT 		= - chng_lnT
		replace chng_lnE 		= - chng_lnE	
		gen double interaction 	= - chng_lnT*ice
		
		drop ice_* *Erp* lnE
		
		drop lnT* up91 in91mkt in91 bTeshare bTwshare beshare bwshare Twshare Teshare bexp chng_lnE
		
		local ivars "fakeid_firm mmc cbo942d cnae95 chng_lnT explib interaction ice T up91mkt"
		
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
		
		gen double gt100 = (emp`baseyear'> 100)
		gen double lt10 = (emp`baseyear'< 10)
		
		foreach X in `intvarsall'{
		foreach var of varlist chng_lnT{
			gen double `var'_int_`X' = `var'*`X'
			replace `var'_int_`X' = -`var'_int_`X' if "`X'"=="ice"
		}
		}
		
		gen double baseemp = emp`baseyear'
		********************************************************************
		****************** Firm-market level Diff in Diff ******************
		********************************************************************
		
		keep `ivars' chng_lnT* ice* D* baseemp
		egen llm = group(cbo942d mmc)
		
		tempfile forregs
		sa `forregs'
		
		cap gen all = 1
			
		* Pre 1990 *
		foreach samp in `allsamples'{
			foreach x in `regvars'{
				forvalues y=1986/`pre'{
					areg D`x'`y' chng_lnT if `samp'==1 [w=baseemp], absorb(llm) cluster(fakeid_firm)
					preserve
						clear
						set obs 1
						gen var = "`x'"
						gen year = `y'
						gen samp = "`samp'"
						gen b 		= _b[chng_lnT]
						gen se 		= _se[chng_lnT]
						
						tempfile m`x'`y'`samp'
						sa `m`x'`y'`samp''
					restore
				}
			}
			
		
			* Post 1990 *
			foreach x in `regvars'{
		
				forvalues y=`next'/2000{
					areg D`x'`y' chng_lnT if `samp'==1 [w=baseemp], absorb(llm) cluster(fakeid_firm)
					preserve
						clear
						set obs 1
						gen var = "`x'"
						gen year = `y'
						gen samp = "`samp'"
						gen b 		= _b[chng_lnT]
						gen se 		= _se[chng_lnT]
						
						tempfile m`x'`y'`samp'
						sa `m`x'`y'`samp''
					restore
				}
			}
		} /* CLose samp */
		
		* Append all
		u `mlnemp1986all', clear
		foreach samp in `allsamples'{
		foreach x in `regvars'{
			forvalues y=1986/2000{
				cap append using `m`x'`y'`samp''
			}
		}
		}
		duplicates drop
		
		***** De-trending: fit line through pre- coefficients and plot distance *****
		foreach k in constant_b constant_se slope_b slope_se{
			gen double `k' = .
		}
		
		gen detrend_y1 =.
		gen detrend_y2 = .
		gen b_resid =.
		foreach samp in `allsamples'{
		foreach x in `regvars'{
			if regexm("`x'","dprem")>0{
				di "Var is wage `x', de-trend with 1986-1989"
				reg b year if year>=1986 &  year<=1989 & var=="`x'" & samp=="`samp'"
				replace detrend_y1 = 1986
				replace detrend_y2 = 1989
			}
			else{
				di "Var is wage `x', de-trend with 1986-1989"
				reg b year if year>=1986 &  year<=1989 & var=="`x'" & samp=="`samp'"
				replace detrend_y1 = 1986
				replace detrend_y2 = 1989
			}
			
			replace constant_b 	= _b[_cons] if var=="`x'"  & samp=="`samp'"
			replace constant_se = _se[_cons] if var=="`x'"  & samp=="`samp'"
			replace slope_b		= _b[year] if var=="`x'"  & samp=="`samp'"
			replace slope_se	= _se[year] if var=="`x'"  & samp=="`samp'"
			
			predict btemp_`x' if var=="`x'" & samp=="`samp'", resid
			replace b_resid =btemp_`x' if var=="`x'" & samp=="`samp'"
			drop  btemp_`x'
		}
		}
		gen se_resid = se
		ren b b_main
		ren se se_main
		keep year var samp b_main se_main constant_b constant_se slope_b slope_se b_resid se_resid detrend_y*
		
		saveold "${monopsonies}/dta/coeffs/`outdate'/firm_dynamic_DD.dta", replace
		
		*************************************************************
		****************** Heterogeneity regressions ****************
		*************************************************************
		
		u `forregs', clear
		cap gen all = 1
		
		foreach samp in `allsamples'{
			
			* Pre 1990 *
			foreach i in `intvars`samp''{
				foreach x in `regvars'{
					forvalues y=1986/`pre'{

						areg D`x'`y' chng_lnT chng_lnT_int_`i' `i' if `samp'==1 [w=baseemp], absorb(llm) cluster(fakeid_firm)
						preserve
							clear
							set obs 1
							gen var 		= "`x'"
							gen year 		= `y'
							gen intvar 		= "`i'"
							gen samp		= "`samp'"
							gen b 			= _b[chng_lnT]
							gen se 			= _se[chng_lnT]
							gen b_int 		= _b[chng_lnT_int_`i']
							gen se_int		= _se[chng_lnT_int_`i']
							gen b_intvar 		= _b[`i']
							gen se_intvar		= _se[`i']
							
							tempfile int`x'`y'`i'`samp'
							sa `int`x'`y'`i'`samp''
						restore
			
					}
				}
			}
		
			* Post 1990 *
			foreach i in `intvars`samp''{
			foreach x in `regvars'{
				forvalues y=`next'/2000{
				
					areg D`x'`y' chng_lnT chng_lnT_int_`i' `i' if `samp'==1, absorb(llm) cluster(fakeid_firm)
					preserve
						clear
						set obs 1
						gen var 		= "`x'"
						gen year 		= `y'
						gen intvar 		= "`i'"
						gen samp		= "`samp'"
						gen b 			= _b[chng_lnT]
						gen se 			= _se[chng_lnT]
						gen b_int 		= _b[chng_lnT_int_`i']
						gen se_int		= _se[chng_lnT_int_`i']
						gen b_intvar 	= _b[`i']
						gen se_intvar	= _se[`i']	
							
						tempfile int`x'`y'`i'`samp'
						sa `int`x'`y'`i'`samp''
					restore
				}
			}
			}
		} /* Close sample */
		
		* Append all
		u `intlnemp1986expliball', clear
		foreach samp in `allsamples'{
		foreach i in `intvarsall'{
		foreach x in `regvars'{
			forvalues y=1986/2000{
				cap append using `int`x'`y'`i'`samp''
			}
		}
		}
		}
		
		***** De-trending: fit line through pre- coefficients and plot distance *****
		foreach k in constant_b constant_se slope_b slope_se ///
					constant_b_int constant_se_int slope_b_int slope_se_int{
			gen double `k' = .
		}
		
		gen detrend_y1 =.
		gen detrend_y2 = .
		gen b_main_resid =.
		gen b_int_resid = .
		
		foreach samp in `allsamples'{
		foreach i in `intvarsall'{
		foreach x in `regvars'{
			* Main coefficient
			if regexm("`x'","dprem")>0{
				di "Detrending `x', samp==`samp', de-trend with 1986-1989"
				reg b year if year>=1986 &  year<=1989 & var=="`x'" & samp=="`samp'"
				replace detrend_y1 = 1986
				replace detrend_y2 = 1989
			}
			else{
				di "Var is `x', samp==`samp',  de-trend with 1986-1989"
				reg b year if year>=1986 &  year<=1989 & var=="`x'" & samp=="`samp'"
				replace detrend_y1 = 1986
				replace detrend_y2 = 1989
			}
			
			replace constant_b 	= _b[_cons] if var=="`x'" 	& samp=="`samp'" & intvar=="`i'"
			replace constant_se = _se[_cons] if var=="`x'" 	& samp=="`samp'" & intvar=="`i'"
			replace slope_b		= _b[year] if var=="`x'" 	& samp=="`samp'" & intvar=="`i'"
			replace slope_se	= _se[year] if var=="`x'" 	& samp=="`samp'" & intvar=="`i'"
			
			predict btemp_`x' if var=="`x'" & samp=="`samp'" & intvar=="`i'", resid
			replace b_main_resid =btemp_`x' if var=="`x'" & samp=="`samp'" & intvar=="`i'"
			drop  btemp_`x'
			
			* Interaction
			if regexm("`x'","dprem")>0{
				di "Var is `x' interaction, samp==`samp',  de-trend with 1986-1989"
				reg b_int year if year>=1986 &  year<=1989 & var=="`x'" & samp=="`samp'"
			}
			else{
				di "Var is `x' interaction, samp==`samp',  de-trend with 1986-1989"
				reg b_int year if year>=1986 &  year<=1989 & var=="`x'" & samp=="`samp'"
			}
			
			replace constant_b_int 	= _b[_cons] if var=="`x'" 	& samp=="`samp'" & intvar=="`i'"
			replace constant_se_int = _se[_cons] if var=="`x'" 	& samp=="`samp'" & intvar=="`i'"
			replace slope_b_int		= _b[year] if var=="`x'" 	& samp=="`samp'" & intvar=="`i'"
			replace slope_se_int	= _se[year] if var=="`x'" 	& samp=="`samp'" & intvar=="`i'"
			
			predict btemp_`x' if var=="`x'" & samp=="`samp'" & intvar=="`i'", resid
			replace b_int_resid =btemp_`x' if var=="`x'" & samp=="`samp'"  & intvar=="`i'"
			drop  btemp_`x'
		}
		}
		}
		
		ren b 	b_main_main
		ren se 	se_main_main
		gen se_main_resid = se_main_main
		
		ren b_int b_int_main
		ren se_int se_int_main
		gen se_int_resid = se_int_main
		
		keep year var intvar samp b_*main se_*main constant_*b constant_*se slope_*b slope_*se b_*resid se_*resid detrend_y*
		
		
		duplicates drop
		saveold "${monopsonies}/dta/coeffs/`outdate'/firm_dynamic_DD_het.dta", replace
		
		*************************************************************
		****************** Regressions with Firm FE *****************
		*************************************************************
		
		u `forregs', clear
	
		cap gen all = 1
		
		foreach samp in `allsamples'{
			
			* Pre 1990 *
				foreach x in `regvars'{
					forvalues y=1986/`pre'{
						reghdfe D`x'`y'  interaction if `samp'==1, absorb(llm fakeid_firm) cluster(llm)
						preserve
							clear
							set obs 1
							gen var 	= "`x'"
							gen samp 	= "`samp'"
							gen year 	= `y'
							gen b 		= _b[interaction]
							gen se 		= _se[interaction]
							
							tempfile int`x'`y'int`samp'
							sa `int`x'`y'int`samp''
						restore
					}
				}
		
			* Post 1990 *
			foreach x in `regvars'{
		
				forvalues y=`next'/2000{
					reghdfe D`x'`y' interaction if `samp'==1, absorb(llm fakeid_firm) cluster(llm)
					preserve
						clear
						set obs 1
						gen var 	= "`x'"
						gen samp 	= "`samp'"
						gen year 	= `y'
						gen b 		= _b[interaction]
						gen se 		= _se[interaction]
							
						tempfile int`x'`y'int`samp'
						sa `int`x'`y'int`samp''
					restore
				}
			}
			
		} /* Close sample */
		
		* Append all
		u `intlnemp1986intall', clear
		foreach samp in `allsamples'{
		foreach x in `regvars'{
			forvalues y=1986/2000{
				cap append using `int`x'`y'int`samp''
			}
		}
		}
		duplicates drop
		
		***** De-trending: fit line through pre- coefficients and plot distance *****
		foreach k in constant_b constant_se slope_b slope_se{
			gen double `k' = .
		}
		
		gen detrend_y1 =.
		gen detrend_y2 = .
		gen b_resid =.
		foreach samp in `allsamples'{
		foreach x in `regvars'{
			if regexm("`x'","dprem")>0{
				di "Var is wage `x', de-trend with 1986-1989"
				reg b year if year>=1986 &  year<=1989 & var=="`x'" & samp=="`samp'"
				replace detrend_y1 = 1986
				replace detrend_y2 = 1989
			}
			else{
				di "Var is wage `x', de-trend with 1986-1990"
				reg b year if year>=1986 &  year<=1990 & var=="`x'" & samp=="`samp'"
				replace detrend_y1 = 1986
				replace detrend_y2 = 1990
			}
			
			replace constant_b 	= _b[_cons] if var=="`x'" 	& samp=="`samp'"
			replace constant_se = _se[_cons] if var=="`x'" 	& samp=="`samp'"
			replace slope_b		= _b[year] if var=="`x'" 	& samp=="`samp'"
			replace slope_se	= _se[year] if var=="`x'" 	& samp=="`samp'"
			
			predict btemp_`x' if var=="`x'" & samp=="`samp'", resid
			replace b_resid =btemp_`x' if var=="`x'" & samp=="`samp'" 
			drop  btemp_`x'
		}
		}
		gen se_resid = se
		ren b b_main
		ren se se_main
		keep year var samp b_main se_main constant_b constant_se slope_b slope_se b_resid se_resid detrend_y*
		
		saveold "${monopsonies}/dta/coeffs/`outdate'/firm_dynamic_DD_firmFE.dta", replace
}

****************************
********* Graphs ***********
****************************

if `graphs'==1{
	
	local eshare 	"Firm employment share in LLM"
	local wshare 	"Firm wagebill share in LLM"
	local lnemp		"Log firm employment in LLM"
	local snemp		"Log(1+ firm employment in LLM)"
	local dprem_zro	"Firm December wage premium in LLM"
	
	*******************************************************
	******************* Firm FE regs **********************
	*******************************************************
	foreach outcome in `regvars'{
		foreach gtype in main resid{
			u "${monopsonies}/dta/coeffs/`outdate'/firm_dynamic_DD_firmFE.dta", clear
			keep if var=="`outcome'" & samp=="all"
		
			ren b_`gtype' 	b
			ren se_`gtype' se
			
			gen double lb = b-1.96*se
			gen double ub = b+1.96*se
			count
			if `r(N)'>0{
				twoway  (rarea lb ub year if year>=1994, sort color(blue%25) lwidth(none)) ///
					(connect b year if year>=1994, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
					(rarea lb ub year if inrange(year, `baseyear'+1,1994), sort color(blue%25) lwidth(none)) ///
					(connect b year if inrange(year, `baseyear'+1,1994), lpattern(solid) msymbol(O) msize(small) color(blue)) ///
					(rarea lb ub year if year<= `baseyear'-1, sort color(gs10%25) lwidth(none)) ///
					(connect b year if year<= `baseyear'-1, lpattern(solid) msymbol(O) msize(small) color(black)), ///
					scheme(s1mono)  ///
					legend(off) xtitle("") ///
					xlabel(1986(1)2000, labsize(small)) yline(0, lpattern(dash) lcolor(gs8) ) ///
					xline( `baseyear', lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(blue)) ///
					ytitle("``outcome''") yscale(titlegap(*5))
				
				graph export "${monopsonies}/eps/`outdate'/firm_DD_`outcome'_firmFE_`gtype'.pdf", replace	
			}
		} /* Close gtype */
	}
	
	*******************************************************
	******************* Main regression *******************
	*******************************************************
	foreach outcome in `regvars'{
		foreach gtype in main resid{
			u "${monopsonies}/dta/coeffs/`outdate'/firm_dynamic_DD.dta", clear
			
			keep if var=="`outcome'" & samp=="all"
			ren b_`gtype' 	b
			ren se_`gtype' se
			gen double lb = b-1.96*se
			gen double ub = b+1.96*se
			count
			if `r(N)'>0{
				twoway  (rarea lb ub year if year>=1994, sort color(blue%25) lwidth(none)) ///
					(connect b year if year>=1994, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
					(rarea lb ub year if inrange(year, `baseyear'+1,1994), sort color(blue%25) lwidth(none)) ///
					(connect b year if inrange(year, `baseyear'+1,1994), lpattern(solid) msymbol(O) msize(small) color(blue)) ///
					(rarea lb ub year if year<= `baseyear'-1, sort color(gs10%25) lwidth(none)) ///
					(connect b year if year<= `baseyear'-1, lpattern(solid) msymbol(O) msize(small) color(black)), ///
					scheme(s1mono)  ///
					legend(off) xtitle("") ///
					xlabel(1986(1)2000, labsize(small)) yline(0, lpattern(dash) lcolor(gs8) ) ///
					xline( `baseyear', lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(blue)) ///
					ytitle("``outcome''") yscale(titlegap(*5))
				
				graph export "${monopsonies}/eps/`outdate'/firm_DD_`outcome'_`gtype'.pdf", replace	
			}
		}
	}
	

	*****************************************************************
	******************* Heterogeneity regressions *******************
	*****************************************************************
	foreach samp in `allsamples'{
	foreach outcome in `regvars'{
	foreach intvar in `intvars`samp''{
	foreach gtype in main resid{

		u "${monopsonies}/dta/coeffs/`outdate'/firm_dynamic_DD_het.dta", clear
		
		keep if var=="`outcome'" & intvar=="`intvar'" & samp=="`samp'"
		gen double lb = b_main_`gtype'-1.96*se_main_`gtype'
		gen double ub = b_main_`gtype'+1.96*se_main_`gtype'
		
		gen double lb_int = b_int_`gtype'-1.96*se_int_`gtype'
		gen double ub_int = b_int_`gtype'+1.96*se_int_`gtype'
		
		ren b_main_`gtype' b
		ren b_int_`gtype' b_int
		sort year
		

		twoway  (rarea lb ub year if year>=1994, sort color(blue%25) lwidth(none)) ///
				(connect b year if year>=1994, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
				(rarea lb ub year if inrange(year, `baseyear'+1,1994), sort color(blue%25) lwidth(none)) ///
				(connect b year if inrange(year, `baseyear'+1,1994), lpattern(solid) msymbol(O) msize(small) color(blue)) ///
				(rarea lb ub year if year<= `baseyear'-1, sort color(blue%25) lwidth(none)) ///
				(connect b year if year<= `baseyear'-1, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
				(rarea lb_int ub_int year if year>=1994, sort color(red%25) lwidth(none)) ///
				(connect b_int year if year>=1994, lpattern(solid) msymbol(O) msize(small) color(red)) ///
				(rarea lb_int ub_int year if inrange(year, `baseyear'+1,1994), sort color(red%25) lwidth(none)) ///
				(connect b_int year if inrange(year, `baseyear'+1,1994), lpattern(solid) msymbol(O) msize(small) color(red)) ///
				(rarea lb_int ub_int year if year<= `baseyear'-1, sort color(red%25) lwidth(none)) ///
				(connect b_int year if year<= `baseyear'-1, lpattern(solid) msymbol(O) msize(small) color(red)) ///
				, 	scheme(s1mono)  ///
					legend(	label(2 "Firm tariff change") ///
							label(8 "Firm tariff change x Interaction") ///
							symxsize(5)  order(2 8) ) xtitle("") ///
					xlabel(1986(1)2000, labsize(small)) yline(0, lpattern(dash) lcolor(gs8) ) ///
					xline( `baseyear', lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(blue)) ///
					ytitle("``outcome''") yscale(titlegap(*5))
		
		graph export "${monopsonies}/eps/`outdate'/firm_DD_`outcome'_het_`intvar'_`samp'_`gtype'.pdf", replace			
	
	}
	}
	}
	}
	
	
} /* Close graphs */
