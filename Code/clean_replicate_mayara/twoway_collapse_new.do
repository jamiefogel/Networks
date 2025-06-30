version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154

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
	global public			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/publicdata"
}

* Set macros
global outdate = 20250407
global mkt 	   "mmc cbo942d gamma"
global g3states "" // "" 

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/${outdate}"
cap mkdir "${monopsonies}/eps/${outdate}"
cap mkdir "${monopsonies}/dta/coeffs/${outdate}"



****************************************************************
** TWO-WAY COLLAPSE (GUAGE CONCORDANCE OF MARKET DEFINITIONS) **
****************************************************************

	u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
	ren cnae10 cnae95
	keep if (year == 1990 | year == 1994) & !missing(TRAINS)
	keep year cnae95 ibgesubsector TRAINS 
	sort cnae95 year
	replace TRAINS = TRAINS/100
	bys cnae95: gen double chng_lnTRAINS = ln(1+TRAINS) - ln(1+TRAINS[_n-1])
	keep if year == 1994
	keep cnae95 ibgesubsector chng_lnTRAINS
	gduplicates drop
	tempfile t_change
	sa 		`t_change'

	u "${monopsonies}/sas/rais_for_earnings_premia1991_gamma${g3states}.dta", clear
	isid fakeid_worker
	drop if mmc==13007 | mmc==23014
	
	* Merge tariff shocks 
	merge m:1 cnae95 using `t_change', keep(1 3) 
	gegen 	ibge = max(ibgesubsector), by(fakeid_firm)
	gen 	T 	 = (ibge<14 | ibge==25)	
	replace chng_lnTRAINS 		= 0 if T==0
	drop _merge ibgesubsector
	
	* Keep if education category is well-defined
	keep if !missing(educ)
	
	* Keep only onbs with non-zero dec earnings
	keep if earningsdecmw > 0
	gen double lndecearn = ln(earningsdecmw)
	
	* Use same control and ranges as DK 
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
	
	* Residual log wages 
	reg     lndecearn female age2-age5 educ2-educ9
	predict res_lndecearn, r 
	
	* Firm wage premium
	gegen double fe_zro = group(fakeid_firm ${mkt})	
	reghdfe lndecearn female age2-age5 educ2-educ9, absorb(lndp=fe_zro) noconstant keepsingletons
	keep if !missing(lndp) 
	
	* Collapse 
	gen 	 	   firm_mkt_emp     = 1
	collapse (sum) firm_mkt_emp earningsdecmw (mean) lndecearn res_lndecearn ///
				   lndp (firstnm) chng_lnTRAINS T, by(fakeid_firm ${mkt})
	
	gen double	   firm_mkt_emp_sq  = firm_mkt_emp^2
	gen double     earningsdecmw_sq = earningsdecmw^2
	gen double     lndecearn_sq 	= lndecearn^2
	gen double     res_lndecearn_sq = res_lndecearn^2
	gen double     lndp_sq 			= lndp^2
	gen double     chng_lnTRAINS_sq = chng_lnTRAINS^2
	gen 		   n_firm      	    = 1 
	collapse (sum) n_firm (mean) firm_mkt_emp firm_mkt_emp_sq earningsdecmw ///
		earningsdecmw_sq lndecearn lndecearn_sq lndp lndp_sq res_lndecearn  ///
		res_lndecearn_sq chng_lnTRAINS chng_lnTRAINS_sq T, by(${mkt})

	duplicates drop
	outsheet using "${monopsonies}/csv/${outdate}/twoway_collapse${g3states}.csv", comma replace
