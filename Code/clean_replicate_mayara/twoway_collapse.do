*version 14.2
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

else if c(username)=="p13861161" & c(os)=="Windows" {
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara"
	*global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\clean_replicate_mayara"
	global dictionaries		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	*global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\monopsonies"
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\clean_replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/clean_replicate_mayara"
	global dictionaries		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdictionaries/harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	*global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies"
	global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/clean_replicate_mayara/monopsonies"
	global public			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/publicdata"
}

****************************************************************
** TWO-WAY COLLAPSE (GUAGE CONCORDANCE OF MARKET DEFINITIONS) **
****************************************************************

* Set firm & market IDs
global firmid "cnpj_raiz"
global mkt 	  "mmc cbo942d"

* Read in data 
u using "${monopsonies}/sas/worker_level.dta", clear

* Keep just 1991 for this collapse 
keep if year == 1991

* Keep if education category is well-defined
keep if !missing(educ)

* Keep only onbs with non-zero dec earnings
keep if earningsdecmw > 0
gen double lndecearn  = ln(earningsdecmw)

* Define tradable sector 
gen T = (ibgesubsector<14 | ibgesubsector==25) //  T is not constant within cnpj_raiz b/c firms can change ibgessubsector over time
bysort cnpj_raiz: egen T_1991 = max(T*(year==1991))

* Re-code tariff shock
replace chng_lnTRAINS = 0 if T==0				// XXBMS -- should we change this only for firms s.t. ~mi(chng_lnTRAINS)? 
count if mi(chng_lnTR) 							// 2 million obs that are missing chng_lnTRAINS but are allegedly tradable. Should these be missing or 0?
replace chng_lnTRAINS = 0 if mi(chng_lnTRAINS) 	// This gets us to ~18,000 markets but doesn't really change eta_hat

* Compute firm-market-year earnings premia, also save residualized wages  
reg     lndecearn i.female i.agegroup i.educ 
predict res_lndecearn, r 

gen double 	   firm_mkt_emp     = 1
collapse (sum) firm_mkt_emp earningsdecmw (mean) lndecearn res_lndecearn (firstnm) chng_lnTRAINS T_1991, by($firmid $mkt gamma)
gen double	   firm_mkt_emp_sq  = firm_mkt_emp^2
gen double     earningsdecmw_sq = earningsdecmw^2
gen double     lndecearn_sq 	= lndecearn^2
gen double     res_lndecearn_sq = res_lndecearn^2
gen double     chng_lnTRAINS_sq = chng_lnTRAINS^2
gen 		   n_firm      	    = 1 

collapse (sum) n_firm (mean) firm_mkt_emp firm_mkt_emp_sq earningsdecmw earningsdecmw_sq ///
	lndecearn lndecearn_sq res_lndecearn res_lndecearn_sq chng_lnTRAINS chng_lnTRAINS_sq ///
	T_1991, by($mkt gamma)

local strmkt = subinstr("${mkt}", " ", "", .)
save 			 twoway_collapse_gamma_`strmkt',     		  replace 
export delimited twoway_collapse_gamma_collapse_`strmkt'.csv, replace 



