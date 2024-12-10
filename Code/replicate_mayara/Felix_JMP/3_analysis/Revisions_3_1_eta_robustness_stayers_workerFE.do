/*
	Firm-market level regressions
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

local outdate			= 20220623
local premiadate	= 20220623
local baseyear 		= 1991
local baseyear_n 	= 91

local eta_regs 			= 1

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"


local tarvars 		"lnT"
local allspecs 		"l"
local wagevars 		"lndp"
local allmodels 	"l"			/* b: back to 1985; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"cnae95"
local allsamp 		"all"


local mainwage 	"lndp"
local mainclust "cnae95"
local maintar	"lnT"

* Specification FEs
local labsorb "fe_ro"			/* When spec is m, absorb mmc-cbo942d */



***************************************************
***************** Eta regressions *****************
***************************************************	

if `eta_regs'==1{

	* Compute change in premia among strict stayers

	u "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_strict_stayers_workerFE`outdate'.dta", clear
	keep fakeid_firm mmc cbo942d year dprem_zrot
	ren dprem_zrot dprem_zro
	reshape wide dprem_zro, i(fakeid_firm mmc cbo942d) j(year)

	gen double chng91_lndp = dprem_zro1997 - dprem_zro1991

	tempfile newprem
	sa `newprem'
	
	u "${monopsonies}/sas/eta_changes_regsfile0.dta", clear
	drop chng91_lndp
	keep if year==1997

	* Merge with strict stayers wage premia

	merge 1:1 fakeid_firm mmc cbo942d using `newprem', ///
	keep(3) nogen keepusing(chng91_lndp)

	cap drop if inlist(cbo942d,31,22,37)
	
	* Cross-section FEs
	gegen fe_ro = group(mmc cbo942d)
		
	ren chng_lnTRAINS chng_lnT
	ren chng_lnErpTRAINS chng_lnE
	
	/* Flip sign for easier interpretation */
	replace chng_lnT = - chng_lnT
	replace chng_lnE = - chng_lnE
	
	gen double firm = fakeid_firm
	gen all = 1
	ren bemp w0
	
	local lhs chng91_lndp
	local rhs chng91_lnemp
	local inst chng_lnT
	
	ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) if all==1 & year==1997 [w=w0], savefirst saverf cluster(fakeid_firm) absorb(delta_ro =fe_ro) 
	
	* Store fixed effects for theta estimation in next step
	preserve
		keep fakeid_firm mmc cbo942d chng91_lndp chng91_lnemp delta_ro
		keep if !missing(delta_ro)

		ren chng91_lndp chng_wagevar
		ren chng91_lnemp chng_lnemp
		
		saveold "${monopsonies}/dta/coeffs/`outdate'/eta_change_delta_ro_strict_stayers_workerFE.dta", replace
	restore
	drop delta_ro
	
	local obs = e(N)
	unique fakeid_firm if e(sample)
	local firms = `r(unique)'
	unique cbo942d mmc if e(sample)
	local mkts = `r(unique)'
	
	local m_iv_b = _b[`rhs']
	local m_iv_se = _se[`rhs']
	
	mat first = e(first)

	estimates restore _ivreg2_`lhs'
	local m_rf_b = _b[`inst']
	local m_rf_se = _se[`inst']
	estimates restore _ivreg2_`rhs'
	local m_fs_b = _b[`inst']
	local m_fs_se = _se[`inst']
	local FS_F = first[4,1]
			
	reghdfe chng91_lndp chng91_lnemp if all==1 & year==1997 [w=w0], vce(cluster fakeid_firm) absorb(fe_ro) 
	local m_ols_b = _b[`rhs']
	local m_ols_se = _se[`rhs']
	
	mat coeffs = (	`m_ols_b',`m_ols_se',`m_iv_b',`m_iv_se', ///
				`m_rf_b',`m_rf_se',`m_fs_b',`m_fs_se', ///
				`FS_F',`obs',`firms',`mkts')

	clear
	svmat coeffs
	gen year 	= 1997
	gen spec    = "l"
	gen model   = "l"
	gen tariff  = "lnT"
	gen clust = "firm"
	gen samp  = "all"
	gen wagevar = "lndp"
	gen weight	= "w0"
	
	ren coeffs1 ols_b
	ren coeffs2 ols_se
	ren coeffs3 iv_b
	ren coeffs4 iv_se
	ren coeffs5 rf_b
	ren coeffs6 rf_se
	ren coeffs7 fs_b
	ren coeffs8 fs_se
	ren coeffs9 fs_F
	ren coeffs10 obs
	ren coeffs11 firms
	ren coeffs12 markets
	keep if !missing(ols_b)

	outsheet using "${monopsonies}/csv/`outdate'/eta_change_regressions_strict_stayers_workerFE.csv", comma replace

}
