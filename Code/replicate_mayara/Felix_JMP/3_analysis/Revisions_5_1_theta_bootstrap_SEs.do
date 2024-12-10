/*
	Estimate bootstrapped SEs for the main specification
	Steps:
		1) Estimate eta
			- Keep eta and market FE
		2) Compute xi_zm using eta
		3) Compute CES labor supply index using eta + xi_zm
		4) Estimate theta-eta


	****** Eta estima matches
	****** Can't match theta estimate
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

local outdate		= 20220623
local premiadate	= 20210802
	
* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"


program boottheta
	
	* Step 1: estimate eta and save market FEs
		u "${monopsonies}/sas/eta_changes_regsfile0.dta", clear
		keep if year==1997
		cap drop if inlist(cbo942d,31,22,37)
		drop if inlist(mmc,13007)
		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
		drop if mmc_drop==1

		replace chng_lnT = - chng_lnT
		ren bemp w0
		gegen double fe_ro = group(mmc cbo942d)

		* Store eta estimate
		qui ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) [w=w0], absorb(delta_ro = fe_ro)
		local eta_inverse = _b[chng91_lnemp]
		local eta = 1/`eta_inverse'

		keep if !missing(delta_ro)
		keep mmc cbo942d delta_ro
		gduplicates drop

		* Grab firm-market pairs in the eta_change regressions
		preserve
			u "${monopsonies}/sas/eta_changes_regsfile0.dta", clear			
			keep fakeid_firm mmc cbo942d 
			cap drop if inlist(cbo942d,31,22,37)
			gduplicates drop 
			tempfile pairs
			sa `pairs'
		restore
		
	* Step 2: compute xi_zm (do not restrict to the pairs with delta_ro yet)
		merge 1:m mmc cbo942d using "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", keep(3) nogen
		keep if year==1991 | year==1997

		merge m:1 fakeid_firm mmc cbo942d using `pairs', keep(3) nogen
		merge 1:1 fakeid_firm mmc cbo942d year using "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_`premiadate'.dta", ///
		keepusing(dprem_zro) keep(3) nogen
		ren dprem_zro lndp

		gegen double fe_ro = group(mmc cbo942d)

		qui areg lndp if year==1991, absorb(fe_ro)
		predict lndpres1991, resid

		qui areg lndp if year==1997, absorb(fe_ro)
		predict lndpres1997, resid

		gen double lndpres = lndpres1997
		replace lndpres = lndpres1991 if year==1991
		drop lndpres1991 lndpres1997

		keep if !missing(lndpres)
		gen double lnemp = ln(emp)
		gen double lnxi_zrot = (lndpres - `eta_inverse'*lnemp)/(1+`eta')

		keep if !missing(lnxi_zrot)
		keep year fakeid_firm mmc cbo942d lnxi_zrot emp delta_ro

		reshape wide lnxi_zrot emp, i(fakeid_firm mmc cbo942d delta_ro) j(year)

		foreach y in 1997 1991 {
			gen double xi_zro`y' 		= exp(lnxi_zrot`y')
			gen double product`y'		= (emp`y'*xi_zro`y')^((`eta'+1)/`eta')
			gegen double Sum`y'			= sum(product`y'), by(mmc cbo942d)
		}
		
		gen double chng_Lro = (`eta'/(`eta'+1))*(ln(Sum1997) - ln(Sum1991) )
		
		keep mmc cbo942d chng_Lro delta_ro
		gduplicates drop

		unique mmc cbo942d
		sum chng_Lro, detail
		
		* Step 3: Estimate theta
		merge 1:m mmc cbo942d using "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", keepusing(ice_dwtrains_hf) keep(1 3) nogen
 	
 		ren ice_dwtrains_hf iceH
 		keep mmc cbo942d chng_Lro delta_ro iceH
 		gduplicates drop

		replace iceH = -iceH
		replace iceH = 0 if missing(iceH)
		gegen double fe_ro = group(mmc cbo942d)

		qui ivreg2 delta_ro (chng_Lro = iceH), first rf

		local gap = _b[chng_Lro]
		return scalar gap = `gap' - `eta_inverse'
end

bootstrap r(gap), reps(1000): boottheta

