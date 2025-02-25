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

	**** Graph showing relationship between tradable sector share and avg wage and emp *****

	u "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", clear
	des
	gen double tshare = mkt_temp/mkt_emp
	egen double fe_ro = group(mmc cbo942d)

	* Remove year FEs
	* Bring in data on real wages to make this easier to see

	* Expres avg_wage in Reais instead of multiple of min wage
	gen month = 12
	merge m:1 year month using "${public}/IPEA/IPEA_minwage/ipea_minwage_1985_2000.dta", keep(3) nogen
	gen double avg_wage_reais = mkt_avgdearn*real
	areg avg_wage_reais, absorb(cbo942d)
	predict residwage, resid

	ren mkt_emp emp
	ren avg_wage_reais wage

	local emp "Total market employment"
	local wage "Market average December wage (2017 Reais)"

	foreach var of varlist wage emp{

		binscatter `var' tshare if inlist(year,1991,1997), absorb(cbo942d) nquantiles(50) by(year) ///
		scheme(s1mono) xtitle("Tradable sector employment share (net of occupation fixed effects)") ytitle("") ylabel(,angle(0)) ///
		legend(label(1 "1991") label(2 "1997")) ///
		subtitle(``var'', place(w) size(small))

		graph export "${monopsonies}/eps/`outdate'/binscatter_tshare_`var'_cboFE.eps", replace

		binscatter `var' tshare if inlist(year,1991,1997), absorb(mmc) nquantiles(50) by(year) ///
		scheme(s1mono) xtitle("Tradable sector employment share (net of microregion fixed effects)") ytitle("") ylabel(,angle(0)) ///
		 legend(label(1 "1991") label(2 "1997")) ///
		subtitle(``var'', place(w) size(small))

		graph export "${monopsonies}/eps/`outdate'/binscatter_tshare_`var'_mmcFE.eps", replace

	}
	
	u "${monopsonies}/sas/eta_changes_regsfile0.dta", clear
	keep if year==1997
	cap drop if inlist(cbo942d,31,22,37)
	ren chng_lnTRAINS chng_lnT
	ren T tradable

	keep fakeid_firm mmc cbo942d chng91_lnemp chng91_lndp chng_lnT bemp tradable
	* Original sample contains 650 firms with missing chng91_lnemp and chng91_lndp that
	* are actually entrants. Drop before appending
	drop if missing(chng91_lnemp)

	* Cross-section FEs
	gegen fe_ro = group(mmc cbo942d)

	/* Flip sign for easier interpretation */
	replace chng_lnT = - chng_lnT
	
	ren bemp w0
	
	local lhs chng91_lndp
	local rhs chng91_lnemp
	local inst chng_lnT

	* Replicate base - all good
	ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 

	*********************************************************************************************
	* How come the estimates are so stable? Graphs suggests importance of including non-tradables
	*********************************************************************************************

	* 1) Okay. Then does do the cross-sector tariff reductions actually matter?
	* Yes. Check IV if replace the tradable sector shocks with noise. You get noisy zero point estimates
	gen double noise = rnormal(0,1)
	gen double noiseT_zeroNT = noise
	replace noiseT_zeroNT = 0 if tradable==0

	ivreghdfe chng91_lndp (chng91_lnemp = noiseT_zeroNT) [w=w0], ///
	savefirst saverf cluster(fakeid_firm) absorb(fe_ro)

	eststo IV_Tnoise_NT0

	estimates restore _ivreg2_chng91_lndp
	eststo RF_Tnoise_NT0 

	estimates restore _ivreg2_chng91_lnemp
	eststo FS_Tnoise_NT0

	* Check IV if replace the tradable sector shocks with avg, min, max shocks
	* ssc install _GWTMEAN
	egen double avgshock = wtmean(cond(tradable==1,chng_lnT,.)), weight(w0)
	gen double avgshockT_zeroNT = avgshock
	replace avgshockT_zeroNT = 0 if tradable==0

	egen double maxshock = max(cond(tradable==1,chng_lnT,.))
	gen double maxshockT_zeroNT = maxshock
	replace maxshockT_zeroNT = 0 if tradable==0

	egen double minshock = min(cond(tradable==1,chng_lnT,.))
	gen double minshockT_zeroNT = minshock
	replace minshockT_zeroNT = 0 if tradable==0

	foreach stat in avg min max{
		ivreghdfe chng91_lndp (chng91_lnemp = `stat'shockT_zeroNT) [w=w0], ///
		savefirst saverf cluster(fakeid_firm) absorb(fe_ro)

		eststo IV_T`stat'_NT0

		estimates restore _ivreg2_chng91_lndp
		eststo RF_T`stat'_NT0 

		estimates restore _ivreg2_chng91_lnemp
		eststo FS_T`stat'_NT0
	}		

	* 1) Can we estimate eta with tradables only, by focusing on markets with only tradables?
	* 	This is not possible
	* 	There are only 13 mmc-occup pairs where there the only employers are
	* 	tradable sector firms. Most of these have only 1 firm and they are all
	* 	in mmc 160002
		/*
			bys mmc `cbo942d': egen onlyT = min(tradable)
			
			ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) if onlyT==1 & survivers==1 [w=w0], ///
			first rf cluster(fakeid_firm) absorb(fe_ro) 	
		*/

	* 	So if you condition the estimation on tradable sector firms only, you
	* 	are capturing the positive correlation that markets with more tradable
	* 	sector firms have higher employment and higher wages. This positive 
	*	relationship can be seen in the cross-section

	ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) [w=w0], ///
	savefirst saverf cluster(fakeid_firm) absorb(fe_ro)
	eststo IV_T

	estimates restore _ivreg2_chng91_lndp
	eststo RF_T 

	estimates restore _ivreg2_chng91_lnemp
	eststo FS_T

	** Combine all ***
	esttab IV_Tnoise_NT0 IV_Tavg_NT0 IV_Tmin_NT0 IV_Tmax_NT0 IV_T using "${monopsonies}/csv/`outdate'/eta_iv_IDsource", ///
	rtf replace se label mtitles("IV_Tnoise_NT0" "IV_Tavg_NT0" "IV_Tmin_NT0" "IV_Tmax_NT0" "IV_T")

	esttab RF_Tnoise_NT0 FS_Tnoise_NT0 RF_Tavg_NT0 FS_Tavg_NT0 RF_Tmin_NT0 FS_Tmin_NT0 RF_Tmax_NT0 FS_Tmax_NT0 RF_T FS_T ///
	using "${monopsonies}/csv/`outdate'/eta_rf_fs_IDsource", ///
	rtf replace se label ///
	mtitles("RF_Tnoise_NT0" "FS_Tnoise_NT0" "RF_Tavg_NT0" "FS_Tavg_NT0" "RF_Tmin_NT0" "FS_Tmin_NT0" "RF_Tmax_NT0" "FS_Tmax_NT0" "RF_T" "FS_T")

}
