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

	* Firm level shocks
	u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
	keep cnae10 TRAINS year
	reshape wide TRAINS, i(cnae10) j(year)
	gen double chng_lnT = ln(1+TRAINS1994/100) - ln(1+TRAINS1990/100)
	ren cnae10 cnae95
	tempfile shocks
	sa `shocks'

	* Get all firms that were there at baseline
	u "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", clear
	keep if inlist(year,1991,1997)

	* Drop manaus and other mmcs dropped by DK (2017)
	drop if inlist(mmc,13007)
	
	merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
	drop if mmc_drop==1
	
	cap drop if inlist(cbo942d,31,22,37)

	merge m:1 cnae95 using `shocks', keep(1 3) nogen

	gen tradable = 1 if !missing(chng_lnT)
	replace chng_lnT = 0 if missing(chng_lnT)
	keep fakeid_firm mmc cbo942d chng_lnT emp year totdecearnmw tradable

	reshape wide emp totdecearnmw, i(fakeid_firm mmc cbo942d chng_lnT tradable) j(year)
	
	foreach y in 1991 1997{
		replace	 emp`y'=0 if missing(emp`y')
		replace totdecearnmw`y'=0 if missing(totdecearnmw`y')	
	}
	
	* Exiters
	gen exitedmkt = (emp1991>0 & emp1997==0)
	bys fakeid_firm: egen double exited = min(exitedmkt)

	* Entrants
	gen enteredmkt = (emp1991==0 & emp1997>0)
	bys fakeid_firm: egen double entered = min(enteredmkt)

	keep if exited==1 | entered==1 

	* Inv hyp sign
	gen double chng91_lndp = ln(1+sqrt(totdecearnmw1997^2+1)) - ln(1+sqrt(totdecearnmw1991^2+1))
	gen double chng91_lnemp = ln(1+sqrt(emp1997^2+1)) - ln(1+sqrt(emp1991^2+1))

	ren emp1991 bemp	/* baseline employment */
	ren emp1997 oemp	/* outyear employment */
	keep fakeid_firm mmc cbo942d chng91_lnemp chng91_lndp chng_lnT exited entered bemp oemp tradable

	gduplicates drop
	tempfile exitentry
	sa `exitentry'

	u "${monopsonies}/sas/eta_changes_regsfile0.dta", clear
	keep if year==1997
	cap drop if inlist(cbo942d,31,22,37)
	ren chng_lnTRAINS chng_lnT
	ren T tradable

	keep fakeid_firm mmc cbo942d chng91_lnemp chng91_lndp chng_lnT bemp tradable
	* Original sample contains 650 firms with missing chng91_lnemp and chng91_lndp that
	* are actually entrants. Drop before appending
	drop if missing(chng91_lnemp)

	gen survivers = 1
	sum chng_lnT, detail
	append using `exitentry'

	isid fakeid_firm mmc cbo942d
	
	unique fakeid_firm if survivers	
	unique fakeid_firm if exited
	unique fakeid_firm if entered

	* Cross-section FEs
	gegen fe_ro = group(mmc cbo942d)

	/* Flip sign for easier interpretation */
	replace chng_lnT = - chng_lnT
	
	ren bemp w0
	
	local lhs chng91_lndp
	local rhs chng91_lnemp
	local inst chng_lnT

	* Replicate base - all good
		ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) if survivers==1 [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
		eststo IVsurvivers 
		mat first = e(first)
		estadd local FS_F = first[4,1]

		unique fakeid_firm if e(sample)
		matrix unique = r(unique)
		estadd local firms = unique[1,1]

		unique fe_ro if e(sample)
		matrix unique = r(unique)
		estadd local mkts = unique[1,1]

		estimates restore _ivreg2_chng91_lndp
		eststo RFsurvivers 

		estimates restore _ivreg2_chng91_lnemp
		eststo FSsurvivers 

	*********** Next ***************
	* Survivers + exiters
	ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) if ( survivers==1 | exited==1 ) [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
	eststo IVaddexit
	mat first = e(first)
	estadd local FS_F = first[4,1]

	unique fakeid_firm if e(sample)
	matrix unique = r(unique)
	estadd local firms = unique[1,1]

	unique fe_ro if e(sample)
	matrix unique = r(unique)
	estadd local mkts = unique[1,1]

	estimates restore _ivreg2_chng91_lndp
	eststo RFaddexit 

	estimates restore _ivreg2_chng91_lnemp
	eststo FSaddexit 

	* Survivers + exiters + entrants
	replace w0 = oemp if entered==1  /* Entrants would have zero weight so consider outcome weight as proxy */
	ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) if ( survivers==1 | exited==1 | entered==1) [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
	eststo IVaddentry
	mat first = e(first)
	estadd local FS_F = first[4,1]

	unique fakeid_firm if e(sample)
	matrix unique = r(unique)
	estadd local firms = unique[1,1]

	unique fe_ro if e(sample)
	matrix unique = r(unique)
	estadd local mkts = unique[1,1]

	estimates restore _ivreg2_chng91_lndp
	eststo RFaddentry 
	
	estimates restore _ivreg2_chng91_lnemp
	eststo FSaddentry 
	
	esttab IVsurvivers IVaddexit IVaddentry using "${monopsonies}/csv/`outdate'/eta_iv_exit_entry", ///
	rtf replace se label mtitles("IVsurvivers" "IVaddexit" "IVaddentry") scalars(FS_F firms mkts)

	esttab RFsurvivers FSsurvivers RFaddexit FSaddexit RFaddentry FSaddentry using "${monopsonies}/csv/`outdate'/eta_rf_fs_exit_entry", ///
	rtf replace se label mtitles("RFsurvivers" "FSsurvivers" "RFaddexit" "FSaddexit" "RFaddentry" "FSaddentry")

}
