version 14.2
clear all
set more off
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


************ CNAE95 ***********

local outdate = 20210802
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/csv/`outdate'"

import excel using "${dictionaries}/cnae10_descriptions_PT.xls", firstrow clear
cap ren código cnae95
cap ren cdigo cnae95
keep cnae95 description
tempfile labels
sa `labels'

u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
ren cnae10 cnae95

keep year cnae95 ibgesubsector TRAINS ErpTRAINS
* Note: ErpTRAINS only available for 1989 - 1998

sum TRAINS if year==1990, detail
sum TRAINS if year==1994, detail

* Merge descriptions
merge m:1 cnae95 using `labels', keep(3) nogen


keep if !missing(TRAINS)
foreach var of varlist TRAINS ErpTRAINS{
	bys cnae95: egen double `var'1990 = max(cond(year==1990,`var',.))
	gsort cnae95 year
	gen double ln`var' = ln(`var'/100 + 1)
	gen double ln`var'1990 = ln(`var'1990/100 + 1)
}

keep if year==1994

gen double chng_lnT = lnTRAINS - lnTRAINS1990
gen double pct_chng = (TRAINS - TRAINS1990)/TRAINS1990

* Export all so can see
outsheet using "${monopsonies}/csv/`outdate'/cnae95_labels_chng_lnT.csv", comma replace

twoway scatter chng_lnT lnTRAINS1990, scheme(s1mono) mcolor(blue%25) mlwidth(none) ///
xtitle("Log (1 + 1990 tariff)") ytitle("1994 - 1990 change in log tariff")

graph export "${monopsonies}/eps/`outdate'/chng_lnT_vs_lnT1990_variation.pdf", replace

unique cnae95
twoway hist chng_lnT, scheme(s1mono) bin(75) color(blue%25) freq ///
xtitle("Log(1 + 1994 tariff)/Log(1 + 1990 tariff)")

graph export "${monopsonies}/eps/`outdate'/chng_lnT_variation.pdf", replace

************ Residual variation **********
u "${monopsonies}/sas/eta_changes_regsfile0.dta", clear

* Cross-section FEs
gegen fe_ro = group(mmc cbo942d)
	
ren chng_lnTRAINS chng_lnT

areg chng_lnT, absorb(fe_ro)
predict res_chng_lnT, res

twoway hist res_chng_lnT, scheme(s1mono) bin(75) color(blue%25) percent ///
xtitle("Residual variation in Log(1 + 1994 tariff)/Log(1 + 1990 tariff)")

graph export "${monopsonies}/eps/`outdate'/res_chng_lnT_variation.pdf", replace

