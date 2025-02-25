/*
	Firms 1990 cross section - 
	exporters vs tradables vs others firm size
	
	# of exporters per CNAE95 and exporter employment by CNAE95
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


local outdate	= 20210802
local baseyear 	= 1991

local expsize   	= 1

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"

if `expsize'==1{

* Ibgesubsector names

import excel using "${dictionaries}/ibgesubsector_descriptions_PT.xls", firstrow clear
keep ibgesubsector *_trans
tempfile ibgedes
sa `ibgedes'

u "${public}/Trade flows/Brazil_trade_flows_by_cnae10.dta", clear
ren cnae10 cnae95

collapse (sum) usd1000, by( exports cnae95)
reshape wide usd1000, i(cnae95) j(exports)
ren usd10000 imports
ren usd10001 exports
replace imports = 0 if missing(imports)
replace exports = 0 if missing(exports)

tempfile trade
sa `trade'

u "${public}/Tariffs/tariffs_maindataset_long.dta", clear

ren cnae10 cnae95
keep if year==1991
keep cnae95 ibgesubsector TRAINS cnae10_des
duplicates drop cnae95, force
tempfile ibge
sa `ibge'
		
u "${monopsonies}/dta/fakeid_importers_exporters_allyears_20191213.dta", clear
keep if year==`baseyear'

gegen bexp = max(cond(year==`baseyear',exporter,.)), by(fakeid_firm)
gegen bimp = max(cond(year==`baseyear',importer,.)), by(fakeid_firm)

keep fakeid_firm  bexp
gduplicates drop
tempfile exporters
sa `exporters'

u "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", clear
keep if year==1991
ds, v(32)

* Total employment and average wage
gen double lnwage = ln(avgdecearn)

collapse (sum) emp (mean) lnwage, by(fakeid_firm cnae95)
gen double lnemp = ln(emp)

merge 1:1 fakeid_firm using `exporters', keep(1 3) nogen
replace bexp = 0 if missing(bexp)

merge m:1 cnae95 using `ibge', keep(1 3) nogen
gen T = !missing(ibgesubsector)

merge m:1 cnae95 using `trade', keep(1 3) nogen

merge m:1 ibgesubsector using `ibgedes', keep(1 3) nogen


* Within each sector, what's the exporter share (labor-wise)
preserve
	keep if T==1
	gen double expemp = emp if bexp==1
	replace expemp = 0 if missing(expemp)
	
	collapse (sum) emp expemp bexp (count) fakeid_firm, by(cnae95 TRAINS)
	gen double exp_empshare = expemp/emp
	gen double exp_firmshare = bexp/fakeid_firm
	
	unique cnae95
	unique cnae95 if exp_firmshare==0

	binscatter exp_empshare TRAINS, scheme(s1mono) mcolor(blue%25) ///
	ytitle("Share of sector workers employed at exporters") ///
	xtitle("Sector 1991 import tariff")
	graph export "${monopsonies}/eps/`outdate'/`baseyear'_exporter_empshare_TRAINS.pdf", replace
	
	binscatter exp_firmshare TRAINS, scheme(s1mono) mcolor(blue%25) ///
	ytitle("Share of sector firms that are exporters") ///
	xtitle("Sector 1991 Import Tariff")
	graph export "${monopsonies}/eps/`outdate'/`baseyear'_exporter_firmshare_TRAINS.pdf", replace
	
	twoway hist exp_empshare, scheme(s1mono) percent xtitle("Share of sector workers employed at exporters")
	graph export "${monopsonies}/eps/`outdate'/`baseyear'_exporter_empshare_histogram.pdf", replace
	
	twoway hist exp_firmshare, scheme(s1mono) percent xtitle("Share of sector firms that are exporters")
	graph export "${monopsonies}/eps/`outdate'/`baseyear'_exporter_firmshare_histogram.pdf", replace
restore


************* Exporters vs non-tradables exporters vs non-tradables *************

sum lnemp if bexp==1 & T==1, detail
local mexp = round(`r(mean)',0.1)
local exp = round(exp(`mexp'),1)

sum lnemp if bexp==0 & T==1, detail
local mno = round(`r(mean)',0.1)
local no = round(exp(`mno'),1)

sum lnemp if T==0, detail
local Nmno = round(`r(mean)',0.01)
local Nno = round(exp(`Nmno'),1)

twoway 	(kdensity lnemp if bexp==1 & T==1, color(red%30) recast(area) bwidth(0.45)) ///
		(kdensity lnemp if bexp==0 & T==1, color(blue%30) recast(area) bwidth(0.45)) ///
		(kdensity lnemp if T==0, color(gs10%30) recast(area) bwidth(0.45)), ///
		xline(`mexp', lpattern(dash) lcolor(red)) text(.25 `mexp' " Avg. `exp'" "employees", place(ne) color(red)) ///
		xline(`mno', lpattern(dash) lcolor(blue))  text(.27 `mno'  " Avg. `no'" "employees", place(ne) color(blue)) ///
		xline(`Nmno', lpattern(dash) lcolor(gs10))  text(.35 `Nmno'  " Avg. `Nno'" "employees", place(ne) color(gs10)) ///
		scheme(s1color) ///
		legend(label(1 "Exporters") label(2 "Non-exporting tradables") label(3 "Non-tradables") size(small) rows(1)) ///
		xtitle("`baseyear' log employment") ytitle("Density") ///
		xlabel(0(2)12) yscale(titlegap(*5))
		
graph export "${monopsonies}/eps/`outdate'/`baseyear'_exporter_size.pdf", replace

sum lnwage if bexp==1 & T==1, detail
local mexp = round(`r(mean)',0.01)
local exp = round(exp(`mexp'),1)

sum lnwage if bexp==0  & T==1, detail
local mno = round(`r(mean)',0.01)
local no = round(exp(`mno'),1)

sum lnwage if T==0, detail
local Nmno = round(`r(mean)',0.01)
local Nno = round(exp(`Nmno'),1)


twoway 	(kdensity lnwage if bexp==1 & T==1, color(red%30) recast(area) bwidth(0.25)) ///
		(kdensity lnwage if bexp==0 & T==1, color(blue%30) recast(area) bwidth(0.25)) ///
		(kdensity lnwage if T==0, color(gs10%30) recast(area) bwidth(0.25)), ///
		xline(`mexp', lpattern(dash) lcolor(red)) text(.45 `mexp' " `exp' x min wage", place(ne) color(red)) ///
		xline(`mno', lpattern(dash) lcolor(blue))  text(.55 `mno'  " `no' x min wage", place(nw) color(blue)) ///	
		xline(`Nmno', lpattern(dash) lcolor(gs10))  text(.70 `Nmno'  " `Nno' x min wage", place(nw) color(gs10))  ///
		scheme(s1color) ///
		legend(label(1 "Exporters") label(2 "Non-exporting tradables") label(3 "Non-tradables") size(small) rows(1)) ///
		xtitle("`baseyear' average log wage") ytitle("Density") ///
		xlabel(4(2)12) yscale(titlegap(*5))
		
graph export "${monopsonies}/eps/`outdate'/`baseyear'_exporter_avglnw.pdf", replace


}

