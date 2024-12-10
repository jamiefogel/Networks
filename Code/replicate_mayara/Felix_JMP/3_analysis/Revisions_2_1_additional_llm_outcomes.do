/*
	Revisions: To help discern concentration increase mechanism, add
		- Employment from non-exporting tradables that survived through 2000
		- Employment from non-exporting tradables that exited before 2000
*/

version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154

* ssc install vcemway

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


global minyear = 1986
global maxyear = 2000
local minyear = ${minyear}
local maxyear = ${maxyear}
local baseyear = 1991

local morevars		= 1

********************************************************************************
******** LLM-level regressions on earnings, employment and concentration *******
********************************************************************************

if `morevars'==1{

	u "${monopsonies}/dta/fakeid_importers_exporters_allyears_20191213.dta", clear
	keep if exporter==1 & year>=1991 & year<=1994
	keep fakeid_firm
	gduplicates drop
	tempfile explib
	sa `explib'

	u "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", clear

	des
	gen tradable = (ibgesubsector<=14 | ibgesubsector==25)
	keep fakeid_firm mmc cbo942d emp year tradable

	* Add obs for missing years
	reshape wide emp, i(fakeid_firm tradable mmc cbo942d) j(year)

	merge m:1 fakeid_firm using `explib', keep(1 3)
	gen explib = (_merge==3)
	drop _merge

	bys fakeid_firm mmc: egen double mmc_emp2000 = sum(emp2000)
	bys fakeid_firm cbo942d: egen double cbo_emp2000 = sum(emp2000)
	bys fakeid_firm: egen double tot_emp2000 = sum(emp2000)

	* Different types of exit by 2000
	gen double exit_llm 	= missing(emp2000)
	gen double exit_cbo942d = cbo_emp2000==0
	gen double exit_mmc		= mmc_emp2000==0
	gen double exit_all		= tot_emp2000==0

	* Summary stats by firms: percent of firms that exit at least one llm, mmc, cbo, exit completely
	preserve
		foreach var of varlist exit_*{
			bys fakeid_firm: egen max`var' = max(`var')
		}
		keep fakeid_firm tradable explib max*
		gduplicates drop

		sum max*
		di "Summary among non-exporting tradables"
		sum max* if tradable==1 & explib==0
	restore

	* From now on keep non-exporting tradables
	keep if tradable==1 & explib==0

	reshape long emp, i(fakeid_firm mmc cbo942d exit_*) j(year)

	foreach var of varlist exit_*{
		gen double emp_`var' = emp*`var'
	}
	drop emp
	collapse (sum) emp_*, by(year mmc cbo942d)
	label var emp_exit_llm "Emp from firms that exited mmc-cbo942d by 2000"
	saveold "${monopsonies}/sas/nonexpT_emp_byexit_mmc_cbo942d.dta", replace
}
