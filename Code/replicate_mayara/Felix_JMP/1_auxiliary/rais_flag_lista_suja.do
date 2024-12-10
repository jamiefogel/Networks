/*
	Mayara Felix
	
	* 	Match lista suja to RAIS by municiaplity
	* 	Many establishments are CPF-level farms
	*	Merge at CPNJ at later point - Need to clean excel first - import not bringing in wrapped cells
	
		
*/

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
	global encrypted 		"/proj/patkin/raismount_mfelix"
	global dictionaries		"/proj/patkin/raisdictionaries/harmonized"
	global deIDrais			"/proj/patkin/raisdeidentified"
	global monopsonies		"/proj/patkin/projects/monopsonies"
	global public			"/proj/patkin/publicdata"
}
* Mayara mounting locally on Mac
else if c(username)=="mayara"{
	global encrypted 		"/Volumes/raismount_mfelix"
	global dictionaries		"/Volumes/proj_atkin/raisdictionaries/harmonized"
	global deIDrais			"/Volumes/proj_atkin/raisdeidentified"
	global monopsonies		"/Volumes/proj_atkin/projects/monopsonies"
	global public			"/Volumes/proj_atkin/publicdata"
	
}
* Mayara mounting locally on Windows
else if c(username)=="Mayara"{
	global encrypted 		"Z:/"
	global dictionaries		"M:/raisdictionaries/harmonized"
	global deIDrais			"M:/raisdeidentified"
	global monopsonies		"M:/projects/monopsonies"
	global public			"M:/publicdata"
}



local import_lists 			= 1
local match_munic			= 0		

local outdate = 20190502

** ID links **

******* Bring in lista suja list *******

if `import_lists'==1{

	* Complete lista suja by municipality
	forvalues i=1995/2010{
		
		import excel using "${public}/other/ReporterBrasil - lista suja/`i'.xls", clear
		keep if regexm(A,"^[0-9]")>0
	
		keep A C D G
		ren A audit_year
		ren C state
		ren D municipality_name
		ren G freed_workers
		
		tempfile y`i'
		sa `y`i''
	}
	
	* 2011
	import excel using "${public}/other/ReporterBrasil - lista suja/2011.xls", clear
	
	keep if regexm(A,"^[0-9]")>0
	keep A C D H
	ren A audit_year
	ren C state
	ren D municipality_name
	ren H freed_workers
	
	tempfile y2011
	sa `y2011'
	
	forvalues j=2012/2014{
		
		import excel using "${public}/other/ReporterBrasil - lista suja/`j'.xlsx", clear
		
		keep if regexm(A,"^[0-9]")>0
		keep A C D H
		ren A audit_year
		ren C state
		ren D municipality_name
		ren H freed_workers
		
		tempfile y`j'
		sa `y`j''
	}
	
	* 2015
	import excel using "${public}/other/ReporterBrasil - lista suja/2015.xlsx", clear
	
	keep if regexm(A,"^[0-9]")>0
	keep A C D I
	ren A audit_year
	ren C state
	ren D municipality_name
	ren I freed_workers
	
	tempfile y2015
	sa `y2015'
	
	u `y1995', clear
	forvalues k=1996/2015{
		
		append using `y`k''
	}
	
	
	destring audit_year freed_workers, replace
	
	saveold "${public}/other/ReporterBrasil - lista suja/list_suja_allyears.dta", replace
}


