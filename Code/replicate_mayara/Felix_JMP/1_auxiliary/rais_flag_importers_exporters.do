/*
	Mayara Felix
	
	* 	Match list of importers and exporters from MDIC
		to RAIS. Uses identified data.
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



local import_lists 			= 0
local check_overlap 		= 0		/* Are importers and exporters the same firms? */
local match_to_real_IDs		= 1		/* Match list to real IDs */

local outdate = 20190502

** ID links **

******* Bring in list of importers and exporters from MDIC *******

if `import_lists'==1{

	* Exporters 1990 - 1996
	import excel using "${public}/MDIC/CNPJ level - exporters and importers/1990-1996/Operadores.xlsx", firstrow clear
	
	* Keep only CNPJs (1990-1996 lists contain CPF of individuals importing/exporting)
	* CNPJs are 14-digit numbers
	keep if length(CO_OPER)==14
	ren CO_OPER estabid_cnpj
	ren CO_ANO year
	destring year, replace

	keep estabid_cnpj year 

	duplicates drop
	gen exporter = 1
	tempfile exp9096
	sa `exp9096'
	
	* Importers 1990 (1991-1996 not availble - see spreadsheet)
	import excel using "${public}/MDIC/CNPJ level - exporters and importers/1990-1996/Operadores IMP.XLSX", firstrow clear
	
	keep if length(CO_OPER)==14
	ren CO_OPER estabid_cnpj
	ren CO_ANO year
	destring year, replace
	
	drop if inrange(year,1991,1996)
	
	keep estabid_cnpj year 
	
	duplicates drop
	gen importer = 1
	tempfile imp9096
	sa `imp9096'
	
	* Exporters and importers 1997-2017
	forvalues y=1997/2017{
		
		* Exporters
		import excel using "${public}/MDIC/CNPJ level - exporters and importers/EMPRESAS_CADASTRO_`y'.xlsx", ///
		sheet(EXP_CNPJ14) clear
		
		* Keep CNPJs
		ren A estabid_cnpj
		keep if regexm(estabid_cnpj,"^[0-9]")
		keep estabid_cnpj
		gen year = `y'
		
		assert length(estabid_cnpj)==14
		
		gen exporter = 1
		tempfile exp`y'
		sa `exp`y''
		
		* Importers
		import excel using "${public}/MDIC/CNPJ level - exporters and importers/EMPRESAS_CADASTRO_`y'.xlsx", ///
		sheet(IMP_CNPJ14) clear
		
		* Keep CNPJs
		ren A estabid_cnpj
		keep if regexm(estabid_cnpj,"^[0-9]")
		keep estabid_cnpj
		gen year = `y'
		
		assert length(estabid_cnpj)==14
		
		gen importer = 1
		tempfile imp`y'
		sa `imp`y''
		
	}
	
	
	******** Combine all ************
	
	* Exporters
	u `exp9096', clear
	forvalues y=1997/2017{
		append using `exp`y''
	}
	
	duplicates drop
	tempfile allexporters
	sa `allexporters'
	
	* Importers
	u `imp9096', clear
	forvalues y=1997/2017{
		append using `imp`y''
	}
	
	duplicates drop
	
	* Merge exporters and importers
	merge 1:1 estabid_cnpj year using `allexporters'
	
	drop _merge
	
	order estabid_cnpj year importer exporter
	sort estabid_cnpj year
	
	tab year importer, m
	tab year exporter, m
	
	saveold "${public}/MDIC/CNPJ level - exporters and importers/importers_exporters_allyears.dta", replace

}

if `check_overlap'==1{


	u "${public}/MDIC/CNPJ level - exporters and importers/importers_exporters_allyears.dta", clear
	
	replace exporter = 0 if exporter==.
	replace importer = 0 if importer==.
	gen double isboth = exporter*importer
	
	collapse (sum) exporter importer isboth, by(year)
	
	cap mkdir "${monopsonies}/csv/`outdate'/"
	outsheet using "${monopsonies}/csv/`outdate'/exporters_importers_both_by_year.csv", comma replace
	

}


if `match_to_real_IDs'==1{
	
	u "${public}/MDIC/CNPJ level - exporters and importers/importers_exporters_allyears.dta", clear
	
	destring estabid_cnpj, replace
	format estabid_cnpj %14.0f 
	
	replace exporter = 0 if exporter==.
	replace importer = 0 if importer==.
	gen double isboth = exporter*importer

	ren estabid_cnpj estabid_cnpjcei 
	merge m:1 estabid_cnpjcei using "${encrypted}/output/dta/IDlinksrais/rais_estabid_fakeid_link.dta", ///
	keepusing(fakeid_firm fakeid_estab)
	keep if _merge==3
	drop _merge
	
	drop estabid_cnpj
	saveold  "${monopsonies}/dta/fakeid_importers_exporters_allyears.dta", replace
	
	/*

	estabid_cnpjcei double  %14.0f                Establishment ID
	mainid_firm     double  %14.0f                CNPJ first 8 digits or CEI
	mainid_estab    double  %14.0f                Full CNPJ first or CEI
	mainid_type     byte    %9.0g                 1=CNPJ 3=CEI else=treateded as unique ID
	fakeid_firm     long    %14.0f                Fake ID based on CNPJ first 8 digits or CEI
	fakeid_estab    double  %14.0f                Fake firm ID + 4-digit branch if CNPJ + ID
													type

	*/
}
