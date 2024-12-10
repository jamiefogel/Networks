/*
	Mayara Felix
	
	Create mapping between CNAE10 to IBGE subsector
	Do this based on most frequent pairing
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
	global dictionaries		"/proj/patkin/raisdictionaries/harmonized"
	global deIDrais			"/proj/patkin/raisdeidentified"
}
* Mayara mounting locally on Mac
else if c(username)=="mayara"{
	global dictionaries		"/Volumes/raisdictionaries/harmonized"
	global deIDrais			"/Volumes/raisdeidentified"
}
* Mayara mounting locally on Windows
else if c(username)=="Mayara"{
	global dictionaries		"M:/raisdictionaries/harmonized"
	global deIDrais			"M:/raisdeidentified"
}

****************************************************************

u "${deIDrais}/dta/analysis/estab_industry_codes.dta", replace

collapse (count) fakeid_firm, by(cnae10 ibgesubsector)

* Keep the most common ibgesubsector for each cnae10
bys cnae10: egen max = max(fakeid_firm)
keep if max==fakeid_firm
duplicates drop

keep cnae10 ibgesubsector
order cnae10
saveold "${dictionaries}/rais_cnae10_to_ibgesubsector.dta", replace
