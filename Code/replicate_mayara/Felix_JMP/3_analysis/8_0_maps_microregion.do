/* Maps of stuff onto Brazil map using Stepahnie Kestelman's code 
https://github.com/skestelman/brazil_maptile

Using mapping made by Stephanie Kestelman

plotplainblind

*/
clear all
set more off
set matsize 11000
*ssc install unicode2ascii
*ssc install spmap
unicode encoding set "ISO-8859-9"
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


/* 1 map the more aggregated microregions based on mayarajmp

maptile_geolist
maptile_geohelp mayarajmp
maptile_geohelp brazil_uf
maptile_geohelp brazil_microregion
maptile_geohelp brazil_arranjo
maptile_geohelp brazil_muni2019

*/
/* Pick 2:
	- Machine installers and mechanics: 84
	- office administration : 39
	- Managers and supervisors of industrial production: 70
 */

local saveinput 	= 0
local map			= 1
local outdate		= 20210802


cap mkdir "${monopsonies}/eps/`graphsdate'"
cap mkdir "${monopsonies}/csv/`graphsdate'"

 if `saveinput'==1{
 
	 u "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", clear

	 keep mmc cbo942d ice_dwtrains_hf
	 gduplicates drop
	 replace ice_dwtrains_hf=0 if missing(ice_dwtrains_hf)
	 
	 merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
	 drop if mmc_drop==1
	
	 saveold "${monopsonies}/dta/mmc_cbo942d_ice_for_maps.dta", replace
	 
	 ** What are the markets with the largest changes? **
	sum ice_dwtrains_hf, detail
	keep if  ice_dwtrains_hf>`r(p1)' & ice_dwtrains_hf<`r(p99)'
	
	keep mmc cbo942d ice_dwtrains_hf
	duplicates drop
	
	gen double year =1991
	merge 1:m mmc cbo942d year using "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", keep(3) nogen
	collapse (sum) emp, by(mmc cbo942d ice_dwtrains_hf cnae95)
	
	outsheet using  "${monopsonies}/csv/`outdate'/cnae95_of_largest_ice_mmc_cbo942d.csv", comma replace
	 
 }
 

 if `map'==1{
	
	* RdGy < PiYG < PrGn < BuRd RdBU< < BrBG RdYlGn < < RdYlBu < BuYlRd << Terrain
	
	u "${monopsonies}/dta/mmc_cbo942d_ice_for_maps.dta", clear

	**** For cbo942d==x ****
	foreach x in 39 70 84 {
	preserve
		u "${monopsonies}/dta/mmc_cbo942d_ice_for_maps.dta", clear
		
		sum ice_dwtrains, detail
		
		keep if cbo942d==`x'
		di in red "Distribution for cbo942d==`x'"
		
		sum ice_dwtrains, detail
		replace ice_dwtrains = 0 if missing(ice_dwtrains)
		replace ice_dwtrains = - ice_dwtrains
		replace ice_dwtrains = round(100*ice_dwtrains)
		replace ice_dwtrains= . if inlist(mmc,13007)
		
		maptile ice_dwtrains , geography(mayarajmp) fcolor(Paired) ///
							legformat(%9.0g) ///
							cutvalues(0 5 10 20 30 40)  ///
							twopt(legend(label(2 "Not affected") ///
										 label(3 "0% - 5%") ///
										 label(4 "5% - 10%") /// 
										 label(5 "10% - 20%") ///
										 label(6 "20% - 30%") ///
										 label(7 "30% - 40%") ///
										 label(1 "Excluded") ///
										 order(2 3 4 5 6 7  1)))
		graph export "${monopsonies}/eps/`outdate'/maps_ice_trains_cbo942d_`x'.png", replace
	restore
	} 
}
