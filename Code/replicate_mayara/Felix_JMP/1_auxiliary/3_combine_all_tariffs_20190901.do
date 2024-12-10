/*
	Combine TRAINS, Erp< mercosur tariffs
*/
*ssc install rangestat
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

local maindataset				= 1 /* Main tariffs datset */
local tariffs_ln1plus			= 1 /* Tariff changes for key years */


local tariff_fy	= 1990		/* Tariff averages first year */
local tariff_ly	= 1994		/* Tariff averages last year */


local outdate = 20190901

**************************************************************
******************** Main CNAE10 tariffs dataset *************
**************************************************************

if `maindataset'==1{

	* CNAE description
	import excel using "${dictionaries}/cnae10_descriptions_PT.xls",  clear
	keep if _n>=2
	keep A C
	ren A cnae10
	ren C cnae10_des
	destring cnae10, replace
	
	tempfile desc
	sa `desc'
	
	u "${public}/Tariffs/cnae10_erp_1989_1998_fixed.dta", clear
	keep cnae10 year ErpTRAINS RealErpTRAINS
	tempfile erp
	sa `erp'
	
	***** Mercosur Tariffs ****
		u "${public}/Trade flows/Brazil_trade_flows_by_cnae10.dta", clear
		
		* Brazil exports to each Mercosur country
		keep if year==1989 & inlist(partner,"ARG","PRY","URY")
		keep if exports==1
		replace exports = exports*usd1000
		
		* Each country export share within each cnae10 category
		bys cnae10: egen double totex = sum(exports)
		gen double expshare = exports/totex
		
		keep partner cnae10  expshare
		
		tempfile merco
		sa `merco'
		
		u "${public}/Tariffs/ARG_TRAINScnae10_via_NCM_KUMEniv50.dta", clear
		append using "${public}/Tariffs/PRY_TRAINScnae10_via_NCM_KUMEniv50.dta"
		append using "${public}/Tariffs/URY_TRAINScnae10_via_NCM_KUMEniv50.dta"
		
		reshape long TRAINS, i(country cnae10 niv100 niv50 ibge) j(year)
		
		* Tariffs reported for all countries only for 1992 onwards
		drop if year<1992
		
		ren country partner
		merge m:1  partner cnae10 using `merco', keep(1 3) nogen
		
		replace expshare = 0 if missing(expshare)
		
		gen double temp = expshare*TRAINS
		collapse (sum) temp, by(cnae10 niv50 niv100 year)
		
		ren temp mercoTRAINS
		
		* Use 1992 tariff level for 1989-1991 
		forvalues y=1989/1991{
			preserve
				keep if year==1992
				replace year = `y'

				tempfile y`y'
				sa `y`y''
			restore
			
			append using `y`y''
		}
		
		tempfile allmerco
		sa `allmerco'
		
		u "${public}/Tariffs/BRA_TRAINScnae10_via_NCM_KUMEniv50.dta", clear
		drop country
		reshape long TRAINS Kume erpKume, i(cnae10 niv100 niv50 ibge) j(year)
		
		merge 1:m cnae10 niv50 niv100 year using `allmerco', keep(1 3) nogen
		
		replace mercoTRAINS = 0 if missing(mercoTRAINS) & !missing(TRAINS)
		
		order year cnae10 TRAINS mercoTRAINS niv100 niv50 Kume erpKume ibgesubsector
	
	merge 1:1 cnae10 year using `erp', keep(1 3) nogen
	merge m:1 cnae10 using `desc', keep(1 3) nogen
	
	label var TRAINS 					"CNAE10-level TRAINS Brazil import tariffs"
	label var mercoTRAINS 				"CNAE10-level TRAINS ARG-PRY-URY import tariffs weighed by 1989 BRA export share"
	
	label var ErpTRAINS					"CNAE10-level TRAINS Brazil effective rate of protection"
	label var RealErpTRAINS				"CNAE10-level TRAINS Brazil Xchange rate-adjusted effective rate of protection"
	
	label var Kume		 	"Niv50-level Kume (2003) Brazil import tariffs"
	label var erpKume		"Niv50-level Kume (2003) Brazil effective rate of protection"
	
	order year cnae10 cnae10_des
	
	saveold "${public}/Tariffs/tariffs_maindataset_long.dta", replace

}

***************************************************
***************** Log tariff changes **************
***************************************************

if `tariffs_ln1plus'==1{
	
	u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
	
	keep if inlist(year,`tariff_fy',`tariff_ly')
	
	gen double tradable = (inrange(ibgesubsector,1,13) | ibgesubsector==25)
	keep if tradable==1
	
	keep year cnae10 ibgesubsector niv50 tradable cnae10_des TRAINS mercoTRAINS ErpTRAINS RealErpTRAINS Kume erpKume
	
	foreach var of varlist  TRAINS mercoTRAINS ErpTRAINS RealErpTRAINS Kume erpKume{
		gen double ln1plus`var' = ln(1 + `var'/100)
	}

	reshape wide TRAINS mercoTRAINS ErpTRAINS RealErpTRAINS Kume erpKume ln1plus*, i(cnae10 cnae10_des) j(year)

	foreach x in TRAINS mercoTRAINS ErpTRAINS RealErpTRAINS Kume erpKume{
		gen double chng`tariff_ly'`tariff_fy'`x' =  ln1plus`x'`tariff_ly' - ln1plus`x'`tariff_fy'
	}
	
	keep if !missing(chng19941990TRAINS)
	
	**** Graph ****
	cap mkdir "${monopsonies}/eps/`outdate'/"
	
	local TRAINS "Import Tariff"
	local ErpTRAINS "ERP"
	local RealErpTRAINS "Exchange Rate Adjusted ERP"
	local Kume "Kume (2003) import tariff"
	local erpKume "Kume (2003) ERP"
	local mercoTRAINS "Mercosur partners import tariff"
	
	* Combined
	foreach x in TRAINS ErpTRAINS RealErpTRAINS Kume erpKume mercoTRAINS{
	
		* Change vs levels graph
		twoway scatter chng`tariff_ly'`tariff_fy'`x' ln1plus`x'`tariff_fy', scheme(s1color) ///
		ytitle("`tariff_ly'-`tariff_fy' change in ``x''") ///
		xtitle("ln(1 + `tariff_fy' ``x'')") mcolor(green)
		
		graph export "${monopsonies}/eps/`outdate'/`x'_`tariff_ly'`tariff_fy'change_vs_`tariff_fy'level.eps", replace
	}
	
	* Split
	foreach x in ErpTRAINS RealErpTRAINS erpKume{
		twoway (scatter chng`tariff_ly'`tariff_fy'`x' ln1plus`x'`tariff_fy' if ln1plus`x'`tariff_fy'>=0, mcolor(red)) ///
			   (scatter chng`tariff_ly'`tariff_fy'`x' ln1plus`x'`tariff_fy' if ln1plus`x'`tariff_fy'<0, mcolor(blue)), ///
		scheme(s1color) legend(label(1 "`tariff_fy' net import tariff") label(2 "`tariff_fy' net import subsidy") rows(1) order(2 1)) ///
		ytitle("`tariff_ly'-`tariff_fy' change in ``x''") ///
		xtitle("ln(1 + `tariff_fy' ``x'')") 

		graph export "${monopsonies}/eps/`outdate'/`x'_`tariff_ly'`tariff_fy'change_vs_`tariff_fy'level_split.eps", replace
		
	}
	
	keep chng* ln1plus* TRAINS* ErpTRAINS* RealErpTRAINS* mercoTRAINS* Kume* erpKume* ///
		 cnae10 ibgesubsector tradable niv50
	order  cnae10 tradable  chng* ln1plus* ibgesubsector niv50 
	
	saveold "${public}/Tariffs/cnae10_tariff_changes_`tariff_fy'_`tariff_ly'.dta", replace
	
}
