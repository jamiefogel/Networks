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

local regressions	= 1
local premiadate	= 20210802

local baseyear		= 1991

global minyear = 1986
global maxyear = 2000
local minyear = ${minyear}
local maxyear = ${maxyear}


if `regressions'==1{
	
	u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
	ren cnae10 cnae95
	keep cnae95 ibgesubsector 
	duplicates drop
	tempfile tariffs
	sa `tariffs'
	
	u "${monopsonies}/dta/fakeid_importers_exporters_allyears_20191213.dta", clear
	keep if inrange(year,1990,1994)
	bys fakeid_firm: gegen explib = max(exporter)
	bys fakeid_firm: gegen implib = max(importer)
	
	/* Importer data only available for 1990*/
	bys fakeid_firm: gegen bexp = max(cond(inlist(year,1990,1991),exporter,.))
	bys fakeid_firm: gegen bimp = max(cond(year==1990,importer,.))	
	
	keep fakeid_firm explib bexp
	gduplicates drop
	tempfile exporters
	sa `exporters'
	
	* Herfindahl with wage premium
	u "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", clear
	keep fakeid_firm mmc cbo942d cnae95 year emp
	
	merge m:1 fakeid_firm using `exporters', keep(1 3) nogen
	replace explib=0 if missing(explib)
	replace bexp = 0 if missing(bexp)
	
	merge m:1 cnae95 using `tariffs', keep(1 3) nogen
	gegen ibge = max(ibgesubsector), by(fakeid_firm)
	gen T = (ibge<14 | ibge==25)

	merge 1:1 fakeid_firm mmc cbo942d year using "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_`premiadate'.dta", ///
	keepusing(dprem_zro) keep(3) nogen
	
	tempfile all
	sa `all'
	
	foreach group in none cbo942d{
		
		u `all', clear
		cap gen none = 1
		gen double w = exp(dprem_zro)
		gen double totprem = emp*w
		`tool'egen double mktprem = sum(totprem), by(mmc `group' year)
		gen double pshare = totprem/mktprem
		
		gen double pshare2 = pshare^2
		`tool'egen double hf_pdbill = sum(pshare2), by(mmc `group' year)
		
		bys mmc `group' year: gegen double hf_t_pdbill = sum(cond(T==1,pshare2,.))
		bys mmc `group' year: gegen double hf_nt_pdbill = sum(cond(T==0,pshare2,.))
		bys mmc `group' year: gegen double hf_explib_pdbill = sum(cond(explib==1,pshare2,.))
		bys mmc `group' year: gegen double hf_Tnexplib_pdbill = sum(cond(explib==0 & T==1,pshare2,.))
		
		bys mmc `group' year: gegen double hf_bexp_pdbill = sum(cond(bexp==1,pshare2,.))
		bys mmc `group' year: gegen double hf_Tnbexp_pdbill = sum(cond(bexp==0 & T==1,pshare2,.))
		
		
		* Herfindahl for Tradables only
		`tool'egen double Tmktprem = sum(cond(T==1,totprem,.)), by(mmc `group' year)
		gen double pTshare = totprem/Tmktprem if T==1
		gen double pTshare2 = pTshare^2
		`tool'egen double hf_pdbill_T = sum(pTshare2), by(mmc `group' year)
		
		* Herfindahl for Non-tradables only
		`tool'egen double NTmktprem = sum(cond(T==0,totprem,.)), by(mmc `group' year)
		gen double pNTshare = totprem/NTmktprem if T==0
		gen double pNTshare2 = pNTshare^2
		`tool'egen double hf_pdbill_NT = sum(pNTshare2), by(mmc `group' year)
		
		keep mmc `group' year hf_*
		`tool'duplicates drop
		
		saveold "${monopsonies}/sas/earnings_premia_Herfindhal_mmc_`group'.dta", replace
	}
}
