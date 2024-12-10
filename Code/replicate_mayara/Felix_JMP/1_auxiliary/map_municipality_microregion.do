/*
	Mayara Felix
	
	* Find and fix establishment's finest location codes available
	
	Before 2002: 	municipality
	Startign 2002:	zip code
	
	
	13 GOias codes remaining to assign to modern day Tocantins
	
		Miracema do Norte
		Itapora de Goias
		Conceicao do Norte
		Ponte Alta do Norte
		Axixa de Goias
		Rio Sono (Desativado)
		Sitio Novo de Goias
		Colinas de Goias
		Pindorama de Goias
		Paraiso do Norte de Goias
		Aurora do Norte
		Pinto Bandeira
		Dois Irmaos de Goias

*/

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

local link_ibge_codes 			= 1

********************************************************************
******** Link municipalities in RAIS with microregion codes ********
********************************************************************

if `link_ibge_codes'==1{

	* Get municipalities in RAIS
	* RAIS will include old municipality codes (e.g. municipalities that 
	* used to be part of Goiais and are now part of Tocantins). So assign
	* unique, fixed, municip codes for those who changed.
	import excel using "${dictionaries}/municipality_codes.xlsx", sheet(Compiled) clear
	
	* Keep only real codes
	drop if regexm(D,"Ignorado")
	keep A B C D
	keep if _n>1
	destring A B, replace
	
	ren A municipality
	ren B state
	ren C state_name
	ren D municipality_name
	keep if !missing(municipality)
	
	* Assign Tocantins municipality codes to Goiais municipality codes that
	* were changed when Tocantins was created
	tempfile all
	sa `all'
	
	preserve
		keep if state_name=="TO"
		renvars _all, postf("_fixed")
		ren municipality_name_fixed municipality_name
		tempfile to
		sa `to'
	
		u `all', clear
		keep if state_name=="GO"
		keep municipality municipality_name
		replace municipality_name = "Pindorama do Tocantins" if municipality_name=="Pindorama de Goias"
		replace municipality_name = "Miracema do Tocantins" if municipality_name=="Miracema do Norte"
		replace municipality_name = "Colinas do Tocantins" if municipality_name=="Colinas de Goias"
		replace municipality_name = "Conceicao do Tocantins" if municipality_name=="Conceicao do Norte"
		replace municipality_name = "Paraiso do Tocantins" if municipality_name=="Paraiso do Norte de Goias"
		replace municipality_name = "Itapora do Tocantins" if municipality_name=="Itapora de Goias"
		replace municipality_name = "Ponte Alta do Tocantins" if municipality_name=="Ponte Alta do Norte"
		replace municipality_name = "Aurora do Tocantins" if municipality_name=="Aurora do Norte"	
		replace municipality_name = "Dois Irmaos do Tocantins" if municipality_name=="Dois Irmaos de Goias"	
		replace municipality_name = "Rio Sono" if municipality_name=="Rio Sono (Desativado)"
		replace municipality_name = "Sitio Novo do Tocantins" if municipality_name=="Sitio Novo de Goias"
		replace municipality_name = "Axixa do Tocantins" if municipality_name=="Axixa de Goias"
		
		merge m:1 municipality_name using `to'
		keep if !missing(municipality_fixed)
		keep if _merge==3
		drop _merge
		
		tempfile fixed
		sa `fixed'
	restore
	
	merge 1:1 municipality using `fixed'
	keep if _merge==1 | _merge==3
	drop _merge
			
	foreach x in municipality state state_name{
		replace `x'_fixed = `x' if missing(`x'_fixed)
		ren `x' `x'_rais
	}
	
	renvars *_fixed, postdrop(6)
	
	order *_rais, first
	
	isid municipality_rais
	
	* Last municipality name fixing before merging with MMC
	replace municipality = 430210 ///
			if municipality_name=="Pinto Bandeira"	// in RS, used to be part of Bento G
	replace municipality_name = "Bento Goncalves" ///
			if municipality_name=="Pinto Bandeira"	// in RS, used to be part of Bento G
	
	tempfile raismun
	sa `raismun'

	* Get microregion code of municipality
	import excel using "${public}/IBGE/geocoding/divisao territorial/2016/DTB_BRASIL_MUNICIPIO.xls", sheet(DTB_2016_Municipio) clear
	
	keep A B C D E F H I
	ren A state
	ren B state_name
	ren C mesoregion
	ren D mesoretion_name
	ren E microregion
	ren F microregion_name
	ren H municipality
	ren I municipality_name
	keep if _n>1
	
	replace mesoregion = state+mesoregion
	replace microregion = state + microregion
	
	* Last digit of municipality is a Codigo Verificador. Keep only first six digits
	replace municipality = substr(municipality,1,6)
	
	destring municipality state mesoregion microregion, replace
	
	keep if !missing(municipality)
	
	order *_name, last
	
	tempfile microreg
	sa `microreg'
	
	* Get contiguous microregion codes used in Kovak 2013
	u "${public}/other/Kovak (2013)/microreg_to_mmc.dta", clear
	ren microreg microregion
	keep if !missing(mmc)
	
	tempfile mmc
	sa `mmc'
	
	* Merge all - Note that need to merge many to one with data including RAIS municipalities
	* because municipality_rais is unique but municipality is not
	
	u `raismun', clear
	merge m:1 municipality using `microreg', update
	keep if _merge!=2
	drop _merge
	
	merge m:1 microregion using `mmc'
	keep if _merge!=2
	drop _merge
	
	order municipality_rais state_rais state_name_rais ///
		  municipality state state_name microregion mmc mesoregion, first
	
	label var municipality_rais "Municipality code in RAIS"
	label var municipality "Continuous municipality code"
	
	unique municipality_rais
	label var mmc "Contiguous microregion groups per Kovak 2013"
	
	saveold "${dictionaries}/municipality_to_microregion.dta", replace
}
