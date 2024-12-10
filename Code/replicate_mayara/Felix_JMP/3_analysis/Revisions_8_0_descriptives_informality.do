/*
	Compute basic baseline characteristics of MMCs using
	census dataset from Dix Carneiro and Kovak (2017)
	Note: is this the whole census? Why only 5.6 million obs?

	Not computing by economic activity sector and occupation because

	- this census dataset also inclues activity code variable atividade
	but don't have a straightforward way to map it to RAIS' IBGESUBSECTOR yet.
	Perhaps Datazoom has that variable already.
		-- Actually Appendix Table A1 in the paper provides
		a map between Atividade in census and Nível 50
		-- Can use this map to link to IBGESUBSECTOR using Muendler Maps

	- this census dataset does not include occupational code.
	Perhaps Datazoom has that variable.

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


local mmc_descriptives 	= 1

if `mmc_descriptives'==1{

	import excel using "${dictionaries}/crosswalk_atividade_ibgesubsector.xlsx", sheet("atividade_ibgesubsector") firstrow clear

	tempfile mapping
	sa `mapping'
	
	/* 1991 */

	u "${public}/other/DK (2017)/ReplicationFiles/Data_Census/cen91.dta", clear
	merge m:1 atividade using  `mapping', keep(1 3) nogen	/* Unemployed won't match to activity so keep if doesn't match */

	* For mmc general characteristics, do not make any restrictions except age and public admin
	keep if inrange(age,18,64) & employed==1 | nilf==1 | unemp==1

	drop if ibgesubsector_grouped==24
	label variable ibgesubsector_grouped "Same as RAIS IBGESUBSECTOR but codes 21, 22, 23 are grouped under 777"

	// Merge in mmc codes
	merge m:1 munic using "${public}/other/DK (2017)/ReplicationFiles/Data_Census/Auxiliary_files/census_1991_munic_to_mmc_1991_2010.dta", keep(3) nogen
	
	****** Population characteristics for heterogeneity analysis ******
	****** (See DicionarioCenso1991.[df on dropbox, p 10 on how to use sample weights)
	
	/*
	var urbanrural

	 Código  Descrição
        1   Área Urbanizada
        2   Área Não Urbanizada
        3   Área Urbana Isolada
        4   Aglomerado Rural de Extensão Urbana
        5   Aglomerado Rural Isolado ou Povoado
        6   Aglomerado Rural Isolado ou Núcleo
        7   Outros Aglomerados
        8   Área Rural (Exclusive Aglomerado Rural)

	*/

	/*
		 Variável no: 0309 - Raça ou Cor
		    Código  Descrição
		1 Branca
		2 Preta
		3 Amarela
		4 Parda
		5 Indígena
		9 Ignorado
	*/

	gen urban = inlist(urbanrural,1,3,4)
	gen nonwhite = (race!=1)

	* Multiply by sampling weight for either sums or means
	foreach var of varlist 	employed unemp nilf ///
				  			formemp nonformemp selfemployed ///
				  			female urban educ age nonwhite{
				qui replace `var' = `var'*xweighti
	}

	collapse (sum) xweighti ///
				   employed unemp nilf ///
				   formemp nonformemp selfemployed ///
				   female urban educ age nonwhite, by(mmc ibgesubsector_grouped)

	foreach var of varlist educ age{
		gen double avg_`var' = `var'/xweighti
		drop `var'
	}

	drop xweighti
	gen double pop18_64 = employed + unemp + nilf

	order mmc ibgesubsector_grouped pop18_64
	
	compress
	saveold "${monopsonies}/sas/mmc_ibgesub_descriptives_1991.dta", replace
}

