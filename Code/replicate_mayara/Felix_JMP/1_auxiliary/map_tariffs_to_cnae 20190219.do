/*
	Mayara Felix
	
	GOAL: 				Assign Annual tariff levels at CNAE level
	Wish-list data:		Annual tariffs from legislation at NBM level for 1980-1988
						(Using WTIS)
						Map between NBM 1979 (8digit) and NBM 1988 (10digit)
	
	******** 1979 legistlation tariff to CNAE1995 (CNAE 1.0) *********
	
	* Cannot map the 1979 Tariff list to 1996 NCM at finest level. 1979 
	* used old NBM at 8-digits level. That nomenclature was changed in 1988
	* to be consistent with International standards, such that
	* a new NBM with 10 digits was used between 1988 and 1994, until
	* NCM was introduced.
	
	So I need either of 2 things to map the 1979 tariffs to CNAE10 (the CNAE in 1995):
		a) A mapping between NBM 1979 and NBM 1988
		b) A list of tariffs at NBM level in 1988
		
	In this code I attempt to conduct a coarse match
	from first 4-digits of NBM in 1979 to first 4-digits NBM in 1988
	
	1980 Tariff		TAB list	NBM codes
	1996 onwards	TEC list	NCM codes
	
	NBM 1980 -> NCM 1996 -> CNAE 1995 (1.0)
	
	Mapping						Source
	NBM 1980 -> NBM 1988		[Don't know]
	NBM 1988 -> NCM 1996		SAEX - correlacao NBM NCM 1996
	NCM 1996 -> CNAE 1995		IBGE
	
	********* Annual tariffs frin WITS fir 1988 onwards *********
	
	Mapping							Source	
	HS			-> ISIC Rev 3 		WITS
	ISIC Rev 3	-> CNAE 95			Muendler (2003)

*/

clear all
set more off
unicode encoding set "latin1"
set seed 34317154


* Mayara mounting on server
if c(username)=="mfelix"{
	global dictionaries		"/proj/patkin/raisdictionaries/harmonized"
	global public			"/proj/patkin/publicdata"
}
* Mayara mounting locally on Mac
else if c(username)=="mayara"{
	global dictionaries		"/Volumes/proj_atkin/raisdictionaries/harmonized"
	global public			"/Volumes/proj_atkin/publicdata"
}
* Mayara mounting locally on Windows
else if c(username)=="Mayara"{
	global dictionaries		"M:/raisdictionaries/harmonized"
	global public			"M:/publicdata"
}


*global public "/Users/mayara/Desktop/coding"

local tariff79toCNAE10			= 0		/* Legislation data */

local wits_conc					= 0		/* WITS concordances*/
local wits_tariffs_mercosur		= 0		/* WITS data Mercosur */

local tariff8916toCNAE10		= 0		/* Map to CNAE10 */
local anuall_tariffs_cnae10		= 0		/* Annual CNAE10-level tariff means dataset; keep tradable ibgesubsectors only */
local annual_tariffs_plus		= 1		/* 	Add Tariffs for intermediate inputs from Muendler (2003) 
											and tariffs + effective rate of protection from Kume (2003)*/

********************************************************************************
******************** Map 1979 legislation tariffs to CNAE10 ********************
********************************************************************************

if `tariff79toCNAE10'==1{

******** Import files **********

* 1) Import Brazilian law files

* 8-digit NBM level tariffs
import excel "${public}/Tariffs/Brazil Law/1979 TAB (Del 1753)_MFedits.xlsx", first clear

destring Tariffcode, gen(tariff79) force	
replace tariff79  = tariff_fix_MF if !missing(tariff_fix_MF)
replace ProductCode = product_fix_mf if product_fix_mf!=""

// Some have "IU" code, need to find out what that is
keep if !missing(tariff79)

ren ProductCode NBM79
keep NBM79 tariff79
duplicates drop

* Check additional duplicates then clean
local exp = 0
if `exp'==1{
preserve
	duplicates tag NBM79, gen(dup)
	sort NBM79
	keep if dup>0
	outsheet using "${public}/Tariffs/Brazil Law/duplicates_odesk.csv", comma replace
restore
}

* Outsheet clean file forever
local out = 0
if `out'==1{
preserve
	outsheet using "${public}/Tariffs/Brazil Law/1979 TAB (Del 1753)_clean_final.csv", comma replace
restore
}

isid NBM79
tempfile t79
sa `t79'

* Collapse at 4-digit level to match with NBM at 4-digit level in 1988
* Goal is to push as much of the 79tariff data to make it to CNAE95
gen NBM4 = substr(NBM79,1,4)
collapse (mean) tariff79_mean=tariff79 ///
		(count) tariff79_items=tariff79 ///
		(sd) 	tariff79_var=tariff79, by(NBM4)

unique NBM4		/* 1,001 4-digit level codes */

isid NBM4
tempfile t79_4
sa `t79_4'

* 2) Import NBM-NCM 1996 correspondance

* 10digit NBM level codes with NCM correspondents
import excel "${public}/Tariffs/Concordances/correlacao NBM NCM 1996.xls", first clear

ren CO_NBM NBM88
ren CORRE NCM96
keep NBM88 NCM96
label var NCM96 ""

* First 4 digits of NBM
gen NBM4 = substr(NBM88,1,4)

unique NBM4	/* 1246 codes */

isid NBM88
tempfile nbmtoncm
sa `nbmtoncm'

* 3) NCM to CNAE95 correspondances

import excel "${public}/Tariffs/Concordances/NCMtoCNAE10/NCM96XCNAE.xls", clear
keep if _n>=13
keep A B
ren A NCM96
ren B CNAE10
// Needs NCM96 to match to 79 tariffs & CNAE10 to match to RAIS data
keep if CNAE10!="" & NCM96!=""	

unique CNAE10	/* 280 CNAE95 codes */
unique NCM96	/* 8723 NCM codes */

// Small number of NCM fall under different CNAE
bys NCM96: gen n=_n
reshape wide CNAE10, i(NCM96) j(n)

ren CNAE101 CNAE10_1
ren CNAE102 CNAE10_2

isid NCM96
tempfile ncm96
sa `ncm96'

import excel "${public}/Tariffs/Concordances/NCMtoCNAE10/NCM2002XCNAE.xls", clear
keep if _n>=6
keep A B
ren A NCM02
ren B CNAE10

duplicates drop

// Some NCM fall under different CNAE
bys NCM02: gen n=_n
levelsof n, local(ns)
reshape wide CNAE10, i(NCM02) j(n)

foreach n in `ns'{
	ren CNAE10`n' CNAE10_`n'
}

isid NCM02
tempfile ncm02
sa `ncm02'

********************************************
*************** Merge files ****************
********************************************

* Merge 1979 tariffs at NBM level to NCM codes
* Merging this at 4 digit level
u `nbmtoncm', clear

merge m:1 NBM4 using `t79_4'
unique NBM4 if _merge==2	// 96 tariffs79 not matched
							// Later: assign those to matched codes at 2-levels

* 913 matched product averages
gen NBM7988_4digit_matched = (_merge==3)
drop _merge

saveold "${public}/Tariffs/legislation_tariff79toNCM96.dta", replace

* Merge tariff79 to CNAE10 using NCM96 
keep if NCM96!="" & !missing(tariff79_mean)
keep NCM96 tariff*
duplicates drop

* NCM96 not unique (recall that matched at NBM level)
* Take wated average of tariff by NCM96, weighted
* by the number of tariff lines in the 1979 data
* Keep track of tariff lines for future weighting

preserve
	collapse (mean) tariff79_mean [fw=tariff79_items], by(NCM96)
	tempfile mean
	sa `mean'
restore

collapse (sum) tariff79_items, by(NCM96)

unique NCM96	// 5,400 NCMs96

merge 1:1 NCM96 using `mean', nogen

* Now merge with NCM96-CNAE10 map
merge 1:1 NCM96 using `ncm96'

* Assign those equally across fine level NCMs with good matches

// 429 Unmatched NCMs with tariff79 data at finest level
// Match at 4-digit
preserve
	keep if _merge==1
	gen NCM96_4 = substr(NCM96,1,4)
	
	tempfile all
	sa `all'
	
	collapse (mean) tariff79_mean [fw=tariff79_items], by(NCM96_4)
	tempfile m
	sa `m'
	
	u `all'
	collapse (sum) tariff79_items, by(NCM96_4)
	merge 1:1 NCM96_4 using `m', nogen
	
	duplicates drop
	
	tempfile notmatched
	sa `notmatched'
restore

// Keep if Any match to NCM96
keep if _merge==3
drop _merge
gen NCM96_4 = substr(NCM96,1,4)

merge m:1 NCM96_4 using `notmatched'
gen matched_NCM96_4or2 = (_merge==3)

// 93 Unmatched NCMs with tariff79 data at 4-digit level
// Match at 2-digit
preserve
	keep if _merge==1
	gen NCM96_2 = substr(NCM96,1,2)
	
	tempfile all
	sa `all'
	
	collapse (mean) tariff79_mean [fw=tariff79_items], by(NCM96_2)
	tempfile m
	sa `m'
	
	u `all'
	collapse (sum) tariff79_items, by(NCM96_2)
	merge 1:1 NCM96_2 using `m', nogen
	
	duplicates drop
	
	tempfile notmatched4
	sa `notmatched4'
restore

keep if _merge==3 | _merge==1
drop _merge
gen NCM96_2 = substr(NCM96,1,2)

merge m:1 NCM96_2 using `notmatched4'

* All matched now
assert _merge!=2
drop _merge

keep NCM96 CNAE* tariff* matched

* Need to collapse this at CNAE10 level
replace CNAE10_1 = subinstr(subinstr(CNAE10_1,"-","",.),".","",.)
replace CNAE10_2 = subinstr(subinstr(CNAE10_2,"-","",.),".","",.)
destring NCM CNAE10*, replace

drop matched
reshape long CNAE10_, i(NCM tariff*) j(n)
drop if missing(CNAE10_)
ren CNAE10_ CNAE10
drop n
duplicates drop

collapse (mean) tariff79_mean (count) NCM96, by(CNAE10)

ren NCM96 NCM96_groups
ren CNAE10 cnae10
order cnae10
saveold "${public}/Tariffs/legislation_tariff79toCNAE10.dta", replace
}

********************************************************************************
************************ Map 1989-2016 tariffs to CNAE10 ***********************
********************************************************************************
*	HS-> ISIC Rev 3 -> ISIC Rev 4 -> CNAE 2.0 -> CNAE 1.0

if `wits_conc'==1{


	* HS0 HS88: 1989 - 1995
	insheet using "${public}/Tariffs/Concordances/HStoISIC3/DataJobID-1457009_1457009_ConcHS88ISICRev3.csv", clear
	keep productcode isicrevision3productcode isicrevision3productdesc
	ren productcode hs0
	ren isicrevision3productcode isic3
	ren isicrevision3productdesc isic3_desc
	
	isid hs0
	saveold  "${public}/Tariffs/Concordances/HStoISIC3/HS0toISIC3.dta", replace
	
	* HS1 HS96: 1986 - 2001
	insheet using "${public}/Tariffs/Concordances/HStoISIC3/DataJobID-1457011_1457011_ConcHS96ISICRev3.csv", clear
	keep productcode isicrevision3productcode isicrevision3productdesc
	ren productcode hs1
	ren isicrevision3productcode isic3
	ren isicrevision3productdesc isic3_desc
	
	isid hs1
	saveold  "${public}/Tariffs/Concordances/HStoISIC3/HS1toISIC3.dta", replace
	
	* HS2 HS02: 2002 - 2008
	insheet using "${public}/Tariffs/Concordances/HStoISIC3/DataJobID-1457012_1457012_ConcHS02ISICRev3.csv", clear
	keep productcode isicrevision3productcode isicrevision3productdesc
	ren productcode hs2
	ren isicrevision3productcode isic3
	ren isicrevision3productdesc isic3_desc
	
	isid hs2
	saveold  "${public}/Tariffs/Concordances/HStoISIC3/HS2toISIC3.dta", replace
	
	* HS3 HS07: 2007 - 2012
	insheet using "${public}/Tariffs/Concordances/HStoISIC3/DataJobID-1457013_1457013_ConcHS07ISICRev3.csv", clear
	keep productcode isicrevision3productcode isicrevision3productdesc
	ren productcode hs3
	ren isicrevision3productcode isic3
	ren isicrevision3productdesc isic3_desc
	
	isid hs3
	saveold  "${public}/Tariffs/Concordances/HStoISIC3/HS3toISIC3.dta", replace
	
	* HS4 HS12: 2013 - 2016
	insheet using "${public}/Tariffs/Concordances/HStoISIC3/DataJobID-1457014_1457014_ConcHS12ISICRev3.csv", clear
	keep productcode isicrevision3productcode isicrevision3productdesc
	ren productcode hs4
	ren isicrevision3productcode isic3
	ren isicrevision3productdesc isic3_desc
	
	isid hs4
	saveold  "${public}/Tariffs/Concordances/HStoISIC3/HS4toISIC3.dta", replace
	
	* ISIC3 to ISIC4
	insheet using "${public}/Tariffs/Concordances/ISIC3toISIC4/ISIC REV. 3.1 - ISIC REV. 4_20181016_012949.csv", clear
	keep v1 v2
	keep if _n>=3
	ren v1 isic3
	ren v2 isic4
	destring isic*, replace
	
	unique isic3		/* 298 unique, 3 or 4 digits */
	unique isic4		/* 419 unique, 3 or 4 digits */
	
	* Make unique at isic3 level
	bys isic3: gen n = _n
	reshape wide isic4, i(isic3) j(n)
	
	saveold  "${public}/Tariffs/Concordances/ISIC3toISIC4/ISIC3toISIC4.dta", replace
	
	* ISIC4 to CNAE20
	import excel using "${public}/Tariffs/Concordances/ISIC4toCNAE20/CNAE20_Correspondencia_Isic4xCnae20.xls", clear
	keep B D
	keep if _n>=55
	ren B isic4
	ren D cnae20
	replace cnae20=subinstr(cnae20,".","",.)
	replace cnae20=subinstr(cnae20,"-","",.)
	drop if regexm(isic4,"[A-Z]")>0
	drop if regexm(cnae20,"[A-Z]")>0
	
	destring isic4 cnae20, replace
	
	unique isic4	/* 710, 1 to 4 digits */
	unique cnae20	/* 1026, 1 to 4 digits  */
	
	* Make unique at ISIC4 level
	bys isic4: gen n = _n
	reshape wide cnae20, i(isic4) j(n)
	
	saveold  "${public}/Tariffs/Concordances/ISIC4toCNAE20/ISIC4toCNAE20.dta", replace
	
	*CNAE20 to CNAE10 (or CNAE10)
	
	import excel using "${public}/Tariffs/Concordances/CNAE20toCNAE10/CNAE20_Correspondencias.xls", ///
	sheet("CNAE 2.0 x CNAE 1.0") clear
	keep B D
	keep if _n>=12
	ren B cnae20
	ren D cnae10
	replace cnae20=subinstr(cnae20,".","",.)
	replace cnae20=subinstr(cnae20,"-","",.)
	replace cnae10=subinstr(cnae10,".","",.)
	replace cnae10=subinstr(cnae10,"-","",.)
	drop if regexm(cnae20,"[A-Z]")>0 | cnae20=="" | cnae10==""
	
	destring cnae*, replace
	
	unique cnae20	/* 672 4 to 5 digit*/
	unique cnae10	/* 580 4 to 5 digit*/
	
	* Make unique at cnae10 level
	bys cnae20: gen n = _n
	reshape wide cnae10, i(cnae20) j(n)
	
	saveold  "${public}/Tariffs/Concordances/CNAE20toCNAE10/CNAE20toCNAE10.dta", replace
	
} /* Close import concordances */
	
if `wits_tariffs_mercosur'==1{
	
	local BRAfiles: dir "${public}/Tariffs/WITS Database" files "*BRA*.CSV"
	local PRYfiles: dir "${public}/Tariffs/WITS Database" files "*PRY*.CSV"
	local URYfiles: dir "${public}/Tariffs/WITS Database" files "*URY*.CSV"
	local ARGfiles: dir "${public}/Tariffs/WITS Database" files "*ARG*.CSV"
	
	foreach country in BRA PRY URY ARG{
		local i = 1
		foreach f in ``country'files'{
		
			di in red "`f'"
			insheet using "${public}/Tariffs/WITS Database/`f'", clear
			replace nomencode = subinstr(nomencode,"H","HS",.)
			levelsof nomencode, local(code_upper) clean
			replace nomencode = lower(nomencode)
			levelsof nomencode, local(code_lower) clean
			
			levelsof year, local(year) clean
			
			ren productcode			`code_lower'
			ren simpleaverage 		tariff_mean
			ren totalnoofvalidlines	tariff_items
			
			keep `code_lower' tariff* year
			
			* Bring in ISIC3 corresponding code
			merge 1:m `code_lower' using "${public}/Tariffs/Concordances/HStoISIC3/`code_upper'toISIC3.dta"
			
			keep if _merge==3 
			drop _merge
			
			* Collapse at isic3 level, weighting by # of line items
			preserve
				collapse (mean) tariff_mean [fw=tariff_items], by(year isic3)
				tempfile means
				sa `means'
			restore
			
			collapse (sum) tariff_items, by(year isic3)
			merge 1:1 isic3 using `means'
			keep if _merge==3
			drop _merge
			
			tempfile f`country'`i'
			sa `f`country'`i''
			local i = `i'+1
		}
		
		local j = `i'-1
		
		* Append all years - ISIC3 level means
		di in red "f`country'1"
		u `f`country'1', clear
		forvalues k=2/`j'{
			di in red "f`country'`k'"
			append using `f`country'`k''
		}
		
		saveold "${public}/Tariffs/tariff8916toISIC3_`country'.dta", replace
	}
	
} /* Close import WITS */

if `tariff8916toCNAE10'==1{
	
		u "${public}/other/Muendler (2003)/cnae-x-isic.dta", clear
		keep cnae isic30
		ren isic30 isic3
		ren cnae cnae10
		duplicates drop
		
		* 564 CNAEs
		* 292 isic3
		bys isic3: gen isic_sectors = _n
		
		reshape wide cnae10, i(isic3)  j(isic_sectors)
		
		tempfile isic3tocnae10
		sa `isic3tocnae10'
		
	foreach country in BRA PRY URY ARG{
	
		u "${public}/Tariffs/tariff8916toISIC3_`country'.dta", clear
		merge m:1 isic3 using `isic3tocnae10'
		
		keep if _merge==3
		drop _merge
		
		reshape long cnae10, i(year isic3 tariff_items tariff_mean) j(isic_sectors)
		keep if !missing(cnae10)
		
		* Take simple mean in case of sectors with several ISIC3
		collapse (mean) tariff_mean [fw=tariff_items], by(cnae10 year)
		
		saveold "${public}/Tariffs/WITS_tariffsCNAE10_`country'.dta", replace
		
	}
}

********************************************************************************
************************** Compile all annual tariffs **************************
********************************************************************************

if `anuall_tariffs_cnae10'==1{

	foreach country in BRA PRY URY ARG{
		u "${public}/Tariffs/WITS_tariffsCNAE10_`country'.dta", clear
	
		reshape wide tariff_mean, i(cnae) j(year)
		
		* Merge IBGE subsector
		merge 1:1 cnae10 using "${dictionaries}/rais_cnae10_to_ibgesubsector.dta"
		keep if _merge!=2
		drop _merge
		
		* Merge IBGE descriptions
		preserve
		/*  Muendler CNAE-ISIC correspondence already has english descriptions
			import excel "${dictionaries}/cnae10_descriptions_PT.xls", firstrow clear
			ren c cnae10
			ren description cnae10_trans
			keep cnae10*
			
			tempfile lab_c
			sa `lab_c'
		*/
		
			import excel "${dictionaries}/ibgesubsector_descriptions_PT.xls", firstrow clear
			drop ibgesubsector_pt
			tempfile lab_i
			sa `lab_i'
			
		restore
		
		/*
		merge 1:1 cnae10 using `lab_c'
		keep if _merge!=2
		drop _merge
		*/
		
		merge m:1 ibgesubsector using `lab_i'
		keep if _merge!=2
		drop _merge
		
		order cnae10* ibge*
		
		* Make sure to only keep tradable sectors
		keep if inrange(ibgesubsector,1,13) |  ibgesubsector==25
		
		saveold "${public}/Tariffs/annual_tariffs_CNAE10_`country'.dta", replace
	}
	
}

**** Add other tariffs from literature ****

/*
	Muendler (2003) tariffs for intermediate goods are based Kume (2003) tariffs at Nivel 80.
	Muendler provides these tariffs at Nivel 100 codes. 
	These tariffs were merged at Nivel 100 with CNAE.
	
	Kume (2003) tariffs at Niv50 are taken from Kovak (2013). These include effective
	protection rates, hence why we take it.

*/

if `annual_tariffs_plus'==1{
	
	* Muendler (2003) converter from CNAE (first 4 digits only) to niv100
	
	* Mining and manufacturing - 4-digits CNAE match *
	u "${public}/other/Muendler (2003)/cnae2niv100.dta", clear
	
	gen double cnae4D = int(cnae)	/* Integer part for matching to tariffs data */
	keep niv100 cnae4D
	duplicates drop
	isid cnae4D
	
	tempfile 4D
	sa `4D'
	
	* Agriculture is matched at 3-digits match
	u "${public}/other/Muendler (2003)/cnae2niv100-app-agric.dta", clear
	
	gen double cnae3D = int(cnae)	/* Integer part for matching to tariffs data */
	keep niv100 cnae3D
	replace  niv100 =100 if cnae3D==111	
	duplicates drop
	isid cnae3D
	
	tempfile 3D
	sa `3D'
	
	* Tariffs for invermediates at niv100 level - go wide
	u "${public}/other/Muendler (2003)/tariffs-intm.dta", clear
	
	keep niv100 year tariff
	replace tariff = tariff*100
	ren tariff tariff_intm_Muendler
	replace year = year+1900
	
	reshape wide tariff_intm, i(niv100) j(year)
	
	tempfile intm
	sa `intm'
	
	* Effective tariffs for Brazil
	u "${public}/Tariffs/annual_tariffs_CNAE10_BRA.dta", clear
	
	tostring cnae10, gen(cnaestr)
	gen cnae4D = substr(cnaestr,1,4)
	gen cnae3D = substr(cnaestr,1,3)
	destring cnae4D cnae3D, replace
	drop cnaestr
	
	merge m:1 cnae4D using `4D'
	keep if _merge!=2
	drop _merge
	
	ren niv100 niv100_4D
	merge m:1 cnae3D using `3D', update
	keep if _merge!=2
	ren niv100 niv100_3D

	gen double niv100 = niv100_4D
	replace niv100 = niv100_3D if missing(niv100) | (!missing(niv100) & ibgesubsector==25)
	drop _merge
	
	drop *4D *3D 
	
	order niv100, after(cnae10)
	
	tab cnae10 if niv100==.	// Motion picture is Niv 100 1530, replace
	tab cnae10 if niv100==., nolabel
	replace niv100 = 1530 if cnae10==92118
	
	* Now bring in intermediate tariffs
	merge m:1 niv100 using `intm'
	keep if _merge!=2
	drop _merge
	
	* Merge with Kume (2003) tariffs to see how they line up
	* Kume: Niv 50
	
	assert !missing(niv100)
	
	* Niv 50
	tostring niv100, gen(niv50)
	replace niv50 = "0"+niv50 if length(niv50)==3
	
	replace niv50 = substr(niv50,1,2)
	destring niv50, replace
	
	merge m:1 niv50 using "${public}/other/Kovak (2013)/kume_etal_tariff.dta"
	keep if _merge!=2
	drop _merge
	
	renvars tariff1987-tariff1998, subst("tariff" "tariff_Kume")
	renvars erp1987-erp1998, subst("erp" "erp_Kume")
	
	unique niv100
	unique niv50
	
	renvars tariff_mean*, subst("mean" "TRAINS")
	
	* Agriculture has no intermediate goods
	label data "TRAINS tariffs by CNAE + Kume (2003) tariffs by Niv50 + Muendler (2003) intermediates by Niv 100"
	saveold "${public}/Tariffs/Tariffs_TRAINScnae10_KUMEniv50_MUENDLERniv100.dta", replace
	
}
