/*
	Mayara Felix
	
	GOAL: 				MAP HS-level Annual tariffs and trade flows to CNAE level

	********* Annual tariffs frin WITS for 1988 onwards *********
	
	Mapping							Source	
	HS			-> NCM 				First 6 digits of NCM corresponds to HS
	NCM			-> CNAE 95			IBGE
	
	Could map the 1979 tariffs as well using NCM-NBM table

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

local ncm_to_cnae				= 0		/* Import NCM to CNAE correspondance from IBGE */
local WITStoCNAEviaNCM			= 0
local annual_tariffs_plus		= 1

********************************
********** NCM to CNAE *********
********************************

if `ncm_to_cnae'==1{

	***** Consistent CNAE mapping *****
	import excel using "${dictionaries}/consistent_cnae10.xls"
	keep if _n>=3
	keep A F
	keep if !missing(F)
	
	ren A cnae10
	ren F cnae10_consistent
	
	destring cnae*, replace
	tempfile consistent
	sa `consistent'
	
	***************** NCM 2004 *****************
	import excel using "${public}/Tariffs/Concordances/NCMtoCNAE10/NCM2004XCNAE10.xls", clear
	
	ren A ncm
	
	keep if _n>=10
	
	gen hs = substr(ncm,1,6)
	destring ncm hs, replace force
	keep if !missing(ncm)

	ren C cnae10

	* Clean RAIS CNAE10 has no periods or dashes or leading zeros
	replace cnae10 = subinstr(subinstr(subinstr(cnae10,".","",.),"-","",.)," ","",.)
	*br if regexm(cnae10,"[0-9]")==0 
	destring cnae10, replace force
	
	keep if !missing(cnae10)
	
	merge m:1 cnae10 using `consistent'
	replace cnae10 = cnae10_consistent if !missing(cnae10_consistent)
	keep if _merge!=2
	
	keep hs cnae10
	duplicates drop
	
	* If CNAE is non-tradable, drop NCM-CNAE association
	/* Bring IBGEsubsector */
	
	* Merge in ibgesubsector
	merge m:1 cnae10 using "${dictionaries}/rais_cnae10_to_ibgesubsector.dta"
	keep if _merge==3
	drop _merge
				
	drop if inrange(ibgesubsector,14,24)
	
	sort cnae10 hs
	
	label var hs "Harmonized system 6 digits, first 6 digits of NCM 2004"
	label var cnae10 "CNAE 1995"
	
	saveold  "${public}/Tariffs/Concordances/NCMtoCNAE10/HStoCNAE10_viaNCM2004.dta", replace
	
	***************** NCM 2007 *****************
	
	import excel using "${public}/Tariffs/Concordances/NCMtoCNAE10/NCM2007XCNAE10XCNAE20ABRIL2010.xls", clear
	
	keep if _n>=3
	
	ren A ncm
	ren C cnae10
	
	keep ncm cnae10
	gen hs = substr(ncm,1,6)
	destring ncm hs, replace force
	keep if !missing(ncm)

	* Clean RAIS CNAE10 has no periods or dashes or leading zeros
	replace cnae10 = subinstr(subinstr(subinstr(cnae10,".","",.),"-","",.)," ","",.)
	
	split cnae10, parse(";")
	
	drop cnae10
	
	destring cnae10*, replace force
	
	*duplicates tag ncm, gen(dup)
	*br if dup
	duplicates drop
	
	reshape long cnae10, i(ncm hs) j(cnae10_num)
	
	keep if !missing(cnae10)
	
	merge m:1 cnae10 using `consistent'
	replace cnae10 = cnae10_consistent if !missing(cnae10_consistent)
	keep if _merge!=2
	
	keep hs cnae10
	duplicates drop
	
	* Merge in ibgesubsector
	merge m:1 cnae10 using "${dictionaries}/rais_cnae10_to_ibgesubsector.dta"
	keep if _merge==3
	drop _merge
				
	drop if inrange(ibgesubsector,14,24)
	
	sort cnae10 hs
	
	label var hs "Harmonized system 6 digits, first 6 digits of NCM 2007"
	label var cnae10 "CNAE 1995"
	
	saveold  "${public}/Tariffs/Concordances/NCMtoCNAE10/HStoCNAE10_viaNCM2007.dta", replace
}


if `WITStoCNAEviaNCM'==1{

	* Append product codes that are unique to 2004 or 2007
	u "${public}/Tariffs/Concordances/NCMtoCNAE10/HStoCNAE10_viaNCM2004.dta", clear
	gen year = 2004
	append using "${public}/Tariffs/Concordances/NCMtoCNAE10/HStoCNAE10_viaNCM2007.dta"
	
	replace year = 2007 if missing(year)
	
	bys hs: egen minyear = min(year)
	bys hs: egen maxyear = max(year)
	
	* Keep 2004 mapping if appears in both mappings
	drop if year==2007 & minyear==2004 & maxyear==2007
	
	* Unique NCMs in each mapping
	unique hs if minyear==2004 & maxyear==2004
	unique hs if minyear==2007 & maxyear==2007
	
	keep hs cnae10 ibgesubsector
	duplicates drop
	
	tempfile ncm_to_cnae
	sa `ncm_to_cnae'
		
	foreach country in BRA PRY URY ARG{
	
	local BRAfiles: dir "${public}/Tariffs/WITS Database" files "*BRA*.CSV"
	local PRYfiles: dir "${public}/Tariffs/WITS Database" files "*PRY*.CSV"
	local URYfiles: dir "${public}/Tariffs/WITS Database" files "*URY*.CSV"
	local ARGfiles: dir "${public}/Tariffs/WITS Database" files "*ARG*.CSV"
	
	foreach country in BRA PRY URY ARG{
		local i = 1
		foreach f in ``country'files'{
		
			di in red "`f'"
			insheet using "${public}/Tariffs/WITS Database/`f'", clear
			
			ren productcode			hs
			ren simpleaverage 		tariff_mean
			ren totalnoofvalidlines	tariff_items
			
			keep hs tariff* year
			
			assert hs<=999999
			
			merge 1:m hs using `ncm_to_cnae'
			
			* Save unmatched somewhere else for checks later
			preserve
				keep if _merge!=3
				gen inWITS 		= (_merge==1)
				gen inNCM  		= (_merge==2)
				
				drop _merge
				
				tempfile un`country'`i'
				sa `un`country'`i''
			restore
			
			keep if _merge==3
			drop _merge
			
			collapse (mean) tariff_mean [w=tariff_items], by(cnae ibgesubsector year)
			
			tempfile matched`country'`i'
			sa `matched`country'`i''
			
			local i=`i'+1
		}
		
		local j = `i'-1
		
		* Append files for each country
		u `matched`country'1', clear
		forvalues x =2/`j'{
			append using `matched`country'`x''
		}
		
		saveold "${public}/Tariffs/WITS_tariffs_to_CNAE10_via_NCM_`country'_matched.dta", replace
		
		* Append files for each country
		u `un`country'1', clear
		forvalues x =2/`j'{
			append using `un`country'`x''
		}
		
		saveold "${public}/Tariffs/WITS_tariffs_to_CNAE10_via_NCM_`country'_unmatched_NCM.dta", replace
		
	}
	} /* Close country */
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

	u "${public}/Tariffs/WITS_tariffs_to_CNAE10_via_NCM_BRA_matched.dta", clear

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
	u "${public}/Tariffs/WITS_tariffs_to_CNAE10_via_NCM_BRA_matched.dta", clear
	
	reshape wide tariff_mean, i(cnae ibgesubsector) j(year)
	*u "${public}/Tariffs/annual_tariffs_CNAE10_BRA.dta", clear
	
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
	
	/* Missing niv100 replace appropriately */
	tab cnae10 if niv100==. 
	tab cnae10 if niv100==., nolabel
	
	tab ibgesubsector if niv100==.
	
	* 22152	Edição de livros, revistas e jornais
	* 92118	Producao  de filmes cinematograficos e fitas de video
	replace niv100 = 1530 if inlist(cnae10,92118,22152)
	
	*27235	Produção de semi-acabados de aço
	replace niv100 = 510 if cnae10==27235
	
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
	label data "TRAINS tariffs by CNAE via NCM + Kume (2003) tariffs by Niv50 + Muendler (2003) intermediates by Niv 100"
	saveold "${public}/Tariffs/Tariffs_TRAINScnae10_via_NCM_KUMEniv50_MUENDLERniv100.dta", replace
	
}
