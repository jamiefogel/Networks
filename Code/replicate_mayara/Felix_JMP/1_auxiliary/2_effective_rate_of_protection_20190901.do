/*
	Mayara Felix
	
	GOAL: 				Compute effective rate of protection at CNAE10 level
						Read notes for methodology
						
	Inputs:				1985 niv 50 intersectoral technical coefficients matrix
						CNAE10 level tariffs from TRAINS
						niv50-CNAE correspondence from CONCLA
						CNAE-CNAE10 correspondence from CONCLA
						
						***** This leads to worse matches:
					
						Niv 50 Kume (2003) tariff from Kovak (2013)
						niv80-100 from muendler
	
	Note: 	Decided to keep using actual Real/US exchange rates for Brazil
			But use separate shocks for the exchange rate change versus for the import tariffs
	
			But note that this means that the 1994-1990 change in exchange-rate adjusted Erp
			has the opposite sign as the 1994-1990 change in adjusted Erp alone, so its effect
			on employment should be interpreted with flipped sign. That is because
			it is also a decrease in the Real/USD Xchange rate (eg a Real appreciation) that increases
			imports. In other words:
			
			(Import Tariff decline) 							= Negative number => more imports, less employment

			(Exchange Rate Decline) x (Import Tariff decline) 	= Positive number => more imports, less employment
			
	
	An alternative is to use changes in USD/Real instead of changes in Real/USD so that the sign stays
	the same as in the Import tariff decline.
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
	******* Exchange rates *******
	// Nominally Brazilian currency was worth a lot more than the dollar in 1990
	// That pattern is still there though not as stark if use effective exchange rates,
	// adjusted using price indices in the US and euro zone.
	// EG see world Bank series data.
	// But for here keep using the nominal. That's the real shock.
	// Just remember that exchange-rate adjusted shocks will have opposite sign
	// of regular shocks.
		preserve
		
			insheet using "${public}/Exchange rates/ipeadata_real_usd_1985_2018_annual.csv",  clear
		
			tempfile Xchange
			sa `Xchange'
		restore

	******* Intersectorial coefficients matrix from 1985 Brazilian IO table *******
		import excel  "${public}/IO tables/TABELA20.XLS", clear
		
		keep if _n>=6
		drop B
		ren A niv50
		destring _all, replace

		// Column (j): 	what will be used to merge with CNAE10; sector for which we are computing EPR
		// Row (i):		what we'll merge with Kume tariffs to sum across
		
		ren niv50 niv50_i		
		ds niv50_i, not
		local i = 1
		foreach var of varlist `r(varlist)'{
			preserve
				keep niv50_i `var'
				ren `var' coeff
				gen niv50_j = `i'
				tempfile f`i'
				sa `f`i''
			restore
			local i = `i'+1
		}
		local j = `i'-1
		
		u `f1', clear
		forvalues y =2/`j'{
			append using `f`y''
		}
		
		// Niv50 goes from 1 to 43 skipping 9
		sum niv50_j
		replace niv50_j = niv50_j + 1 if niv50_j>=9
		sum niv50_j
		
		// Drop coefficients for non-tradables (no tariffs for them)
		drop if niv50_i>32 | niv50_j>32
		
		tempfile coeffs
		sa `coeffs'
		
	******* niv50 to CNAE from CONCLA ******
	import excel using "${public}/Tariffs/Concordances/SCN_CNAE.xls", clear
	keep if _n>=5
	keep A C
	ren A niv50
	ren C cnae
	keep if !missing(cnae)
	replace niv50=niv50[_n-1] if missing(niv50)
	destring niv50, replace
	replace cnae = subinstr(cnae,"-","",.)
	destring cnae, replace
	gen cnae10 = cnae
	
	******** CNAEtocnae10 fixes *****
	preserve
		import excel using "${public}/Tariffs/Concordances/CNAE10xCNAE.xlsx", firstrow clear
		replace cnae = subinstr(cnae,".","",.)
		replace cnae10 = subinstr(cnae10,".","",.)
		replace cnae = subinstr(cnae,"-","",.)
		replace cnae10 = subinstr(cnae10,"-","",.)
		
		destring cnae cnae10, replace
		tempfile fixes
		sa `fixes'
	restore
	
	merge 1:m cnae using `fixes', update replace 
	keep if _merge!=2
	replace niv50 = . if _merge==5		// Update niv50 of updated cnae
	drop _merge
	keep niv50 cnae10
	collapse (max) niv50, by(cnae10)
	
	keep if !missing(niv50)
	tempfile cnae10toniv50
	sa `cnae10toniv50'
		
	******* Kume (2003) tariffs from Kovak (2013) *******
		u "${public}/other/Kovak (2013)/kume_etal_tariff.dta", clear
		
		keep niv50 tariff*
		foreach var of varlist tariff*{
			replace `var' = `var'/100
		}
		renvars tariff*, subst("tariff" "kume")
		
		tempfile kume
		sa `kume'
		
	******* Compute the coefficients-weighted components of ERP *******
		* Get input Kume Niv50 tariffs t_i for row (i)
		u `kume', clear
		
		tab niv50
		ren niv50 niv50_i
		
		* Multiply each input tariff t_i with a_ij 1985 Niv 50 Leontieff coefficients
		merge 1:m niv50_i using `coeffs'
		
		* Drop non-tradable from rows (missing tariffs)
		assert _merge!=2 // Non-tradable industries don't have nominal tariffs
		drop _merge
		
		order niv50_j niv50_i coeff
		drop niv50_i
		
		// Si_aij: Sum of leontieff coeffs across i for each j
		bys niv50_j: egen Si_aij = sum(coeff)	
		sum Si_aij, detail
		
		// Si_ti_aij: Sum of (kume tariff*leontieff coeff) across i for each j
		forvalues y=1987/1998{
			gen double ti_aij`y' = kume`y'*coeff
			bys niv50_j: egen Si_ti_aij`y' = sum(ti_aij`y')
		}
		
		keep niv50_j Si_aij Si_ti_aij*
		duplicates drop
		
		reshape long Si_ti_aij, i(niv50_j Si_aij) j(year)
	
		tempfile ai
		sa `ai'
		
	***** Compute ERP at CNE10 level using TRAINS tariffs *****
		u "${public}/Tariffs/WITS_tariffs_to_CNAE10_via_NCM_BRA_matched.dta"
		
		merge m:1 cnae10 using "${dictionaries}/rais_cnae10_to_ibgesubsector.dta"
		keep if _merge==3
		drop _merge

		merge m:1 cnae10  using `cnae10toniv50', keepusing(niv50) keep(1 3) nogen 
		
		* Coefficient
		ren niv50 niv50_j
		merge m:1 niv50_j year using `ai'
	
		keep if _merge!=1

		preserve
			keep if _merge==2
			keep year niv50 Si*
			saveold "${public}/Tariffs/nontradables_input_tariff_sum_1985_1998.dta", replace
		restore
		
		keep if _merge==3
		drop _merge
		replace tariff_mean = tariff_mean/100
		gen double ErpTRAINS = (tariff_mean - Si_ti_aij)/(1-Si_aij)
		
		ren  tariff_mean TRAINS
		
		* Convert to Exchange-rate adjusted ERp
		merge m:1 year using `Xchange'
		keep if _merge==3
		drop _merge
		
		gen double RealErpTRAINS = ErpTRAINS*real_usd
		
		order cnae10 ibgesubsector year TRAINS ErpTRAINS RealErpTRAINS real_usd niv50_j 
		
		foreach var of varlist TRAINS ErpTRAINS RealErpTRAINS{
			replace `var' = `var'*100
		}
		
		sum TRAINS ErpTRAINS RealErpTRAINS
		saveold "${public}/Tariffs/cnae10_erp_1985_1998_fixed.dta", replace
