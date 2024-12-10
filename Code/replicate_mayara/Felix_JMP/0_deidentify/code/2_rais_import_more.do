/*
	Mayara Felix
	Goal:	Import additional files from RAIS
*/

clear all
set more off
unicode encoding set "latin1"
set seed 34317154

* Mayara mounting on server
if c(username)=="mfelix"{
	global encrypted 		"/proj/patkin/raismount"
	global dictionaries		"/proj/patkin/raisdictionaries/harmonized"
}
* Mayara mounting locally on Mac
else if c(username)=="mayara"{
	global encrypted 		"/Volumes/raismount"
	global dictionaries		"/Volumes/raisdictionaries/harmonized"
}
* Mayara mounting locally on Windows
else if c(username)=="Mayara"{
	global encrypted 		"Z:"
	global dictionaries		"M:/raisdictionaries/harmonized"
}

* Output folder for rais .dta files
global importedrais			"${encrypted}/output/dta/importedrais"

* These files have no actual delimiter. Exclude them from files we'll loop through
local files_more "ES_RJ_MG2003"

local import = 1

* Display date and time
di "Starting import code on $S_DATE at $S_TIME"

/*
File ES_RJ_MG2003 is missing. Its zip file contains file sudeste.TXT. But this is not the
2004 sudesde.TXT file, it's 2003 for those 3 states, checked. 
When renamed in batch, the 2004 sudeste file overwrote this.
So now renamed file as ES_RJ_MG2003.TXT. Go import it.

Variables are the same as sp2003 in the harmonizing sheet
*/

	import excel using "${dictionaries}/descsave_rais_files_20180829_clean.xlsx", firstrow sheet(clean)
	keep if keep=="yes"	& file=="sp2003.TXT"	// Keep only list of variables to be kept in the import
	keep name cleanname cleanlabel cleanorder cleanlevel
	
	tempfile harmonized
	sa `harmonized'
	
	* Check that each cleanname is unique within file
	isid cleanname
	
	* Import with most common delimiter
	import delimited "${encrypted}/unzipped/nodelim/ES_RJ_MG2003.TXT", delimiter(";") /* rowrange(1:10000) */  clear 
	
	* Get clean names and labels from harmonized file. Sort to desired final order of vars.
	preserve
		u `harmonized', clear
		sort cleanorder
		
		qui valuesof name
			local rawnames = r(values)
		qui valuesof cleanname		
			local cleannames = r(values)
		qui valuesof cleanlabel
			local cleanlabels = r(values)
	restore
	
	* Keep and rename
	keep `rawnames'
	ren (`rawnames') (`cleannames')
	labvars `cleannames' `cleanlabels'
	order `cleannames'
	
	*********** Basic cleaning ***********
	* In Brazil, periods indicate thousands, commas indicate decimal places
	foreach var in 	contractsal contracthours earningsavgmw earningsavgnom earningsdecmw ///
					earningsdecnom empmonths{
		cap replace `var' = subinstr(`var',".","",.)
		cap replace `var' = subinstr(`var',",",".",.)
		cap destring `var', replace
	}
	
	* All strings, remove "ignored" categories
	qui ds, v(32) has(type string)
	
	foreach var in `r(varlist)'{
		cap replace `var' = subinstr(`var',"{ñ class}",".n",.)
		cap replace `var' = subinstr(`var',"{ñ class",".n",.)
		cap replace `var' = subinstr(`var',"{ñ cl",".n",.)
		cap replace `var' = subinstr(`var',"{ñ c",".n",.)
		cap replace `var' = subinstr(`var',"{ñ",".n",.)
		cap replace `var' = subinstr(`var',"IGNORADO",".n",.)
		cap replace `var' = subinstr(`var',"OUTR/IGN",".n",.)
	}
	
	* CBO variables sometimes include "CBO " prefix
	foreach var in cbo cbo94 cbo02{
		cap replace `var' = subinstr(`var',"CBO ","",.)
	}
	
	* CNAE and CBO codes sometimes come with dashes. Replace with leading zero
	foreach var in cbo cbo94 cbo02 cnaeclass95 cnaeclass02 cnaesubclass02{
		cap replace `var' = subinstr(`var',"-","0",.)
		cap replace `var' = subinstr(`var',"-","0",.)
	}
	
	* Other number-valued variables that might be stored as string
	foreach var in 	admmonth admtype  ///
					contracttype contractsaltype ///
					cbo cbo94 cbo02 situacaovinculo sepreason emp1231 sepday sepmonth ///
					age agegroup educ nationality race gender disability ///
					ibgesubactivity ibgesubsector juridnature ///
					cnaeclass95 cnaeclass02 cnaesubclass02  estabpat indsimples indraisnegativa{
		cap destring `var', replace
	}
	
	* Variables that we must have in numbers. If strings then
	* replace with missing
	foreach var in municipality admyear{
		cap destring `var', replace force
	}
	
	* Missing worker and firm IDs, and establishment open/close dates often coded as zero. Replace with missing
	foreach var in workerid_pis workerid_cpf estabid_cnpj_cei ///
					estabid_constructioncei ///
					estabopendate estabclosedate estabbaixadate sepday sepmonth sepreason{
		cap replace `var' = . if `var'==0
	}
	
	qui compress
	qui saveold "${importedrais}/ES_RJ_MG2003.dta", replace
	
	* Check states
	tostring municipality, gen(state)
	replace state = substr(state,1,2)
	tab state
	clear
	exit
