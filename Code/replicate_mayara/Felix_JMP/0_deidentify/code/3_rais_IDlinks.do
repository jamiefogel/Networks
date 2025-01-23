/*
	Mayara Felix
	Goal:	Remove identifiable information from RAIS .dta datasets
			Repalce identifiers with fake identifiers
			
	For workers:
		- Use PIS as ID. Randomly generate unique ID number based on PIS.
		- Drop all other IDs (CPF, CTPS, etc.)
		- Any names already dropped at import (never imported to Stata)
	For firms:
		- Use CNPJ or CEI as ID.
		- If ID is CNPJ, randomly generate ID for first 8 digits. Keep branch number
		(next 4 digits).
		- If ID is CEI, randomly generate ID for entire CEI.
		- Keep names in the ID links file, drop it from analysis files
*/

clear all
set more off
unicode encoding set "latin1"
set seed 34317154

* Mayara mounting on server
if c(username)=="mfelix"{
	global encrypted 		"/proj/patkin/raismount"
	global dictionaries		"/proj/patkin/dictionaries/harmonized"
}
* Mayara mounting locally on Mac
else if c(username)=="mayara"{
	global encrypted 		"/Volumes/raismount"
	global dictionaries		"/Volumes/dictionaries/harmonized"
}
* Mayara mounting locally on Windows
else if c(username)=="Mayara"{
	global encrypted 		"Z:"
	global dictionaries		"M:/dictionaries/harmonized"
}

else if c(username)=="p13861161" & c(os)=="Windows" {
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara"
	global dictionaries		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara"
	global dictionaries		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdictionaries/harmonized"
}

* Change directory to encrypted output folder just in case something is saved
* to current directory
cd "${encrypted}/output"

* Output folder for rais .dta files
global importedrais			"${encrypted}/output/dta/importedrais"
global IDlinksrais			"${encrypted}/output/dta/IDlinksrais"

* All files
local files_dta: dir "${importedrais}" files "*.dta", respect 

*local files_dta "Estb1985ID.dta"

local getids 		= 1
local idsappend		= 1
local eraseind		= 1

* Display date and time
di "Starting IDlinks code on $S_DATE at $S_TIME"

if `getids'==1{

	* File listing all variables in each dataset
	import excel using "${dictionaries}/descsave_rais_files_20180829_clean.xlsx", firstrow sheet(clean)
	replace file = upper(file)
	keep if (keep=="yes"	& is_id=="yes") | cleanname=="estabid_type"	// Keep only variables imported to Stata & vars that are IDs
	keep file cleanname cleanlevel
	
	replace file = subinstr(file,".txt",".dta",.)
	replace file = subinstr(file,".TXT",".dta",.)
	
	* Check that each cleanname is unique within file
	isid file cleanname
	
	tempfile harmonized
	sa `harmonized'
	
	* Import, keep relevant, rename, simple clean
	foreach dtaname in `files_dta'{
		
		di "Getting IDs for `dtaname'"
		
		* Load worker ID variables
		
		u `harmonized', clear
		keep if file=="`dtaname'" & cleanlevel=="worker"
		des
		if `r(N)'>0{
			levelsof cleanname, local(workerids) clean
			
			qui u `workerids' using "${importedrais}/`dtaname'" /* if _n==1 */, clear
			duplicates drop
			*gen file = "`dtaname'"
			*replace file = subinstr(file,".dta","",.)
			*order file
			qui compress
			qui saveold "${IDlinksrais}/workerIDs_`dtaname'", replace
		}
		else{
			di in red "`dtaname' does not have any worker IDs"
		}
		
		* Load estab ID variables
		u `harmonized', clear
		keep if file=="`dtaname'" & cleanlevel=="estab"
		des
		if `r(N)'>0{
			levelsof cleanname, local(estabids) clean
			
			qui u `estabids' using "${importedrais}/`dtaname'" /* if _n==1 */, clear
			duplicates drop
			cap tostring estabid_type, replace
			*gen file = "`dtaname'"
			*replace file = subinstr(file,".dta","",.)
			*order file
			qui compress
			qui saveold "${IDlinksrais}/estabIDs_`dtaname'", replace
		}
		else{
			di in red "`dtaname' does not have any estab IDs"
		}
	} /* Close file loop */
} /* Close import boolean */


if `idsappend'==1{
	
	* Append all worker ids
	local i = 1
	foreach dtaname in `files_dta'{
		cap confirm file "${IDlinksrais}/workerIDs_`dtaname'"
		if !_rc{
			if `i'==1{
				u "${IDlinksrais}/workerIDs_`dtaname'", clear
				local i = 0
			}
			else if `i'!=1{
				append using "${IDlinksrais}/workerIDs_`dtaname'"
			}
		}
	}
	
	* Save appended
	duplicates drop
	cap saveold "${IDlinksrais}/rais_workerIDs.dta", replace
	
	* Append all estab ids
	* Note: estabid_type is string for some observations in several
	*		datasets. Would not notice by descsaving based on first 100 obs.
	*		occurs if filed out incorrectly or database assempled incorrectly
	*		for those obs. It's very few observations, however.
	*		force destring and drop if missing.
	local i = 1
	foreach dtaname in `files_dta'{
		cap confirm file "${IDlinksrais}/estabIDs_`dtaname'"
		if !_rc{
			if `i'==1{
				u "${IDlinksrais}/estabIDs_`dtaname'", clear
				cap destring estabid_type, replace force
				cap keep if !missing(estabid_type)
				cap saveold "${IDlinksrais}/rais_estabIDs.dta", replace
				local i = 0
			}
			else if `i'!=1{
				u "${IDlinksrais}/estabIDs_`dtaname'", clear
				cap destring estabid_type, replace force
				cap keep if !missing(estabid_type)
				append using "${IDlinksrais}/rais_estabIDs.dta"
				saveold "${IDlinksrais}/rais_estabIDs.dta", replace
			}
		}
	}
	
	u "${IDlinksrais}/rais_estabIDs.dta", clear
	duplicates drop
	saveold "${IDlinksrais}/rais_estabIDs.dta", replace
}

if `eraseind'==1{

	* Erase individual files
	local erase_w: dir "${IDlinksrais}" files "workerIDs*.dta"
	local erase_e: dir "${IDlinksrais}" files "estabIDs*.dta"
	
	foreach f in `erase_w' `erase_e'{
		erase "${IDlinksrais}/`f'"
	}

}
* Display date and time
di "Done with IDlinks code on $S_DATE at $S_TIME"
clear
exit
