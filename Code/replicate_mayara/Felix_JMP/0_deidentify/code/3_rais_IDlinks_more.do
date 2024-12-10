/*
	Mayara Felix
	Goal:	Remove identifiable information from newly added RAIS .dta datasets
			Repalce identifiers with fake identifiers
	
	20181010:	Getting IDs from file ES_RJ_MG2003.dta, which was in the 
				ES_RJ_MG2003.7z file under name sudeste.TXT, and because of
				that was overwritten by sudeste.7z contents, for year 2004.
			
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

* Change directory to encrypted output folder just in case something is saved
* to current directory
cd "${encrypted}/output"

* Output folder for rais .dta files
global importedrais			"${encrypted}/output/dta/importedrais"
global IDlinksrais			"${encrypted}/output/dta/IDlinksrais"

* Additional files
local files_dta "ES_RJ_MG2003.dta"

local getids 				= 1
local idsappend_more		= 1
local eraseind_more			= 1

* Display date and time
di "Starting IDlinks code on $S_DATE at $S_TIME"

if `getids'==1{
	
	* Import, keep relevant, rename, simple clean
	foreach dtaname in `files_dta'{
		
		di "Getting IDs for `dtaname'"
		
		qui u workerid_pis workerid_cpf using "${importedrais}/`dtaname'" /* if _n==1 */, clear
		duplicates drop
		
		qui compress
		qui saveold "${IDlinksrais}/workerIDs_`dtaname'", replace
		
		qui u estabid_cnpjcei estabid_constructioncei estabid_type using "${importedrais}/`dtaname'" /* if _n==1 */, clear
		keep if !missing(estabid_type)
		qui compress
		qui saveold "${IDlinksrais}/estabIDs_`dtaname'", replace
		
	} /* Close file loop */
} /* Close GET IDS boolean */

if `idsappend_more'==1{
	
	foreach dtaname in `files_dta'{
		u "${IDlinksrais}/rais_estabIDs.dta", clear
		append using  "${IDlinksrais}/estabIDs_`dtaname'"
		duplicates drop
		saveold "${IDlinksrais}/rais_estabIDs.dta", replace
		
		u "${IDlinksrais}/rais_workerIDs.dta", clear
		append using  "${IDlinksrais}/workerIDs_`dtaname'"
		duplicates drop
		saveold "${IDlinksrais}/rais_workerIDs.dta", replace
	}
}

if `eraseind_more'==1{
	
	foreach dtaname in `files_dta'{
		erase "${IDlinksrais}/workerIDs_`dtaname'"
		erase "${IDlinksrais}/estabIDs_`dtaname'"
	}

}
* Display date and time
di "Done with IDlinks code on $S_DATE at $S_TIME"
clear
exit
