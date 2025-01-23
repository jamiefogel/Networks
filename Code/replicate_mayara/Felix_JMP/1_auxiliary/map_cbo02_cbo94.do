/*
	Use RAIS data to map CBO02 to CBO04
	Years 2003-2009 report both codes for the same individuals
	Use that to construct mapping for earlier and later years
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

else if c(username)=="p13861161" & c(os)=="Windows" {
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara"
	global dictionaries		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara"
	global dictionaries		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdictionaries/harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}

* Test boolean
local ctest = 0
if `ctest'==1{
	local cond "if _n<100"
}
else{
	local cond ""
}

local get2002files 		= 1
local rais_cbo_mapping	= 1

*log using "${monopsonies}/code/setup/z_log/map_cbo02_cbo94", replace

******************************************************************
******** Collect CBO02-CBO94 pairs for files reporting both ******
******************************************************************

if `get2002files'==1{

	* List state sepcific worker files
	foreach state in 	AC	AL	AM	AP	BA	CE	DF	///
						ES	GO	MA	MG	MS	MT	PA ///
						PB	PE	PI	PR	RJ	RN	RO ///
						RR	RS	SC	SE	SP	TO{
						
			forvalues y=2003/2009{
				local files_`state'`y': dir "${deIDrais}/20191213/dta" files "deID_`state'`y'*.dta"
			}
	}

	
	local i = 1	
	foreach state in 	AC	AL	AM	AP	BA	CE	DF	///
						ES	GO	MA	MG	MS	MT	PA ///
						PB	PE	PI	PR	RJ	RN	RO ///
						RR	RS	SC	SE	SP	TO{
					
			forvalues y=2003/2009{			
				foreach f in `files_`state'`y''{
				
				u "${deIDrais}/dta/`f'" `cond', clear
				qui ds cbo*, v(32)
				if "`r(varlist)'"=="cbo94" | "`r(varlist)'"=="cbo"{
						di in red "`f' does not have both cbo94 and cbo02"
				}
				else{
					di in red "Use `f' for mapping"
					keep fakeid_worker cbo94 cbo02
					keep if !missing(cbo94) & !missing(cbo02)
					
					`tools'duplicates drop fakeid_worker, force
					`tools'collapse (count)  raisobs=fakeid_worker, by(cbo*)
				
					tempfile ocups`i'
					sa `ocups`i''
					local i = `i'+1
				}
				} /* Close f list */
			} /* Close year loop */
		} /* Close state loop */
	* Append all
	local j = `i'-1
	
	u `ocups1', clear
	forvalues k=2/`j'{
		append using `ocups`k''
		`tools'collapse (sum)  raisobs, by(cbo*)
	}
	
	`tools'collapse (sum)  raisobs, by(cbo94 cbo02)
	
	label var raisobs "RAIS 2003-2009 obs with this occcupational pairing"
	misstable sum
	saveold "${dictionaries}/rais_cbo94_cbo02_pairs.dta", replace

} /* Match boolean */

******************************************************************
*********** Create mappings between two occupation codes *********
******************************************************************

if `rais_cbo_mapping'==1{

	u "${dictionaries}/rais_cbo94_cbo02_pairs.dta", clear

	* CBO94 to CBO02 best mapping (that is mapping pre
	bys cbo94: `tools'egen cbo94obs = sum(raisobs)
	bys cbo02: `tools'egen cbo02obs = sum(raisobs)

	gen pct_cbo94 = raisobs/cbo94obs
	gen pct_cbo02 = raisobs/cbo02obs

	bys cbo94: `tools'egen max_pct_cbo94 = max(pct_cbo94)
	bys cbo02: `tools'egen max_pct_cbo02 = max(pct_cbo02)

	** Assign cbo94 to cbo02 based on max % pairing ***
	* some cbo02s have multiple cbo94 assignments
	* randomly choose which
	preserve
		keep if pct_cbo02==max_pct_cbo02
		keep cbo02 cbo94 max_pct_cbo02
		
		duplicates tag cbo02, gen(dup)
		tab dup
		
		sample 1, count by(cbo02)
		
		drop dup
		
		label data "Pct of CBO02-CBO94 pairs for assigned CBO94 - RAIS 2003-2009"
		saveold "${dictionaries}/rais_cbo02_to_cbo94.dta"
	restore

	** Assign cbo02 to cbo94 based on max % pairing ***
	* Every cbo94 can be uniquely assigned to a cbo02
	preserve
		keep if pct_cbo94==max_pct_cbo94
		keep cbo02 cbo94 max_pct_cbo94
		
		isid cbo94

		order cbo94
		label data "Pct of CBO02-CBO94 pairs for assigned CBO02 - RAIS 2003-2009"
		saveold "${dictionaries}/rais_cbo94_to_cbo02.dta"
	restore
	
	clear
}

* log close
