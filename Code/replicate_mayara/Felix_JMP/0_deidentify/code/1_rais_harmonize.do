/*
	Mayara Felix
	Goal:	Loop through all raw files to see its contents, identify variables
			and harmonize them across datasets
	
	Files were unzipped from the terminal using 7za command as follows
	
	7za -e filename
	
	then moved to unziped folder using mv command.

*/

clear all
set more off
*unicode encoding set "ISO-8859-1"
*unicode encoding set "Cp037"
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
	global encrypted 		"Z:/"
	global dictionaries		"M:/raisdictionaries/harmonized"
}

* Output folder for .dta files
global dta				"${encrypted}/output/dta"

* All files
local files_txt: dir "${encrypted}/unzipped" files "*.txt"
local files_TXT: dir "${encrypted}/unzipped" files "*.TXT"

* These files have no actual delimiter, failed to read contents in first attempt
* Exclude them from files we'll loop through
local files_problem "MG2016ID.txt MS2016ID.txt PE2016ID.txt	PI2016ID.txt SC2016ID.txt SE2016ID.txt"

* Re-set files_txt list to exclude problem files
local files_txt: list files_txt- files_problem

local cleandate = 20180829

local import = 1
local append = 1


**** Import to save description of all datasets *****

if `import'==1{

	foreach f in `files_txt' `files_TXT'{
	
		di in red "Reading contents of `f'"
		
		* Import just first 100 obs so that descsave can tell variable type
		import delimited "${encrypted}/unzipped/`f'", delimiter(";") rowrange(1:100) clear 
		
		* If delimiter is wrong, import as pipe
		cap confirm variable v1, exact
		if !_rc{
			di "File `f' delimited with tab or pipe"
			import delimited "${encrypted}/unzipped/`f'", delimiter("|") rowrange(1:100) clear 
		}
		
		local dtaname = subinstr("`f'",".txt","",.)
		local dtaname = subinstr("`dtaname'",".TXT","",.)

		* Save attributes of each dataset
		descsave, sa("${dta}/descsave_`dtaname'.dta", replace)

		u "${dta}/descsave_`dtaname'.dta", clear

		sort name
		keep order name type format varlab
		gen file = "`f'"
		saveold "${dta}/descsave_`dtaname'.dta", replace
	}
}

***** Append all then erase individual files *****

if `append'==1{
	local j = 1
	foreach f in `files_txt' `files_TXT'{
	
		di in red "Appending contents of `f'"
				 
		local dtaname = subinstr("`f'",".txt","",.)
		local dtaname = subinstr("`dtaname'",".TXT","",.)
		
		if `j'==1{
			u "${dta}/descsave_`dtaname'.dta", clear
			saveold "${dta}/descsave_rais_files.dta", replace
			erase "${dta}/descsave_`dtaname'.dta"
		}
		else{
			u "${dta}/descsave_rais_files.dta", clear
			append using "${dta}/descsave_`dtaname'.dta"
			erase "${dta}/descsave_`dtaname'.dta"
		}
		
		saveold "${dta}/descsave_rais_files.dta", replace
		local j = `j'+1
	}
	
	***** Outsheet *****
	
	u "${dta}/descsave_rais_files.dta", clear
	sort name file
	
	bys name: egen totfiles = count(order)
	egen minfiles = min(totfiles)
	egen maxfiles = max(totfiles)
	
	export excel using "${dictionaries}/descsave_rais_files_`cleandate'.xlsx", firstrow(var) replace
	erase "${dta}/descsave_rais_files.dta"
}
