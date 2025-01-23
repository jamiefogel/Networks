/*
	Mayara Felix
	Goal:	Import RAIS .txt identified files and save as .dta
			
			1) 	Files were unzipped from the terminal using 7za command as follows
	
				7za -e filename
	
				then moved to unziped folder using mv command.
			
			2) Then code rais_harmonize was run to identify the contents of each
			   text file, and to decide which contents to keep and save to .dta
			   (e.g. some variables are only available for very recent years,
			   and several variables are just computed from other variables, so
			   no need to keep them.
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

else if c(username)=="p13861161" & c(os)=="Windows" {
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara"
	global dictionaries		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara"
	global dictionaries		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdictionaries/harmonized"
}

* Output folder for rais .dta files
global importedrais			"${encrypted}/output/dta/importedrais"

* All files
* XX Added , respect to respect case
local files_txt: dir "${encrypted}/unzipped" files "*.txt", respect 
local files_TXT: dir "${encrypted}/unzipped" files "*.TXT", respect

* XX Added this to filter on year to improve parallelizability
if 1==1{
	local first_year = 2009
	local last_year  = 2009
	// Loop over each file in the combined list
	foreach file in `files_TXT' {
		di `"`file'"'
		// Extract the year portion (YYYY) from the filename using regex
		local year = substr("`file'", 3, 4)
		di `year'
		// Check if the year falls within the desired range (1994–1998)
		if inrange(`year', `first_year', `last_year') {
			// Add the file to the filtered list
			local filtered_files "`filtered_files' `file'"
		}
	}
}

local files_TXT `"`filtered_files'"'

di `"`files_txt'"'
di `"`files_TXT'"'


* These files have no actual delimiter. Exclude them from files we'll loop through
local files_problem "MG2016ID.txt MS2016ID.txt PE2016ID.txt	PI2016ID.txt SC2016ID.txt SE2016ID.txt"
local files_txt: list files_txt- files_problem

local import = 1

* Display date and time
di "Starting import code on $S_DATE at $S_TIME"

if `import'==1{
	
	import excel using "${dictionaries}/descsave_rais_files_20180829_clean.xlsx", firstrow sheet(clean)
	replace file = upper(file)
	keep if keep=="yes"		// Keep only list of variables to be kept in the import
	keep name file cleanname cleanlabel cleanorder cleanlevel
	
	* XX Dealing with some weird issue
	replace file = "GO2009ID.TXT" if file=="GO2009ID2.TXT"
	drop if inlist(file, "SP2002ID1.TXT", "SP2009ID1.TXT", "SP2010ID1.TXT")
	replace file = "SP2002ID.TXT" if file=="SP2002ID2.TXT"
	replace file = "SP2009ID.TXT" if file=="SP2009ID2.TXT"
	replace file = "SP2010ID.TXT" if file=="SP2010ID2.TXT"
	
	* Check that each cleanname is unique within file
	isid file cleanname
	
	tempfile harmonized
	sa `harmonized'
	
	* Import, keep relevant, rename, simple clean
	foreach f in /*`files_txt'*/ `files_TXT' {
		
		di "Importing `f'"
		
		* Import with most common delimiter
		* XX import delimited "${encrypted}/unzipped/`f'", delimiter(";") /*rowrange(1:10)*/ clear 
		import delimited "${encrypted}/unzipped/`f'", delimiter(";")  clear encoding(utf8)
		
		* Other delimiter is pipe
		cap confirm variable v1, exact
		if !_rc{
			di "File `f' delimited with tab or pipe"
			*XX import delimited "${encrypted}/unzipped/`f'", delimiter("|") /*rowrange(1:10)*/ clear 
			import delimited "${encrypted}/unzipped/`f'", delimiter("|")  clear encoding(utf8)
		}
		
		* Get clean names and labels from harmonized file. Sort to desired final order of vars.
		preserve
			u `harmonized', clear
			keep if file=="`f'"
			sort cleanorder
			
			valuesof name
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
		}
		
		* Gender variable sometimes is spelled out. Standard in older datasets
		* is 1: MASCULINO, 2 FEMININO
		cap replace gender = subinstr(gender,"MASCULINO","1",.)
		cap replace gender = subinstr(gender,"FEMININO","2",.)
		
		* Contract types for years 2002-2011. See diferencas_layout.xslx
		cap replace contracttype="10" if contracttype=="CLT U/PJ IND"
		cap replace contracttype="15" if contracttype=="CLT U/PF IND"
		cap replace contracttype="20" if contracttype=="CLT R/PJ IND"
		cap replace contracttype="25" if contracttype=="CLT R/PF IND"
		cap replace contracttype="30" if contracttype=="ESTATUTARIO"
		cap replace contracttype="35" if contracttype=="ESTAT N/EFET"
		cap replace contracttype="40" if contracttype=="AVULSO"
		cap replace contracttype="50" if contracttype=="TEMPORARIO"
		cap replace contracttype="55" if contracttype=="APREND CONTR"
		cap replace contracttype="60" if contracttype=="CLT U/PJ DET"
		cap replace contracttype="65" if contracttype=="CLT U/PF DET"
		cap replace contracttype="80" if contracttype=="DIRETOR"
		cap replace contracttype="90" if contracttype=="CONT PRZ DET"
		cap replace contracttype="95" if contracttype=="CONT TMP DET"
		
		* IBGE subsector codes are strings in 1985-2010. After that coded.
		* Change strings to coding convention post 2011
		cap replace ibgesubsector="1" if ibgesubsector=="EXTR MINERAL"
		cap replace ibgesubsector="2" if ibgesubsector=="MIN NAO MET"
		cap replace ibgesubsector="3" if ibgesubsector=="IND METALURG"
		cap replace ibgesubsector="4" if ibgesubsector=="IND MECANICA"
		cap replace ibgesubsector="5" if ibgesubsector=="ELET E COMUN"
		cap replace ibgesubsector="6" if ibgesubsector=="MAT TRANSP"
		cap replace ibgesubsector="7" if ibgesubsector=="MAD E MOBIL"
		cap replace ibgesubsector="8" if ibgesubsector=="PAPEL E GRAF"
		cap replace ibgesubsector="9" if ibgesubsector=="BOR FUM COUR"
		cap replace ibgesubsector="10" if ibgesubsector=="IND QUIMICA"
		cap replace ibgesubsector="11" if ibgesubsector=="IND TEXTIL"
		cap replace ibgesubsector="12" if ibgesubsector=="IND CALCADOS"
		cap replace ibgesubsector="13" if ibgesubsector=="ALIM E BEB"
		cap replace ibgesubsector="14" if ibgesubsector=="SER UTIL PUB"
		cap replace ibgesubsector="15" if ibgesubsector=="CONSTR CIVIL"
		cap replace ibgesubsector="16" if ibgesubsector=="COM VAREJ"
		cap replace ibgesubsector="17" if ibgesubsector=="COM ATACAD"
		cap replace ibgesubsector="18" if ibgesubsector=="INST FINANC"
		cap replace ibgesubsector="19" if ibgesubsector=="ADM TEC PROF"
		cap replace ibgesubsector="20" if ibgesubsector=="TRAN E COMUN"
		cap replace ibgesubsector="21" if ibgesubsector=="ALOJ COMUNIC"
		cap replace ibgesubsector="22" if ibgesubsector=="MED ODON VET"
		cap replace ibgesubsector="23" if ibgesubsector=="ENSINO"
		cap replace ibgesubsector="24" if ibgesubsector=="ADM PUBLICA"
		cap replace ibgesubsector="25" if ibgesubsector=="AGRICULTURA"

		
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
						estabopendate estabclosedate estabbaixadate{
			cap replace `var' = . if `var'==0
		}
		
		local dtaname = subinstr("`f'",".txt","",.)
		local dtaname = subinstr("`dtaname'",".TXT","",.)
		
		qui compress
		qui saveold "${importedrais}/`dtaname'.dta", replace
		
	} /* Close file loop */
} /* Close import boolean */

* Display date and time
di "Done with import code on $S_DATE at $S_TIME"
clear
exit
