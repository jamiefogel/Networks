/*
	Mayara Felix
	
	12/13/2019	- No longer delete observations whose IDs are not valid IDs
				ID #s could have been messed up by data entry, but don't want to lose
				those workers or firms.
	
	For firms
	
	fakid_firm:		de-identified first 8 digits of CNPJ or de-identified CEI
	fakid_estab:	de-identified first 8 digits of CNPJ or de-identified CEI, 
					with last 4 digits being the branch # (in case of CNPJ IDs)
					If ID is CEI, then fakeid_firm = fakeid_estab
					
*/

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
	global encrypted 		"/proj/patkin/raismount_mfelix"
	global dictionaries		"/proj/patkin/raisdictionaries/harmonized"
	global deIDrais			"/proj/patkin/raisdeidentified"
	global monopsonies		"/proj/patkin/projects/monopsonies"
	global public			"/proj/patkin/publicdata"
}
* Mayara mounting locally on Mac
else if c(username)=="mayara"{
	global encrypted 		"/Volumes/raismount_mfelix"
	global dictionaries		"/Volumes/proj_atkin/raisdictionaries/harmonized"
	global deIDrais			"/Volumes/proj_atkin/raisdeidentified"
	global monopsonies		"/Volumes/proj_atkin/projects/monopsonies"
	global public			"/Volumes/proj_atkin/publicdata"
	
}
* Mayara mounting locally on Windows
else if c(username)=="Mayara"{
	global encrypted 		"Z:"
	global dictionaries		"M:/raisdictionaries/harmonized"
	global deIDrais			"M:/raisdeidentified"
	global monopsonies		"M:/projects/monopsonies"
	global public			"M:/publicdata"
}

* Change directory to encrypted output folder just in case something is saved
* to current directory
cd "${encrypted}/output"

* Output folder for rais .dta files
global importedrais			"${encrypted}/output/dta/importedrais"
global IDlinksrais			"${encrypted}/output/dta/IDlinksrais"

* All files
local files_dta: dir "${importedrais}" files "*.dta"

local updatedate		= 20191213

local testing 			= 0

local makefake_workers 	= 0
local makefake_estab 	= 1
local deid_workers		= 1

* Display date and time
di "Starting deidentify code on $S_DATE at $S_TIME"

****************************************************
******************* Make fake IDs ******************
****************************************************

********** Fake ids for workers **********

if `makefake_workers'==1{
	
	* Testing
	if `testing'==1{
		u "${importedrais}/AC2015ID.dta", clear
		append using "${importedrais}/AC2000ID.dta"
		append using "${importedrais}/AC1985ID.dta"
		append using "${importedrais}/AC1990ID.dta"
		append using "${importedrais}/AC1995ID.dta"
		
		local files_dta "AC2015ID AC2010ID AC2000ID AC1985ID AC1995ID"
	}
	else{
		u workerid_pis workerid_cpf using "${IDlinksrais}/rais_workerIDs.dta", clear
	}
	
	keep workerid_pis workerid_cpf
	`tools'duplicates drop
	
	* Obs with the same CPF and multiple PIS (eg same person with different PIS and PASEP #s, common problem)
	* Assign same mainPIS to that worker
	bys workerid_cpf: `tools'egen allpis = count(workerid_pis)
	tab allpis
	
	* CPF - PIS links: assign mainPIS
	preserve
		keep if !missing(workerid_cpf) & !missing(workerid_pis) & allpis>1
		sample 1, count by(workerid_cpf)
		ren workerid_pis mainpis
		keep workerid_cpf mainpis
		tempfile multipis
		sa `multipis'
	restore
	
	gen double mainpis = workerid_pis
	merge m:1 workerid_cpf using `multipis', update replace	nogen // Replace with unique mainpis

	misstable sum
	
	* PIS - mainPIS links : Make sure PIS in obs with no CPF has same mainPIS as in obs with CPF
	keep if !missing(mainpis) & !missing(workerid_pis)

	preserve
		sample 1, count by(workerid_pis)
		tempfile mainpis
		sa `mainpis'
	restore
	merge m:1 workerid_pis using `mainpis', update replace nogen // Replace with unique mainpis
	
	keep mainpis workerid_pis
	`tools'duplicates drop
	
	`tools'isid workerid_pis
	
	tempfile rawpis
	sa `rawpis'
	
	* Assign ID based on unique CPF for those that have any CPF and multiple PIS
	* That is, assign ID based on mainpis. Merge to data based on raw workerid_pis
	keep mainpis
	`tools'duplicates drop
	
	* Random sort order
	gen sortorder = runiform()
	hashsort sortorder		
	
	gen double fakeid_worker = _n
	format fakeid_worker %14.0f
	drop sortorder
	
	`tools'isid mainpis
	`tools'isid fakeid_worker	
	
	* Now get back raw workerid_pis
	merge 1:m mainpis using `rawpis', nogen
	keep workerid_pis mainpis fakeid_worker
	order  workerid_pis mainpis fakeid_worker
	
	`tools'isid workerid_pis
	
	list if _n<=10

	compress
	saveold "${IDlinksrais}/rais_workerid_fakeid_link_`updatedate'.dta", replace
}
********** Fake ids for firms **********

if `makefake_estab'==1{
	
	if `testing'==1{
	
		u "${importedrais}/AC2015ID.dta", clear
		append using "${importedrais}/AC2000ID.dta"
		append using "${importedrais}/AC1985ID.dta"
		append using "${importedrais}/AC1990ID.dta"
		append using "${importedrais}/AC1995ID.dta"
		
		local files_dta "AC2015ID AC2010ID AC2000ID AC1985ID AC1995ID"

	}
	else{
		u estabid* using "${IDlinksrais}/rais_estabIDs.dta", clear
	}
	
	keep estabid_cnpjcei estabid_type estabid_construction
	`tools'duplicates drop
		
	cap destring estabid_type, replace force
	format estabid_cnpjcei %14.0f
	format estabid_constructioncei %14.0f
	
	* Step 1: 	find the CNPJ of each CEI by matching estabid_cnpjcei
	* 			of estabid_type==CEIs to estabid_constructioncei of estabid_cnpjcei==CNPJs
	preserve
		keep if !missing(estabid_constructioncei)
		keep estabid_cnpjcei estabid_constructioncei
		
		ren estabid_cnpjcei 			estabid_constructioncnpj
		ren estabid_constructioncei		estabid_cnpjcei
		
		sample 1, count by(estabid_cnpjcei)

		tempfile ceis
		sa `ceis'
	restore
	
	* Main ID: Replace CEIs with CNPJ whenever the CEI is linked to a CNPJ
	merge m:1 estabid_cnpjcei using `ceis', keepusing(estabid_constructioncnpj) keep(1 3) nogen
	replace estabid_cnpjcei = estabid_constructioncnpj if !missing(estabid_constructioncnpj)
	replace estabid_cnpjcei = 1 if !missing(estabid_constructioncnpj)
	
	keep estabid_cnpjcei estabid_type
	`tools'duplicates drop
	
	tab estabid_type

	* Now take CNPJ raiz and branch # if CNPJ, else
	* treat as CEI or other unique ID
	tostring estabid_cnpjcei, gen(str)  format(%14.0f)
	assert regexm(str,"[a-zA-Z]")==0
	gen len = length(str)
	tab len
	
	* If estabID is not 1 or 3, assume it's CNPJ if 14 digits, CEI if 12
	replace estabid_type = 3 if !inlist(estabid_type,1,3) & len<=12
	replace estabid_type = 1 if !inlist(estabid_type,1,3) & len>12
	
	* If CEI and CNPJ are the same, assign CNPJ estabidtype
	gen cnpj = (estabid_type == 1)
	`tools'bys estabid_cnpjcei: egen hascnpj = max(cnpj)
	`tools'duplicates tag estabid_cnpjcei, gen(dup)
	tab dup
	drop if hascnpj== 1 & cnpj== 0
	
	* Unique by estabid_cnpjcei
	sample 1, count by(estabid_cnpjcei)
	
	* CNPJ: 14 digits
	replace str = "0000000000000"+str if len==1 & estabid_type==1
	replace str = "000000000000"+str if len==2 & estabid_type==1
	replace str = "00000000000"+str if len==3 & estabid_type==1
	replace str = "0000000000"+str if len==4 & estabid_type==1
	replace str = "000000000"+str if len==5 & estabid_type==1
	replace str = "00000000"+str if len==6 & estabid_type==1
	replace str = "0000000"+str if len==7 & estabid_type==1
	replace str = "000000"+str if len==8 & estabid_type==1
	replace str = "00000"+str if len==9 & estabid_type==1
	replace str = "0000"+str if len==10 & estabid_type==1
	replace str = "000"+str if len==11 & estabid_type==1
	replace str = "00"+str if len==12 & estabid_type==1
	replace str = "0"+str if len==13 & estabid_type==1
	
	* CEI: 12 digits
	replace str = "00000000000"+str if len==1 & estabid_type==3
	replace str = "0000000000"+str if len==2 & estabid_type==3
	replace str = "000000000"+str if len==3 & estabid_type==3
	replace str = "00000000"+str if len==4 & estabid_type==3
	replace str = "0000000"+str if len==5 & estabid_type==3
	replace str = "000000"+str if len==6 & estabid_type==3
	replace str = "00000"+str if len==7 & estabid_type==3
	replace str = "0000"+str if len==8 & estabid_type==3
	replace str = "000"+str if len==9 & estabid_type==3
	replace str = "00"+str if len==10 & estabid_type==3
	replace str = "0"+str if len==11 & estabid_type==3
	
	gen cnpj_raiz 		= substr(str,1,8) 		if estabid_type==1
	gen cnpj_branch		= substr(str,9,4)		if estabid_type==1
	
	gen mainid_firm = str
	
	replace mainid_firm = cnpj_raiz if cnpj_raiz!=""
	
	// Finally, the first 8 digit CNPJ of a firm might 
	// coincide with the full CEI of another firm whose CEIs have leading zeros. 
	// To avoid this issue when computing a fake ID at the mainid level,
	// create fakeid for the firm at the mainid_firm estabid_type level
	
	keep estabid_cnpjcei estabid_type mainid_firm cnpj_branch

	tempfile allbranches
	sa `allbranches'
	
	*** Generate Fake ID: De-identify first 8 digits of CNPJ or all digits of CEI
	keep mainid_firm estabid_type
	`tools'duplicates drop
	
	* Sort per some random number
	gen sortorder = runiform()
	hashsort sortorder		
	
	gen double fakeid_firm = _n
	format fakeid_firm %14.0f
	drop sortorder
	
	* Merge back all branches and create Fake IDs from them by adding branch # and ID type
	merge 1:m mainid_firm estabid_type using `allbranches', nogen
	
	tostring fakeid_firm, gen(fakeid_estab) format(%14.0f) 
	assert regexm(fakeid_estab,"[a-zA-Z]")==0
	gen lenf = length(fakeid_estab)
	tab lenf
	
	replace fakeid_estab = "0000000"+fakeid_estab if lenf==1
	replace fakeid_estab = "000000"+fakeid_estab if lenf==2
	replace fakeid_estab = "00000"+fakeid_estab if lenf==3
	replace fakeid_estab = "0000"+fakeid_estab if lenf==4
	replace fakeid_estab = "000"+fakeid_estab if lenf==5
	replace fakeid_estab = "00"+fakeid_estab if lenf==6
	replace fakeid_estab = "0"+fakeid_estab if lenf==7
	
	drop lenf
	
	* Include ID type as last digit
	replace fakeid_estab = fakeid_estab + cnpj_branch + "1" if estabid_type==1 
	replace fakeid_estab = fakeid_estab + "9999" + "3" if estabid_type==3
	
	* Now destring fakeid_estab and branch #
	destring fakeid_firm fakeid_estab, replace
	format fakeid_firm fakeid_estab %14.0f

	keep estabid_cnpjcei estabid_type fakeid_firm fakeid_estab
	
	order estabid_cnpjcei estabid_type fakeid_firm fakeid_estab
	
	* Confirm estabid_cnpjcei estabid_type is still unique 
	`tools'isid estabid_cnpjcei	
	
	* Multiple estabid_cnpjcei with same fakeid_estab can occur if
	* multiple CEIs are linked to same CNPJ (constructions of same firm)
	`tools'unique fakeid_estab
	
	`tools'sort fakeid_firm
	
	label var fakeid_estab 		"Fake firm ID + 4-digit branch if CNPJ + ID type"
	label var fakeid_firm  		"Fake ID based on CNPJ first 8 digits or CEI"
	label var estabid_cnpjcei  	"Establishment ID in raw data"
	label var estabid_type  	"1=CNPJ 3=CEI"
	
	list if _n<=10
	
	compress
	saveold "${IDlinksrais}/rais_estabid_fakeid_link_`updatedate'", replace
}

****************************************************
********** Create de-identified datasets ***********
****************************************************

if `deid_workers'==1{

	if `testing'==1{
		local files_dta "AC2015ID AC2010ID AC2000ID AC1985ID AC1995ID"
	}

	foreach f in `files_dta'{
		clear all
		local cleanname = subinstr("`f'",".dta","",.)
		
		u "${importedrais}/`f'", clear

		* Swap worker IDs with fake ones. Remove all ID vars.
		cap confirm variable workerid_pis
		if !_rc{
			format workerid_pis %14.0f
			merge m:1 workerid_pis using "${IDlinksrais}/rais_workerid_fakeid_link_`updatedate'.dta", ///
			keepusing(fakeid_worker) keep(3) nogen
		
			drop workerid*
			order fakeid*, first		
		}
		
		* Swap firm IDs with fake ones. Remove all ID vars.
		cap confirm variable estabid_cnpjcei
		if !_rc{
			format estabid_cnpjcei %14.0f
			merge m:1 estabid_cnpjcei using "${IDlinksrais}/rais_estabid_fakeid_link_`updatedate'.dta", ///
			keepusing(fakeid_firm fakeid_estab) keep(3) nogen
			
			drop estabid* 
			order fakeid*, first
		}
		
		************************************************************************
		* Additional cleaning to remove all strings, any identifiable data that
		* might have been saved in another column
		************************************************************************
		
		di in red "1. These vars are string in `f'"
		ds, v(32) has(type string)
		
		* Classes
		foreach x in cnaeclass95 cnaeclass20 cnaesubclass20{
			cap replace `x' = subinstr(`x',"SUB ","",.)
			cap replace `x' = subinstr(`x',"CLASSE ","",.)
			cap destring `x', force replace
			cap replace `x' = . if inlist(`x',0,-1)
		}
		
		* Separation dates
		foreach x in sepday sepmonth sepreason{
			cap replace `x'="" if `x'=="NAO DESL ANO"
			cap destring `x', replace force
		}
		
		foreach x in sepday sepmonth sepreason{
			cap confirm variable `x'
			if !_rc{
				replace `x'=. if `x'==0
			}
		}
		
		* If observation has one variable that is all
		* strings, that variable could be a name, so data might be misplaced/
		* innacurate. Drop the obs
		di in red "2. These vars are still string in `f'"
		ds, v(32) has(type string)
		if "`r(varlist)'"!=""{
			foreach var of varlist `r(varlist)'{
				tab `var' if (regexm(`var',"a-zA-Z")!=0 & regexm(`var',"0-9")==0)
				drop if (regexm(`var',"a-zA-Z")!=0 & regexm(`var',"0-9")==0)
			}
		}
		
		* Economic variable
		cap destring ibgesubsector, force replace
		cap destring ibgesubactivity, force replace
		
		* Location
		cap destring municipality, force replace
		cap destring estabzip, force replace
		cap replace municipality 	= . if inlist(municipality,0,-1)
		cap replace estabzip 		= . if inlist(estabzip,0,-1)
		
		di in red "3. These vars are still string in `f'"
		ds, v(32) has(type string)
		if "`r(varlist)'"!=""{
			foreach var of varlist `r(varlist)'{
				tab `var' if _n<10
				cap destring `var', replace force
			}
		}
		
		* Worker variables
		foreach x in disability educ gender cbo02 cbo94{
			cap replace `x' = . if inlist(`x',-1,0)
		}
		
		cap mkdir "${deIDrais}/dta/`updatedate'"
		local cleanname = subinstr("`cleanname'","id","ID",.)
		compress
		saveold "${deIDrais}/dta/`updatedate'/deID_`cleanname'", replace
	}
	
}

* Display date and time
di "Ending deidentify code on $S_DATE at $S_TIME"
