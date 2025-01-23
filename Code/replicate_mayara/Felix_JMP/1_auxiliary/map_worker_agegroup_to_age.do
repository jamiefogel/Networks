/*
	Mayara Felix
	
	Use 1994 datasets (which have agegroup and age) across all states
	to get the average age for an age group. Use that to assign
	age to workers in pre-1994 data that only had age group
	and completely left dataset
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
		
/*	Birthdate and age availability
	agegroup only: 		1985-1993
	age & agegroup:		1994-2001
	age:				1994-2001, 2011-2016
	birthdate:			2002-2010, 2014-2016
*/

* State sepcific files
		
foreach state in    AC    	AL 		AM   	AP      BA   CE   DF ///
					ES      GO      MA      MG      MS   MT   PA ///
					PB      PE      PI      PR      RJ   RN   RO ///
					RR      RS      SC      SE   	SP  TO {
		
			u "${deIDrais}/dta/20191213/deID_`state'1994ID.dta", clear //XX Fixed file path to include 20191213
			keep fakeid_worker agegroup age
			keep if !missing(agegroup) & !missing(age)
			keep if inrange(age,18,85)
			
			tempfile f`state'1994
			sa `f`state'1994'
}

* Append all and get mean age by agegroup
u `fAC1994', clear

foreach state in    AL 		AM   	AP      BA   CE   DF ///
					ES      GO      MA      MG      MS   MT   PA ///
					PB      PE      PI      PR      RJ   RN   RO ///
					RR      RS      SC      SE   	SP  TO {
		append using `f`state'1994'
}

sample 1, count by(fakeid_worker)
collapse (mean) age (count) workers=fakeid_worker, by(agegroup)

replace age = round(age)

table agegroup, contents(mean age mean workers)

saveold "${dictionaries}/agegroup_to_age.dta", replace
