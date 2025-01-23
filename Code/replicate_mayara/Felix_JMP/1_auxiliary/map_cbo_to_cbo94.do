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

* 1993 - Last year with CBO codes
import sas fakeid_worker fakeid_firm cbo agegroup municipality emp1231 earningsdecmw ibgesubsector  using "${monopsonies}/sas/rais1993.sas7bdat", case(lower) clear
keep if emp1231==1
keep if earningsdecmw>0 & !missing(earningsdecmw)
keep if ibgesubsector!=24 & ibgesubsector!=.
keep if inrange(agegroup,3,7)
keep if !missing(municipality)

unique cbo

tempfile w1993
sa `w1993'

* 1994 - Introduction of CBO94 codes
import sas fakeid_worker fakeid_firm cbo94 agegroup municipality emp1231 earningsdecmw ibgesubsector  using "${monopsonies}/sas/rais1994.sas7bdat", case(lower) clear
keep if emp1231==1
keep if earningsdecmw>0 & !missing(earningsdecmw)
keep if ibgesubsector!=24 & ibgesubsector!=.
keep if inrange(agegroup,3,7)
keep if !missing(municipality)

unique cbo94

keep fakeid_worker fakeid_firm cbo94
merge 1:1 fakeid_worker fakeid_firm using `w1993', keep(3) keepusing(cbo)

collapse (count) workers=fakeid_worker, by(cbo cbo94)
gsort  cbo94 -workers
by cbo94: gen top_cbo94_to_cbo = _n
gsort  cbo -workers
by cbo: gen top_cbo_to_cbo94 = _n

saveold "${dictionaries}/crosswalk_cbo_to_cbo94.dta", replace
