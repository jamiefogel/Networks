/*
	Input: mmc-level files from SAS
*/
version 14.2
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


local outdate	= 20210802

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"

local informality_analysis 	= 1

if `informality_analysis'==1{
	
	/* 1991 */
	u "${public}/other/DK (2017)/ReplicationFiles/Data_Census/cen91.dta", clear
	keep if inrange(age,18,64) & employed==1
	
	merge m:1 atividade using  "${public}/other/DK (2017)/ReplicationFiles/Data_Census/Auxiliary_files/atividade_to_indlinkn.dta", keep(3) nogen
	
	keep if employed==1  // employed
	drop if indlinkn==98 // drop public admin
	keep if indlinkn < . // valid industry code
	
	// exclude alternative employment (i.e. self-employment, no answer to employment
	drop if prev_anonformemp==1
	
	// Keep if has positive earnings
	keep if ymain>0 & !missing(ymain)
	
	// Merge in mmc codes
	merge m:1 munic using "${public}/other/DK (2017)/ReplicationFiles/Data_Census/Auxiliary_files/census_1991_munic_to_mmc_1991_2010.dta", keep(3) nogen
	
	replace prev_formem = prev_formem*xweighti
	replace prev_nonformemp = prev_nonformemp*xweighti
	
	collapse (sum) prev_formem prev_nonformemp, by(mmc)
	gen double share_informal = prev_nonformemp/(prev_nonformemp + prev_formem)
	keep if !missing(share_informal)
	
	sum share_informal, detail
	gen double year = 1991
	tempfile 1991
	sa `1991'
	
	/* 2000 */
	u "${public}/other/DK (2017)/ReplicationFiles/Data_Census/cen00.dta", clear
	keep if inrange(age,18,64) & employed==1
	ren cnae cnaedom
	merge m:1 cnae using  "${public}/other/DK (2017)/ReplicationFiles/Data_Census/Auxiliary_files/cnaedom_to_indlinkn.dta", keep(3) nogen
	
	keep if employed==1  // employed
	drop if indlinkn==98 // drop public admin
	keep if indlinkn < . // valid industry code
	
	// exclude alternative employment (i.e. self-employment, no answer to employment
	drop if anonformemp==1
	
	// Keep if has positive earnings
	keep if ymain>0 & !missing(ymain)
	
	// Merge in mmc codes
	merge m:1 munic using "${public}/other/DK (2017)/ReplicationFiles/Data_Census/Auxiliary_files/census_2000_munic_to_mmc_1991_2010.dta", keep(3) nogen
	
	replace formem = formem*xweighti
	replace nonformemp = nonformemp*xweighti
	
	collapse (sum) formem nonformemp, by(mmc)
	gen double share_informal = nonformemp/(nonformemp + formem)
	
	sum share_informal, detail
	gen double year = 2000
	keep if !missing(share_informal)
	tempfile 2000
	sa `2000'
	
	u "${monopsonies}/sas/regsfile_mmc_none.dta", clear
	keep if inlist(year,1991,2000)
	keep mmc year hf_wdbill hf_emp mkt_emp
	merge 1:1 year mmc using `1991', keep(1 3) nogen
	merge 1:1 year mmc using `2000', keep(1 3 4 5) update nogen
	

	twoway  (scatter hf_wdbill share_informal /* [w=mkt_emp] */ if year==1991, color(blue%25)) ///
			(scatter hf_wdbill share_informal /* [w=mkt_emp] */ if year==2000, color(red%25)) ///
			(lfit hf_wdbill share_informal /* [w=mkt_emp] */ if year==1991, color(blue)) ///
			(lfit hf_wdbill share_informal /* [w=mkt_emp] */ if year==2000, color(red)), ///
			scheme(s1color) ytitle("Formal sector wagebill Herfindahl") ///
			xtitle("Informality share of total employment") legend(label(1 "1991") label(2 "2000") order(1 2) rows(1))
	graph export "${monopsonies}/eps/`outdate'/hf_wagebill_informality_1991_2000.pdf", replace
	
	twoway  (scatter hf_emp share_informal /* [w=mkt_emp] */ if year==1991, color(blue%25)) ///
			(scatter hf_emp share_informal /* [w=mkt_emp] */ if year==2000, color(red%25)) ///
			(lfit hf_emp share_informal /* [w=mkt_emp] */ if year==1991, color(blue)) ///
			(lfit hf_emp share_informal /* [w=mkt_emp] */ if year==2000, color(red)), ///
			scheme(s1color) ytitle("Formal sector employment Herfindahl") ///
			xtitle("Informality share of total employment") legend(label(1 "1991") label(2 "2000") order(1 2) rows(1))
	graph export "${monopsonies}/eps/`outdate'/hf_employment_informality_1991_2000.pdf", replace

}

