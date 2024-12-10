/*
	Input: 	updated market-level files from SAS
			same specifications as 20200219 file
			
	20200513
		- Fix sign of long differences
		
		- Don't run it yet until have some prediction re # of exporters in the MMC/occupation
		and what that will do
		
		- Consider looking at data on tradables only
	
	20200528
		- IMPORTANT: keeping balanced sample of mmc-cbo942d
		is actually a binding sample selection criterion
		if requiring that pair exist for 1985-2000 especially
		because occupation codes change in 2002.
		
		So this analysis is best focused on years 1985-2000
		Longer-term effects can be provided for mmc level
		
									  totyears
	-------------------------------------------------------------
		  Percentiles      Smallest
	 1%            6              1
	 5%           15              1
	10%           18              1       Obs             958,803
	25%           28              1       Sum of Wgt.     958,803

	50%           31                      Mean           27.90642
							Largest       Std. Dev.      5.930185
	75%           31             31
	90%           31             31       Variance        35.1671
	95%           31             31       Skewness      -2.041767
	99%           31             31       Kurtosis       6.459692
	
	
	Maybe should use rais_codemun_to_mmc_1991_2010.dta to get mmc mapping
	487 mmcs
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



u "${public}/other/DK (2017)/ReplicationFiles/Data_Other/rais_codemun_to_mmc_1991_2010.dta", clear
destring codemun mmc, replace 
keep if !missing(mmc)
saveold "${dictionaries}/rais_codemun_to_mmc_1991_2010.dta", replace

