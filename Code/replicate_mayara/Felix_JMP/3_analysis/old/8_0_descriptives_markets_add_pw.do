version 14.2
clear all
set more off
set matsize 11000
*ssc install unicode2ascii
*ssc install spmap
unicode encoding set "ISO-8859-9"
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

local etaweights	"w0 all"
local thetaweight 	= "all"
local firms			= 1

local premiadate	= 20210802
local outdate		= 20210802
local etadate		= 20210802
local thetadate		= 20210802

cap mkdir "${monopsonies}/eps/`graphsdate'"
cap mkdir "${monopsonies}/csv/`graphsdate'"

if `firms'==1{

	************************
	*** Get eta estimate ***
	************************
	foreach etaweight in `etaweights'{
		insheet using "${monopsonies}/csv/`etadate'/eta_change_regressions.csv", clear
		keep if samp=="all" & model=="l" & wagevar=="lndp" & year==1997 & clust=="firm" & spec=="l" & tariff=="lnT" & weight=="`etaweight'"
		levelsof iv_b, local(eta_inverse_`etaweight')

		**************************
		*** Get theta estimate ***
		**************************
		insheet using "${monopsonies}/csv/`thetadate'/theta_change_regressions_simpler.csv", clear
		keep if esamp=="all" & tsamp=="all" & wagevar=="lndp" & year==1997 & thetaclust=="fe_ro" & deltatype == "delta_ro" & weight=="`thetaweight'" & chng_lrotype=="chng_Lro" & etaweight=="`etaweight'"
		levelsof theta_inverse_b, local(theta_inverse_`etaweight')
	}	
	
	 u "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", clear
	 compress
	 saveold  "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", replace
	 
		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
		drop if mmc_drop==1
		drop mmc_drop
		cap drop if inlist(cbo942d,31,22,37)
		
		* Wage premia
		merge m:1 mmc cbo942d year using "${monopsonies}/sas/earnings_premia_Herfindhal_mmc_cbo942d.dta", keep(1 3) nogen
		
		merge m:1 mmc cbo942d year using "${monopsonies}/sas/rais_lnearn_premia_mmc_cbo942d_`premiadate'.dta", keep(1 3) keepusing(dprem_ro) nogen
		
		preserve
			u "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_`premiadate'.dta", clear
			gen double firmprem= exp(dprem_zro)
			merge 1:1 fakeid_firm mmc cbo942d year using "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", keepusing(emp) keep(3) nogen
			
			collapse (mean) avg_exp_dprem_zro=firmprem [w=emp], by(mmc cbo942d year)
			tempfile premf
			sa `premf'
		restore
		
		merge 1:1 mmc cbo942d year using `premf', keep(1 3) nogen
		
		gen double exp_dprem_ro = exp(dprem_ro)
		
		* Market average take-home share
		foreach etaweight in `etaweights'{
			gen double mkt_avgmu_eta`etaweight' = 1/(1+ `theta_inverse_`etaweight''*hf_pdbill + (1-hf_pdbill)*`eta_inverse_`etaweight'')
		}
		
	unique mmc cbo942d
	 
	 * Balanced panel
	 bys mmc cbo942d: egen years = count(year)
	 egen maxyears = max(years)
	 keep if years==maxyears
	 
	 unique mmc cbo942d
	 drop years maxyears
	 
	 foreach var of varlist ice*{
		replace `var' = 0 if missing(`var')
		replace `var' = -`var'
	 }
	 
	 tempfile mkts
	 sa `mkts'
	 
	foreach stat in mean p10 p25 p50 p75 p90 {
		 u `mkts', clear
		 
		 ds mmc cbo942d year, not
		 collapse (`stat') `r(varlist)', by(year)
		 
		 renvars _all, pref(`stat'_)
		 ren `stat'_year year

		 reshape long `stat'_, i(year) j(var) string
		 
		 ren `stat'_ `stat'
		 
		 gen weight = 0
		  order weight
		 
		 tempfile f`stat'
		 sa `f`stat''
	}
	
	* Employment-weighted
	foreach stat in mean{
		 u `mkts', clear
		 
		 bys year: egen double tot = sum(mkt_emp)
		 gen double w1 = mkt_emp/tot
		 drop tot
		 
		 ds mmc cbo942d year, not
		 
		 collapse (`stat') `r(varlist)' [w=w1], by(year)
		 drop w1
		 
		 renvars _all, pref(`stat'_)
		 ren `stat'_year year

		 reshape long `stat'_, i(year) j(var) string
		 
		 ren `stat'_ `stat'
		 
		 gen weight = 1
		 order weight
		 
		 tempfile w1`stat'
		 sa `w1`stat''
	}
	
	* Payroll-weighted
	foreach stat in mean {
		 u `mkts', clear
		 
		 bys year: egen double tot = sum(mkt_wdbill)
		 gen double w2 = mkt_wdbill/tot
		 drop tot
		 
		 ds mmc cbo942d year, not
		 
		 collapse (`stat') `r(varlist)' [w=w2], by(year)
		 drop w2
		 
		 renvars _all, pref(`stat'_)
		 ren `stat'_year year

		 reshape long `stat'_, i(year) j(var) string
		 
		 ren `stat'_ `stat'
		 
		 gen weight = 2
		 order weight
		 
		 tempfile w2`stat'
		 sa `w2`stat''
	}
 
 u `fmean', clear
 foreach stat in p10 p25 p50 p75 p90{
	merge 1:1 var year weight using `f`stat'', keep(3) nogen
 }
 
 append using `w1mean'
 append using `w2mean'
 
 outsheet using  "${monopsonies}/csv/`outdate'/market_descriptives_firms_add_pw.csv", comma replace
 
}
