/*
	Distribution of top 10 firm shares across LLMs
	Distribution of labor shares by cnae nationally
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


local outdate		= 20210802

local baseyear 	= 1991
local baseyear_o1 	= `baseyear'+3
local baseyear_o2 	= `baseyear'+6

local baseyear_n = 91

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"

*local intvars "bTwshare bTeshare bwshare beshare explib ice_erp"

local sectshares 	= 1
local topshares 	= 1
local usesample 	= 0		/* Use 10% random sample of firms (for de-bugging) */


* Compute national shares and compare against market shares
if `sectshares'==1{
	
	* Share in national industry - include only traadbles
	u "${monopsonies}/sas/firm_DD_regsfile`usesample'.dta", clear
	keep if year==`baseyear'
	keep if T==1
	
	collapse (sum) emp, by(fakeid_firm cnae95)
	`tool'egen double tot = sum(emp), by(cnae95)
	gen double sshare = emp/tot
	keep sshare fakeid_firm
	
	sum sshare, detail
	duplicates drop
	tempfile secshares
	sa `secshares'
	
	* Share in LLM - include all firms for denominator
	u "${monopsonies}/sas/firm_DD_regsfile`usesample'.dta", clear
	
	merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
	drop if mmc_drop==1
	
	cap drop if inlist(cbo942d,31,22,37)
		
	keep if year==`baseyear'
	collapse (sum) emp, by(fakeid_firm mmc cbo942d)
	
	`tool'egen double tot = sum(emp), by(mmc cbo942d)
	gen double mshare = emp/tot
	
	collapse (min) minshare = mshare (max) maxshare=mshare, by(fakeid_firm)
	sum minshare maxshare , detail
	merge 1:1 fakeid_firm using `secshares', keep(3) nogen
	
	twoway (hist sshare, color(red%25)) (hist minshare, color(blue%25)), ///
	xtitle("Firm share") scheme(s1mono) ///
	legend(label(1 "National employment share in industry") label(2 "Smallest local labor market employment share") cols(1))
	graph export "${monopsonies}/eps/`outdate'/hist_cnae95_vs_llm_shares_smallest.pdf", replace	
	
	twoway (hist sshare, color(red%25)) (hist maxshare, color(blue%25)), ///
	xtitle("Firm share") scheme(s1mono) ///
	legend(label(1 "National employment share in industry") label(2 "Largest local labor market employment share") cols(1))
	graph export "${monopsonies}/eps/`outdate'/hist_cnae95_vs_llm_shares_largest.pdf", replace	
}

*** Compute distributions of top 10 firm shares conditional on markets above
*** a certain number of employees ****

if `topshares'==1{
	
foreach mktsize in 1 10 100{
	
	if `mktsize'==1{
		local workers "worker"
	}
	else{
		local workers "workers"
	}
	
	u "${monopsonies}/sas/firm_DD_regsfile`usesample'.dta", clear
	
	merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
	drop if mmc_drop==1
	
	cap drop if inlist(cbo942d,31,22,37)
	
	keep if year==`baseyear'
	keep if emp>=`mktsize'
	
	unique mmc cbo942d
	local mkts = `r(unique)'
	di "Number of markets is `mkts'"
	
	keep fakeid_firm mmc cbo942d eshare emp
	
	* Number of firms per LLM conditional on LLM size
	preserve
		collapse (count) firms = fakeid_firm, by(mmc cbo942d)
		
		gen double lnfirms = ln(firms)
		
		sum firms, detail
		local firms = `r(p50)'
		
		sum lnfirms, detail
		local mediant = round(`r(p50)',0.1)
		local place   = `r(p50)'+0.1

		twoway histogram lnfirms, percent color(blue%25) ///
		xtitle("Log(Number of firms in LLM)") scheme(s1mono) ///
		xline(`r(p50)', lcolor(black) lpattern(dash)) ///
		text(10 `place' "Median LLM has" "`firms' firms", placement(ne)) ///
		note("Note: Distribution across `mkts' LLMs with at least `mktsize' `workers'.")
		graph export "${monopsonies}/eps/`outdate'/kdensity_nfirms_mmc_cbo942d_all_mktsgt`mktsize'.pdf", replace	
	restore
	
	sum eshare, detail
	local mediant = round(100*`r(p50)', 0.1)
	local place   = `r(p50)'+0.1
	twoway histogram eshare, percent color(blue%25) ///
	xtitle("Firm eployment share in LLM") scheme(s1mono) ///
	xline(`r(p50)', lcolor(black) lpattern(dash)) ///
	text(25 `place' "Median firm employs" "`mediant' percent of market", placement(ne)) ///
	note("Note: Distribution across `mkts' LLMs with at least `mktsize' `workers'.")
	graph export "${monopsonies}/eps/`outdate'/kdensity_eshare_mmc_cbo942d_all_mktsgt`mktsize'.pdf", replace
	
	* Top 10, top 5 employers
	
	gsort mmc cbo942d -eshare
	bys mmc cbo942d: gen rank = _n
	
	* What share of all employment do the top firms account for?
	if `mktsize'<=50{
	preserve
		gen double top10emp 	= (rank<=10)*emp
		gen double top5emp 		= (rank<=5)*emp
		
		collapse (sum) top* emp
		
		gen double top5share 	= top5emp /emp
		gen double top10share 	= top10 / emp
		
		sum top5share
		di "Top 5 firms have `r(mean)' of all employment among LLMs with at least `mktsize' `workers'"
		
		sum top10share
		di "Top 10 firms have `r(mean)' of all employment among LLMs with at least `mktsize' `workers'"
	restore
	}
	gen double top10share 	= (rank<=10)*eshare
	gen double top5share 	= (rank<=5)*eshare
	gen double top2share 	= (rank<=2)*eshare
	
	collapse (sum) top*, by(mmc cbo942d)
	
	sum top10, detail
	local mediant = round(100*`r(p50)', 1)
	local place   = `r(p50)'+0.1
	twoway histogram top10share, percent color(blue%25) ///
	xtitle("Firm eployment share in LLM") scheme(s1mono) ///
	xline(`r(p50)', lcolor(black) lpattern(dash)) ///
	text(10 `place' "Median LLM has" "`mediant'% of employment" "at Top 10 firms", placement(ne)) ///
	note("Note: Distribution across `mkts' LLMs with at least `mktsize' `workers'.")
	graph export "${monopsonies}/eps/`outdate'/kdensity_eshare_mmc_cbo942d_top10_mktsgt`mktsize'.pdf", replace
	
	sum top5, detail
	local mediant = round(100*`r(p50)', 1)
	local place   = `r(p50)'+0.01
	twoway histogram top5share, percent color(blue%25) ///
	xtitle("Firm eployment share in LLM") scheme(s1mono) ///
	xline(`r(p50)', lcolor(black) lpattern(dash)) ///
	text(10 `place' "Median LLM has" "`mediant'% of employment" "at Top 5 firms", placement(ne)) ///
	note("Note: Distribution across `mkts' LLMs with at least `mktsize' `workers'.")
	graph export "${monopsonies}/eps/`outdate'/kdensity_eshare_mmc_cbo942d_top5_mktsgt`mktsize'.pdf", replace
	
	sum top2, detail
	local place   = `r(p50)'+0.01
	local mediant = round(100*`r(p50)', 1)
	twoway histogram top2share, percent color(blue%25) ///
	xtitle("Firm eployment share in LLM") scheme(s1mono) ///
	xline(`r(p50)', lcolor(black) lpattern(dash)) ///
	text(10 `place' "Median LLM has" "`mediant'% of employment" "at Top 2 firms", placement(ne)) ///
	note("Note: Distribution across `mkts' LLMs with at least `mktsize' `workers'.")
	graph export "${monopsonies}/eps/`outdate'/kdensity_eshare_mmc_cbo942d_top2_mktsgt`mktsize'.pdf", replace
	
}
	
}
