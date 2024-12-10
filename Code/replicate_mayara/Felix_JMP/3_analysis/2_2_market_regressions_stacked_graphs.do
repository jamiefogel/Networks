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


global minyear = 1986
global maxyear = 2000
local minyear = ${minyear}
local maxyear = ${maxyear}

local baseyear = 1991
local regsdate		= 20210802		
local graphsdate	= 20210802

local graphweights "0 1"

*local shocks "iceW iceWErp iceE iceH icebW"
local shocks "iceH iceE"

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/eps/`graphsdate'"

local hf_wdbill 		"Change in payroll Herfindahl relative to `baseyear'"
local hf_pdbill 		"Change in payroll Herfindahl relative to `baseyear'"
local hf_emp			"Change in employment Herfindahl relative to `baseyear'"

local lnhf_wdbill 		"Change in log payroll Herfindahl relative to `baseyear'"
local lnhf_pdbill 		"Change in log payroll Herfindahl relative to `baseyear'"

local lnmkt_emp			"Change in log employment relative to `baseyear'"

local dpremos_r 		"Change in wage premium relative to `baseyear'"
local dprems_r 			"Change in wage premium relative to `baseyear'"
local davgw_r 			"Change in average wage relative to `baseyear'"
local dprem_r 			"Change in wage premium relative to `baseyear'"
local davgw_ro 			"Change in average wage relative to `baseyear'"
local dprems_ro 		"Change in wage premium relative to `baseyear'"
local dprem_ro 			"Change in wage premium relative to `baseyear'"

local baseyear = 1991
/*
*************** Log employment by firm size based on 1991 percentiles ***************
foreach ice in `shocks'{
foreach weight in `graphweights'{
	insheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_residual_mktregs_cbo942d.csv", clear
	keep if ice=="`ice'" & weight==`weight' & clust=="llm"
	
	replace outcome = subinstr(outcome,"D","",.)
	reshape long y, i(outcome stat n) j(year)
	keep if (stat=="b" | stat=="ll" | stat=="ul")

	keep if (   outcome=="lntop25_1991_emp" | ///
				outcome=="lnmid5075_1991_emp" | ///
				outcome=="lnmid2550_1991_emp" | ///
				outcome=="lnbot25_1991_emp" )

	reshape wide y, i(outcome year n) j(stat) string

	ren yb  b_
	ren yll lb_
	ren yul ub_

	reshape wide b_ lb_ ub_, i(n year) j(outcome) string

	expand 2 in 1
	replace year=1991 if _n==15
	foreach x of varlist b_* lb_* ub_*{
		replace `x' = 0 if year==1991
	}
		
	sort year
	 
	twoway 	(rarea lb_lntop25_1991_emp ub_lntop25_1991_emp year, sort color(blue%25) lwidth(none)) ///
			(connect b_lntop25_1991_emp year, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
			(rarea lb_lnmid5075_1991_emp ub_lnmid5075_1991_emp year, sort color(blue%10) lwidth(none)) ///
			(connect b_lnmid5075_1991_emp year, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
			(rarea lb_lnmid2550_1991_emp ub_lnmid2550_1991_emp year, sort color(red%25) lwidth(none)) ///
			(connect b_lnmid2550_1991_emp year, lpattern(solid) msymbol(O) msize(small) color(red)) ///
			(rarea lb_lnbot25_1991_emp ub_lnbot25_1991_emp year, sort color(red%10) lwidth(none)) ///
			(connect b_lnbot25_1991_emp year, lpattern(solid) msymbol(O) msize(small) color(red)), ///
			scheme(s1mono)  legend(	label(2 "Above 75th") ///
									label(4 "50th - 75th") ///
									label(6 "25th - 50th") ///
									label(8 "Below 25th") ///
							order(2 4 6 8) rows(1) size(small)) ///
			xtitle("") ///
			xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
			xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
			ytitle("`lnmkt_emp'", size(small)) yscale(titlegap(*5))

			graph export "${monopsonies}/eps/`graphsdate'/DD_mmc_cbo942d_stacked_size_pctiles_1986_2000_`ice'_w`weight'.pdf", replace
}
}
************** Log employment by firm size based on 1991 levels ***************
foreach ice in `shocks'{
foreach weight in `graphweights'{
	insheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_residual_mktregs_cbo942d.csv", clear
	keep if ice=="`ice'" & weight==`weight' & clust=="llm"
	replace outcome = subinstr(outcome,"D","",.)
	reshape long y, i(outcome stat n) j(year)
	keep if (stat=="b" | stat=="ll" | stat=="ul")

	keep if (outcome=="lngt1000_1991_emp" | outcome=="lnlt1000_1991_emp" )

	reshape wide y, i(outcome year n) j(stat) string

	ren yb  b_
	ren yll lb_
	ren yul ub_

	reshape wide b_ lb_ ub_, i(n year) j(outcome) string

	expand 2 in 1
	replace year=1991 if _n==15
	foreach x of varlist b_* lb_* ub_*{
		replace `x' = 0 if year==1991
	}
		
	sort year
	 
	twoway 	(rarea lb_lngt1000_1991_emp ub_lngt1000_1991_emp year, sort color(blue%25) lwidth(none)) ///
			(connect b_lngt1000_1991_emp year, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
			(rarea lb_lnlt1000_1991_emp ub_lnlt1000_1991_emp year, sort color(red%25) lwidth(none)) ///
			(connect b_lnlt1000_1991_emp year, lpattern(solid) msymbol(O) msize(small) color(red)), ///
			scheme(s1mono)  legend(	label(2 "More than 1,000 employees in 1991") ///
									label(4 "Less than 1,000 employees in 1991") ///
									order(2 4) rows(2) size(small)) ///
			xtitle("") ///
			xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
			xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
			ytitle("`lnmkt_emp'", size(small)) yscale(titlegap(*5))

			graph export "${monopsonies}/eps/`graphsdate'/DD_mmc_cbo942d_stacked_size_levels_1986_2000_`ice'_w`weight'.pdf", replace
}
}
*/
*************** Exporters vs Non-exporting tradables vs others (Log employment) ***************
foreach ice in `shocks'{
foreach weight in `graphweights'{
	insheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_mktregs_cbo942d.csv", clear
	keep if ice=="`ice'" & weight==`weight' & clust=="llm"
	replace outcome = subinstr(outcome,"D","",.)
	reshape long y, i(outcome stat n) j(year)
	keep if (stat=="b" | stat=="ll" | stat=="ul")

	keep if (regexm(outcome,"lnmkt_nexplibTemp") | outcome=="lnexplib_emp" | outcome=="lnmkt_ntemp") 

	reshape wide y, i(outcome year n) j(stat) string

	ren yb  b_
	ren yll lb_
	ren yul ub_

	reshape wide b_ lb_ ub_, i(n year) j(outcome) string

	expand 2 in 1
	replace year=1991 if _n==15
	foreach x of varlist b_* lb_* ub_*{
		replace `x' = 0 if year==1991
	}
		
	sort year
	 
	twoway 	(rarea lb_lnexplib_emp ub_lnexplib_emp year, sort color(blue%25) lwidth(none)) ///
			(connect b_lnexplib_emp year, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
			(rarea lb_lnmkt_nexplibTemp ub_lnmkt_nexplibTemp year, sort color(red%25) lwidth(none)) ///
			(connect b_lnmkt_nexplibTemp year, lpattern(solid) msymbol(O) msize(small) color(red)) ///
			(rarea lb_lnmkt_ntemp ub_lnmkt_ntemp year, sort color(gs8%25) lwidth(none)) ///
			(connect b_lnmkt_ntemp year, lpattern(solid) msymbol(O) msize(small) color(gs8)), ///
			scheme(s1mono)  legend(label(2 "Exporters") label(4 "Non-exporting tradables") label(6 "Non-tradables") order(2 4 6) rows(1) size(small)) ///
			xtitle("") ///
			xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
			xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
			ytitle("`lnmkt_emp'", size(small)) yscale(titlegap(*5))

			graph export "${monopsonies}/eps/`graphsdate'/DD_mmc_cbo942d_stacked_3groups_lnemp_1986_2000_`ice'_w`weight'.pdf", replace

**** Just 2 groups ***
twoway  (rarea lb_lnexplib_emp ub_lnexplib_emp year, sort color(blue%25) lwidth(none)) ///
		(connect b_lnexplib_emp year, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
		(rarea lb_lnmkt_nexplibTemp ub_lnmkt_nexplibTemp year, sort color(red%25) lwidth(none)) ///
		(connect b_lnmkt_nexplibTemp year, lpattern(solid) msymbol(O) msize(small) color(red)), ///
		scheme(s1mono)  legend(label(2 "Exporters") label(4 "Non-exporting tradables") order(2 4 ) rows(1) size(small)) ///
		xtitle("") ///
		xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
		xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
		ytitle("`lnmkt_emp'", size(small)) yscale(titlegap(*5))

		graph export "${monopsonies}/eps/`graphsdate'/DD_mmc_cbo942d_stacked_2groups_lnemp_1986_2000_`ice'_w`weight'.pdf", replace
}
}
*************** Exporters vs Non-exporting tradables vs others (Log employment) RESIDUAL ***************
foreach ice in `shocks'{
foreach weight in `graphweights'{
	insheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_residual_mktregs_cbo942d.csv", clear
	keep if ice=="`ice'" & weight==`weight' & clust=="llm"
	replace outcome = subinstr(outcome,"D","",.)
	reshape long y, i(outcome stat n) j(year)
	keep if (stat=="b" | stat=="ll" | stat=="ul")

	keep if (regexm(outcome,"lnmkt_nexplibTemp") | outcome=="lnexplib_emp" | outcome=="lnmkt_ntemp") 

	reshape wide y, i(outcome year n) j(stat) string

	ren yb  b_
	ren yll lb_
	ren yul ub_

	reshape wide b_ lb_ ub_, i(n year) j(outcome) string

	expand 2 in 1
	replace year=1991 if _n==15
	foreach x of varlist b_* lb_* ub_*{
		replace `x' = 0 if year==1991
	}
		
	sort year
	 
	twoway 	(rarea lb_lnexplib_emp ub_lnexplib_emp year, sort color(blue%25) lwidth(none)) ///
			(connect b_lnexplib_emp year, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
			(rarea lb_lnmkt_nexplibTemp ub_lnmkt_nexplibTemp year, sort color(red%25) lwidth(none)) ///
			(connect b_lnmkt_nexplibTemp year, lpattern(solid) msymbol(O) msize(small) color(red)) ///
			(rarea lb_lnmkt_ntemp ub_lnmkt_ntemp year, sort color(gs8%25) lwidth(none)) ///
			(connect b_lnmkt_ntemp year, lpattern(solid) msymbol(O) msize(small) color(gs8)), ///
			scheme(s1mono)  legend(label(2 "Exporters") label(4 "Non-exporting tradables") label(6 "Non-tradables") order(2 4 6) rows(1) size(small)) ///
			xtitle("") ///
			xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
			xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
			ytitle("`lnmkt_emp'", size(small)) yscale(titlegap(*5))

			graph export "${monopsonies}/eps/`graphsdate'/DD_mmc_cbo942d_stacked_resid_3groups_lnemp_1986_2000_`ice'_w`weight'.pdf", replace

	**** Just 2 groups *****
	twoway  (rarea lb_lnexplib_emp ub_lnexplib_emp year, sort color(blue%25) lwidth(none)) ///
			(connect b_lnexplib_emp year, lpattern(solid) msymbol(O) msize(small) color(blue)) ///
			(rarea lb_lnmkt_nexplibTemp ub_lnmkt_nexplibTemp year, sort color(red%25) lwidth(none)) ///
			(connect b_lnmkt_nexplibTemp year, lpattern(solid) msymbol(O) msize(small) color(red)), ///
			scheme(s1mono)  legend(label(2 "Exporters") label(4 "Non-exporting tradables") order(2 4 ) rows(1) size(small)) ///
			xtitle("") ///
			xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
			xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
			ytitle("`lnmkt_emp'", size(small)) yscale(titlegap(*5))

			graph export "${monopsonies}/eps/`graphsdate'/DD_mmc_cbo942d_stacked_resid_2groups_lnemp_1986_2000_`ice'_w`weight'.pdf", replace
}
}


**** Stand alone graphs *****
local graphvars "lnmkt_emp  davgw_ro dprem_ro hf_pdbill hf_emp hf_wdbill"

foreach outc in `graphvars'{

	* Levels
	foreach ice in `shocks'{
	foreach weight in `graphweights'{
		insheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_mktregs_cbo942d.csv", clear
		keep if ice=="`ice'" & weight==`weight' & clust=="llm"
		replace outcome = subinstr(outcome,"D","",.)
		reshape long y, i(outcome stat n) j(year)
		keep if outcome=="`outc'" & (stat=="b" | stat=="ll" | stat=="ul")
		
		reshape wide y, i(outcome n year) j(stat) string
		
		expand 2 in 1
		replace year=1991 if _n==15
		foreach x in yb yll yul{
			replace `x' = 0 if year==1991
		}
		
		ren yb b
		ren yll lb
		ren yul ub
		
		sort year
		twoway  (rarea lb ub year, sort color(blue%25) lwidth(none)) ///
				(connect b year, lpattern(solid) msymbol(O) msize(small) color(blue)), ///
		scheme(s1mono)  ///
		legend(off) xtitle("") ///
		xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
		xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
		ytitle("``outc''", size(small)) yscale(titlegap(*5))

		graph export "${monopsonies}/eps/`graphsdate'/DD_mmc_cbo942d_`outc'_1986_2000_stacked_`ice'_w`weight'.pdf", replace
		
		* De-trended
		insheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_residual_mktregs_cbo942d.csv", clear
		keep if ice=="`ice'"  & weight==`weight' & clust=="llm"
		replace outcome = subinstr(outcome,"D","",.)
		reshape long y, i(outcome stat n) j(year)
		keep if outcome=="`outc'" & (stat=="b" | stat=="ll" | stat=="ul")
		
		reshape wide y, i(outcome n year) j(stat) string
		
		expand 2 in 1
		replace year=1991 if _n==15
		foreach x in yb yll yul{
			replace `x' = 0 if year==1991
		}
		
		ren yb b
		ren yll lb
		ren yul ub
		
		sort year
		twoway  (rarea lb ub year, sort color(blue%25) lwidth(none)) ///
				(connect b year, lpattern(solid) msymbol(O) msize(small) color(blue)), ///
		scheme(s1mono)  ///
		legend(off) xtitle("") ///
		xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
		xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
		ytitle("``outc''", size(small)) yscale(titlegap(*5))

		graph export "${monopsonies}/eps/`graphsdate'/DD_mmc_cbo942d_`outc'_1986_2000_stacked_resid_`ice'_w`weight'.pdf", replace
		}
	}
}


	
