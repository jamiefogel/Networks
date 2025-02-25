version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154

*ssc install vcemway

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



local premiadate	= 20210802		/* Firm premia */

local etadate		= 20210802
local thetadate		= 20210802
local regsdate		= 20210802
local graphsdate	= 20210802

local baseyear		= 1991
local baseyear_o1	= 1994
local baseyear_o2	= 1997

local minyear	= 1986
local maxyear	= 2000

local avg_decomp				= 1
local regressions 				= 1
local graphs					= 1

local etaweights	= "all w0"
local thetaweight 	= "all"
local iceshock		"ice_dwtrains_hf"

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"


if `avg_decomp'==1{
	
  foreach etaweight in `etaweights'{	
	************************
	*** Get eta estimate ***
	************************
	insheet using "${monopsonies}/csv/`etadate'/eta_change_regressions.csv", clear
	keep if samp=="all" & model=="l" & wagevar=="lndp" & year==`baseyear_o2' & clust=="firm" & spec=="l" & tariff=="lnT" & weight=="`etaweight'"
	levelsof iv_b, local(eta_inverse_`etaweight')
  
	**************************
	*** Get theta estimate ***
	**************************
	insheet using "${monopsonies}/csv/`thetadate'/theta_change_regressions.csv", clear
	keep if esamp=="all" & tsamp=="all" & model=="l" & wagevar=="lndp" & year==`baseyear_o2' & thetaclust=="fe_ro" & deltatype == "delta_ro" & spec=="m" & weight=="`thetaweight'" & chng_lrotype=="chng_Lro" & etaweight=="`etaweight'"
	levelsof theta_inverse_b, local(theta_inverse_`etaweight')
	}
	
	u "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", clear

	keep fakeid_firm mmc cbo942d year emp
	
	*  Wage premium share
	merge 1:1 fakeid_firm mmc cbo942d year using "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_`premiadate'.dta", keep(3) keepusing(dprem_zro) nogen
	gen double prem = exp(dprem_zro)
	gen double totprem = prem*emp
	
	gegen double mktprem = sum(totprem), by(mmc cbo942d year)
	gen double pshare = totprem/mktprem
	drop totprem mktprem
	
	* Employment share
	gegen double totemp = sum(emp), by(mmc cbo942d year)
	gen double eshare = emp/totemp
	
	* Wage markdown
	gen double epsilon_inverse_etaall = `eta_inverse_all'*(1-pshare) + `theta_inverse_all'*pshare
	gen double epsilon_inverse_etaw0 = `eta_inverse_w0'*(1-pshare) + `theta_inverse_w0'*pshare
	
	gen double takehome_etaall = 1/(1+epsilon_inverse_etaall)
	gen double markdown_etaall = 1/takehome_etaall
	
	gen double takehome_etaw0 = 1/(1+epsilon_inverse_etaw0)
	gen double markdown_etaw0 = 1/takehome_etaw0
	
	* MRPL
	gen double mrpl_etaall = prem*(1+epsilon_inverse_etaall)
	gen double mrpl_etaw0 = prem*(1+epsilon_inverse_etaw0)
	
	preserve
		keep fakeid_firm mmc cbo942d year dprem_zro takehome* markdown* pshare epsilon_inverse* emp eshare
		compress
		saveold "${monopsonies}/sas/firm_mmc_cbo942d_mus.dta", replace
	restore
	
	compress
	
	* Weights
	foreach x in eshare mrpl_etaall mrpl_etaw0{
		bys fakeid_firm mmc cbo942d: egen `x'`baseyear'= mean(cond(year==`baseyear',`x',.))
	}
	
	preserve
		collapse (mean) avg_mrpl_etaall_w_emp=mrpl_etaall ///
						avg_mrpl_etaw0_w_emp=mrpl_etaw0 ///
						avg_prem_w_emp=prem [w=emp], by(mmc cbo942d year)
		tempfile avg_prem_w_emp
		sa `avg_prem_w_emp'
	restore
	
	foreach etaweight in `etaweights'{
		preserve
			collapse (mean) avg_mrpl_eta`etaweight'_w_eshare1991=mrpl_eta`etaweight' [w=eshare1991], by(mmc cbo942d year)
			
			tempfile avg_mrpl_eta`etaweight'_w_eshare1991
			sa `avg_mrpl_eta`etaweight'_w_eshare1991'
		restore

		preserve
			collapse (mean) avg_eshare_w_mrpl_eta`etaweight'1991=eshare [w=mrpl_eta`etaweight'1991], by(mmc cbo942d year)
			
			tempfile avg_eshare_w_mrpl_eta`etaweight'1991
			sa `avg_eshare_w_mrpl_eta`etaweight'1991'
		restore

	}
	
	keep mmc cbo942d year
	gduplicates drop
	merge 1:1 mmc cbo942d year using `avg_prem_w_emp', keep(3) nogen
	merge 1:1 mmc cbo942d year using `avg_mrpl_etaall_w_eshare1991', keep(3) nogen
	merge 1:1 mmc cbo942d year using `avg_mrpl_etaw0_w_eshare1991', keep(3) nogen
	merge 1:1 mmc cbo942d year using `avg_eshare_w_mrpl_etaall1991', keep(3) nogen
	merge 1:1 mmc cbo942d year using `avg_eshare_w_mrpl_etaw01991', keep(3) nogen
	
	saveold "${monopsonies}/sas/avg_wage_decomposition_mmc_cbo942d.dta", replace
}

if `regressions'==1{ 
	
	local group "cbo942d"
	
	u "${monopsonies}/sas/avg_wage_decomposition_mmc_cbo942d.dta", clear
	
	*** Merge in market-level dataset ***
	merge 1:1 mmc `group' year using "${monopsonies}/sas/regsfile_mmc_`group'.dta", keep(3) nogen ///
	keepusing(ice* mkt_emp)
	drop if inlist(mmc,13007)
	merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
	drop if mmc_drop==1
	
	* Drop civil service, diplomats, mailmen
	drop if inlist(cbo942d,31,22,37)
	
	* Keep balanced panel
	bys mmc `group': `tool'egen totyears = count(year)
	egen double maxy = max(totyears)

	keep if totyears==maxy
	drop maxy totyears
	
	egen double llm = group(mmc `group')
	if "`thetaweight'"=="w0"{
		bys llm: egen double mktweight = max(cond(year==`baseyear',mkt_emp,.))
	}
	else{
		gen double mktweight=1
	}
	
	keep mmc `group' llm year `iceshock' mktweight avg_*
	ds mmc `group' llm year `iceshock' mktweight, not
	local regvars "`r(varlist)'"
	
	* Long differences
	foreach var of varlist `regvars'{
		bys mmc cbo942d: egen double `var'`baseyear' = max(cond(year==`baseyear',`var',.))
		
		gen double D`var' = (`var' - `var'`baseyear')
		replace D`var' = -D`var' if year<`baseyear'
		drop `var'`baseyear'
	}
	
	keep mmc `group' year llm `iceshock' D* mktweight
	
	replace `iceshock' = 0 if missing(`iceshock')
	replace `iceshock'=- `iceshock'
	
	* Interactions with year
	forvalues y=1986/2000{
		gen double fe_`y' = (year==`y')
		gen double ICE_`y' = `iceshock'*(year==`y')
	}
	
	* Ommit base year
	renvars ICE_`baseyear' fe_`baseyear', pref("om_")
	
	*gen double ICEtrend =`iceshock'*year
	gen double ICEtrend =`iceshock'*(`baseyear' - year)
	
	foreach x in `regvars'{
	
		* De-trending
		reghdfe D`x' ICEtrend [w=mktweight] if year<=1989, absorb(llm year) vce(cluster mmc cbo942d)

		gen double nres`x' = D`x'
		replace nres`x' = D`x' - _b[ICEtrend]*`iceshock'*(`baseyear' - year) if year>1989
				
		foreach var in i_llm i_year fe_llm fe_year res pred{
			cap drop `var'
		}
		
		* Annual specification in stacked mode
			* Levels
				reghdfe D`x' ICE_* fe_* [w=mktweight], absorb(llm) vce(cluster mmc cbo942d)
				
				mat b  = r(table)[1,1..14]
				mat se = r(table)[2,1..14]
				mat ll = r(table)[5,1..14]
				mat ul = r(table)[6,1..14]
				
				mat y`x' =(b \ se \ ll \ ul)
				count if e(N)
				local Ny`x' = `r(N)'
				
			* De-trended
				* Adjust dof to account for estimated coefficient used in detrending
				qui reghdfe nres`x' ICE_* fe_* [w=mktweight], absorb(llm) vce(cluster mmc cbo942d)
				local ndf = `e(df_r)'-1
				local adj = `e(df_r)'/`ndf'
				mat v1 = e(V)
				mat V = `adj'*v1
				
				mat se = vecdiag(cholesky(diag(vecdiag(V))))
				
				mat b  = r(table)[1,1..14]
				mat se = se[1,1..14]
				
				mat ll = b - 1.96*se
				mat ul = b + 1.96*se
				mat ny`x' = (b \ se \ ll \ ul)
				
				/*
				mat ny`x' = r(table)
				mat ny`x' = ny`x'[1..6,1..14]
				
				*/
				count if e(N)
				local Nny`x' = `r(N)'
	}
	
	clear
	foreach x in `regvars'{
		foreach stype in y ny{
			
			clear
			svmat `stype'`x'
			gen outcome = "D`x'"
			gen stat = ""
			replace stat = "b" if _n==1
			replace stat = "se" if _n==2
			
			/*
			replace stat = "t"  if _n==3
			replace stat = "pvalue" if _n==4
			replace stat = "ll" if _n==5
			replace stat = "ul" if _n==6
			*/
			
			replace stat = "ll" if _n==3
			replace stat = "ul" if _n==4
			gen double N = `N`stype'`x''
			
			keep if !missing(stat)
			
			ren `stype'`x'1 y1986
			ren `stype'`x'2 y1987
			ren `stype'`x'3 y1988
			ren `stype'`x'4 y1989
			ren `stype'`x'5 y1990
			ren `stype'`x'6 y1992
			ren `stype'`x'7 y1993
			ren `stype'`x'8 y1994
			ren `stype'`x'9 y1995
			ren `stype'`x'10 y1996
			ren `stype'`x'11 y1997
			ren `stype'`x'12 y1998
			ren `stype'`x'13 y1999
			ren `stype'`x'14 y2000
			
			order outcome stat N
			
			tempfile c`stype'`x'
			sa `c`stype'`x''
		}
	}	
	
	u `cyavg_prem_w_emp', clear
	foreach x in `regvars'{
		append using `cy`x''
	}
	duplicates drop
	
	outsheet using "${monopsonies}/csv/`regsdate'/DD_avg_wage_decomposition.csv", comma replace
	
	u `cnyavg_prem_w_emp', clear
	foreach x in `regvars'{
		append using `cny`x''
	}
	duplicates drop
	
	outsheet using "${monopsonies}/csv/`regsdate'/DD_avg_wage_decomposition_resid.csv", comma replace
	
}

if `graphs'==1{

	foreach etaweight in `etaweights'{
		insheet using "${monopsonies}/csv/`regsdate'/DD_avg_wage_decomposition_resid.csv", clear
		
			keep if outcome=="Davg_mrpl_eta`etaweight'_w_eshare1991" 
			
			keep if stat=="b" | stat=="ll" | stat=="ul"
			
			reshape long y, i(outcome stat n) j(year)

			reshape wide y, i(outcome n year) j(stat) string
			
			count
			expand 2 in 1

			replace year=1991 if _n==`r(N)'+1
			foreach x in yb yll yul{
				replace `x' = 0 if year==1991
			}
			
			ren yb b
			ren yll lb
			ren yul ub
			sort outcome year
			
			twoway  (rarea lb ub year if outcome=="Davg_mrpl_eta`etaweight'_w_eshare1991", sort color(red%25) lwidth(none) ) ///
				(connect b year if outcome=="Davg_mrpl_eta`etaweight'_w_eshare1991", lpattern(solid) msymbol(O) msize(small) color(red) ), ///
				scheme(s1mono) xtitle("") legend(off) ///
			xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
			xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
			ytitle("LLM average MRPL: within change", size(small)) yscale(titlegap(*5) )

			graph export "${monopsonies}/eps/`graphsdate'/DD_MRPL_decomposition_within_eta`etaweight'.pdf", replace
		
		insheet using "${monopsonies}/csv/`regsdate'/DD_avg_wage_decomposition_resid.csv", clear
		
			keep if outcome=="Davg_eshare_w_mrpl_eta`etaweight'1991" 
			
			keep if stat=="b" | stat=="ll" | stat=="ul"
			
			reshape long y, i(outcome stat n) j(year)

			reshape wide y, i(outcome n year) j(stat) string
			
			count
			expand 2 in 1

			replace year=1991 if _n==`r(N)'+1
			foreach x in yb yll yul{
				replace `x' = 0 if year==1991
			}
			
			ren yb b
			ren yll lb
			ren yul ub
		
		sort outcome year
		
		twoway  (rarea lb ub year if outcome=="Davg_eshare_w_mrpl_eta`etaweight'1991", sort color(blue%25) lwidth(none) ) ///
				(connect b year if outcome=="Davg_eshare_w_mrpl_eta`etaweight'1991", lpattern(solid) msymbol(O) msize(small) color(blue) ), ///
				scheme(s1mono) xtitle("") legend(off) ///
		xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
		xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
		ytitle("LLM average MRPL: cross-firm reallocation", size(small)) yscale(titlegap(*5) )

		graph export "${monopsonies}/eps/`graphsdate'/DD_MRPL_decomposition_cross_eta`etaweight'.pdf", replace
		
		}

}

