/*
	Market-level effect on weighted average wage markdown
	
*/
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

local premiadate	= 20210802
local etadate		= 20210802
local thetadate		= 20210802
local mktregsdate	= 20210802

local outdate	= 20210802

local gamma					= 0		/* Get Gamma estimate */
local graphs				= 1		/* Get Gamma estimate */

local etaclust			"firm"
local etaweights		= "all w0"
local thetaweight 		= "all"
local iceshock			"ice_dwtrains_hf"
local thetaclusts		"2way fe_ro"

local etaweightmain		"w0"
local thetaclustmain	"fe_ro"
local mktregclust		"llm"
	
* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"

******************************************************************************

if `gamma'==1{
	
	foreach thetaclust in `thetaclusts'{
	foreach etaweight in `etaweights'{
		insheet using "${monopsonies}/csv/`etadate'/eta_change_regressions.csv", clear
		keep if samp=="all" & model=="l" & wagevar=="lndp" & year==1997 & clust=="`etaclust'" & spec=="l" & tariff=="lnT" & weight=="`etaweight'"
		levelsof iv_b, local(eta_inverse_b)
		levelsof iv_se, local(eta_inverse_se)

		**************************
		*** Get theta estimate ***
		**************************
		insheet using "${monopsonies}/csv/`thetadate'/theta_change_regressions_simpler.csv", clear
		keep if esamp=="all" & tsamp=="all" & wagevar=="lndp" & year==1997 & thetaclust=="`thetaclust'" & deltatype == "delta_ro" & weight=="`thetaweight'" & chng_lrotype=="chng_Lro" & etaweight=="`etaweight'"
		levelsof iv_b, local(diff_b)
		levelsof iv_se, local(diff_se)
		levelsof theta_inverse_b, local(theta_inverse_b)
		levelsof theta_inverse_se, local(theta_inverse_se)

		**************************
		* Get effects on HHI
		**************************
		
		insheet using "${monopsonies}/csv/`mktregsdate'/DD_stacked_annual_mktregs_cbo942d.csv", clear
		tab outcome
		
			keep if outcome=="Dhf_pdbill" & weight==0 & ice=="iceH" & clust == "`mktregclust'"
			
			isid stat
			keep if stat=="b" | stat=="se"
			
			reshape long y, i(stat) j(year)
			ren y beta_
			keep stat year beta_
			reshape wide beta_, i(year) j(stat) string
			
			gen double gamma = -(`diff_b')*beta_b
			
			gen double gamma_V = (`diff_se'^2 +`diff_b'^2)*(beta_se^2 + beta_b^2) - (`diff_b'^2)*beta_b^2 
			assert gamma_V>0
			
			gen double gamma_se = sqrt(gamma_V)
			
			gen  etaweight = "`etaweight'"
			gen  thetaweight = "`thetaweight'"
			gen  thetaclust = "`thetaclust'"
			gen  mktregspec = "level"
			
			order mktregspec etaweight thetaweight thetaclust
			
			tempfile level`etaweight'`thetaclust'
			sa `level`etaweight'`thetaclust''
			
		insheet using "${monopsonies}/csv/`mktregsdate'/DD_stacked_annual_residual_mktregs_cbo942d.csv", clear
		tab outcome
		
			keep if outcome=="Dhf_pdbill" & weight==0 & ice=="iceH" & clust=="`mktregclust'"
			
			isid stat
			keep if stat=="b" | stat=="se"
			
			reshape long y, i(stat) j(year)
			ren y beta_
			keep stat year beta_
			reshape wide beta_, i(year) j(stat) string
			
			gen double gamma = -(`diff_b')*beta_b
			
			gen double gamma_V = (`diff_se'^2 +`diff_b'^2)*(beta_se^2 + beta_b^2) - (`diff_b'^2)*beta_b^2 
			assert gamma_V>0
			
			gen double gamma_se = sqrt(gamma_V)

			gen  etaweight = "`etaweight'"
			gen  thetaweight = "`thetaweight'"
			gen  thetaclust = "`thetaclust'"
			gen  mktregspec = "residual"
			
			order mktregspec etaweight thetaweight thetaclust
			
			tempfile resid`etaweight'`thetaclust'
			sa `resid`etaweight'`thetaclust''

	}
	}
	
	u `level`etaweightmain'`thetaclustmain''
	foreach thetaclust in `thetaclusts'{
	foreach etaweight in `etaweights'{
		append using `level`etaweight'`thetaclust''
		append using `resid`etaweight'`thetaclust''
	}
	}
	duplicates drop
	outsheet using "${monopsonies}/csv/`outdate'/gamma_estimates_based_on_hhi.csv", comma replace
}

if `graphs'==1{

		insheet using  "${monopsonies}/csv/`outdate'/gamma_estimates_based_on_hhi.csv", clear
		keep if etaweight=="`etaweightmain'" & mktregspec=="level" & thetaclust=="`thetaclustmain'"
		gen double b = -gamma	/* Made sign edit here because now gamma is the markdown, not take-home share. Re-run all later */
		gen double lb = -gamma + 1.96*gamma_se
		gen double ub = -gamma - 1.96*gamma_se
		
		expand 2 in 1
			replace year=1991 if _n==15
			foreach x in b lb ub{
				replace `x' = 0 if year==1991
		}
		
		sort year
		
		twoway  (rarea lb ub year, sort color(blue%25) lwidth(none)) ///
				(connect b year, lpattern(solid) msymbol(O) msize(small) color(blue)), ///
		scheme(s1mono)  ///
		legend(off) xtitle("") ///
		xlabel(1986(2)2000, labsize(small) ang(hor)) yline(0, lpattern(dash) lcolor(gs8) ) ///
		xline(1990, lpattern(dash) lcolor(black)) xline(1994, lpattern(dash) lcolor(black)) ///
		ytitle("LLM average wage markdown", size(small)) yscale(titlegap(*5))

		graph export "${monopsonies}/eps/`outdate'/DD_gamma_fixed.pdf", replace

}
