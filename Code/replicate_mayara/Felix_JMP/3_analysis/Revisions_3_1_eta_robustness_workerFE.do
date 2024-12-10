/*
	Firm-market level regressions
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

local outdate			= 20220623
local premiadate	= 20220623		/* Date of worker FE premia */
local baseyear 		= 1991
local baseyear_n 	= 91

local lagyears		= 3
local baseyear_o1 	= `baseyear'+3
local baseyear_o2	= `baseyear'+6
local baseyear_p1	= `baseyear'-3
local baseyear_p2	= `baseyear'-5

local eta_regs 			= 1
local usesample 		= 0			/* Use 10% random sample of firms*/


* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"

/*
local tarvars 		"lnT"
local allspecs 		"l"
local wagevars 		"lndp"
local allmodels 	"l"			/* b: back to 1985; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"cnae95"
local allsamp 		"all up`baseyear_n'mkt"
*/

local allspecs 		"l"
local alltars 		"lnT lnE"
local allwages 		"lndp lndF lndw"
local allmodels 	"l"			/* b: back to 1986; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"firm cnae95 fe_ro"
local allsamp 		"all T up`baseyear_n'mkt up`baseyear_n' explib Tnexplib"

local mainwage 	"lndp"
local mainclust "cnae95"
local maintar	"lnT"

* Specification FEs
local labsorb "fe_ro"			/* When spec is m, absorb mmc-cbo942d */


***************************************************
***************** Eta regressions *****************
***************************************************	

if `eta_regs'==1{

	* Get premia based on worker FEs

	u "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_workerFE`premiadate'.dta", clear

	reshape wide dprem_zrot, i(fakeid_firm mmc cbo942d) j(year)

	foreach year in `baseyear_p2' `baseyear_p1' `baseyear' `baseyear_o1' `baseyear_o2'{
		gen double chng91_lndF`year' =  dprem_zrot`year' - dprem_zrot1991
	}
	
	keep fakeid_firm mmc cbo942d chng91_lndF*
	reshape long chng91_lndF, i(fakeid_firm mmc cbo942d) j(year)

	tempfile wchange
	sa `wchange'

	u "${monopsonies}/sas/eta_changes_regsfile`usesample'.dta", clear
	
	cap drop if inlist(cbo942d,31,22,37)
	merge 1:1 fakeid_firm mmc cbo942d year using `wchange', keep(1 3) nogen

	* Cross-section FEs
	gegen fe_ro = group(mmc cbo942d)
		
	ren chng_lnTRAINS chng_lnT
	ren chng_lnErpTRAINS chng_lnE
	
	/* Flip sign for easier interpretation */
	replace chng_lnT = - chng_lnT
	replace chng_lnE = - chng_lnE
	replace ice_dwtrains = - ice_dwtrains
	
	gen double firm = fakeid_firm
	gen all = 1
	ren bemp w0
	
	ren ice_dwtrains 	ice
	ren bexp 		bex
	ren bwshare     bws
	ren beshare		bes
	
	foreach tsamp in `allsamp'{
		if "`tsamp'"=="all" | "`tsamp'"=="up`baseyear_n'mkt"{
			local clusters "`allclust'"
			local wagevars "`allwages'"
			local tarvars  "`alltars'"
		}
		else{
			local clusters "`mainclust'"
			local wagevars "`mainwage'"
			local tarvars  "`maintar'"
		}
	foreach weight in all w0{
	foreach wvar in `wagevars'{
	foreach tariff in `tarvars'{
		local inst "chng_`tariff'"
	foreach model in `allmodels'{
	
		if "`model'"=="s"{
			local lhs "chng_`wvar'"
			local rhs "chng_lnemp"
			*local years "`baseyear_o1' `baseyear_o2'"
		}
		else if "`model'"=="l"{
			local lhs "chng`baseyear_n'_`wvar'"
			local rhs "chng`baseyear_n'_lnemp"
			*local years  "`baseyear_p2' `baseyear_o2'"
			local years  "`baseyear_o2'"
		}
		
		foreach year in `years'{
		foreach clust in `clusters'{		
		foreach spec in `allspecs'{
			
			* Store fixed effects for theta estimation in next step
			if `year'==`baseyear_o2' & ("`tsamp'"=="all" | "`tsamp'"=="up`baseyear_n'mkt"){
				
				di "Running ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], cluster(`clust') absorb(delta_ro =``spec'absorb') "
				qui ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], savefirst saverf cluster(`clust') absorb(delta_ro =``spec'absorb') 
			
				preserve
					keep if `tsamp'==1 & year==`year'
					keep fakeid_firm mmc cbo942d `lhs' `rhs' delta_ro
					keep if !missing(delta_ro)

					ren `lhs' chng_wagevar
					ren `rhs' chng_lnemp
					
					gen outyear 	= `year'
					gen spec    	= "`spec'"
					gen model   	= "`model'"
					gen tariff  	= "`tariff'"
					gen cluster 	= "`clust'"
					gen sample  	= "`tsamp'"
					gen wagevartype = "`wvar'"
					gen weight		= "`weight'"
					
					tempfile r`spec'`clust'`tsamp'`model'`tariff'`year'`wvar'`weight'
					sa `r`spec'`clust'`tsamp'`model'`tariff'`year'`wvar'`weight''
				restore
				drop delta_ro
			}
			else{
				di "Running ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], cluster(`clust') absorb(``spec'absorb') "
				qui ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], savefirst saverf cluster(`clust') absorb(``spec'absorb') 
			}
			
			local obs = e(N)
			unique fakeid_firm if e(sample)
			local firms = `r(unique)'
			unique cbo942d mmc if e(sample)
			local mkts = `r(unique)'
			
			local m_iv_b = _b[`rhs']
			local m_iv_se = _se[`rhs']
			
			mat first = e(first)

			estimates restore _ivreg2_`lhs'
			local m_rf_b = _b[`inst']
			local m_rf_se = _se[`inst']
			estimates restore _ivreg2_`rhs'
			local m_fs_b = _b[`inst']
			local m_fs_se = _se[`inst']
			local FS_F = first[4,1]
			
			di "Running OLS: reghdfe `lhs' `rhs' if `tsamp'==1 & year==`year' [w=`weight'], vce(cluster `clust') absorb(``spec'absorb') "
			qui reghdfe `lhs' `rhs' if `tsamp'==1 & year==`year' [w=`weight'], vce(cluster `clust') absorb(``spec'absorb') 
			local m_ols_b = _b[`rhs']
			local m_ols_se = _se[`rhs']
			
			mat coeffs = (	`m_ols_b',`m_ols_se',`m_iv_b',`m_iv_se', ///
						`m_rf_b',`m_rf_se',`m_fs_b',`m_fs_se', ///
						`FS_F',`obs',`firms',`mkts')
			preserve
				clear
				svmat coeffs
				gen year 	= `year'
				gen spec    = "`spec'"
				gen model   = "`model'"
				gen tariff  = "`tariff'"
				gen clust = "`clust'"
				gen samp  = "`tsamp'"
				gen wagevar = "`wvar'"
				gen weight	= "`weight'"
				
				ren coeffs1 ols_b
				ren coeffs2 ols_se
				ren coeffs3 iv_b
				ren coeffs4 iv_se
				ren coeffs5 rf_b
				ren coeffs6 rf_se
				ren coeffs7 fs_b
				ren coeffs8 fs_se
				ren coeffs9 fs_F
				ren coeffs10 obs
				ren coeffs11 firms
				ren coeffs12 markets
				keep if !missing(ols_b)
		
				tempfile k`spec'`clust'`tsamp'`model'`tariff'`year'`wvar'`weight'
				sa `k`spec'`clust'`tsamp'`model'`tariff'`year'`wvar'`weight''
			restore
			

		
	estimates clear
	} /* Close spec */
	} /* Close sample */
	} /* Close cluster */
	} /* close model */
	} /* Close tariff */
	} /* close year */
	} /* Close wage */
	} /* Close weight */
	
	*********************************************
	************* Outsheet results  *************
	*********************************************
	
	** Append all main regression coefficients and export to csv ***
	u `kl`mainclust'allllnT`baseyear_o2'lndpall', clear
	foreach weight in all w0{
	foreach wvar in `allwages'{
	foreach year in `baseyear_p2' `baseyear_p1' `baseyear' `baseyear_o1' `baseyear_o2'{
	foreach tariff in `alltars'{
	foreach model in `allmodels'{
	foreach spec in `allspecs'{
	foreach clust in `allclust'{
	foreach tsamp in `allsamp'{
		cap append using `k`spec'`clust'`tsamp'`model'`tariff'`year'`wvar'`weight''
	}
	}
	}
	}
	}
	}
	}
	}
	duplicates drop
	outsheet using "${monopsonies}/csv/`outdate'/eta_change_regressions.csv", comma replace
	
	******************************************************************************
	** Append all fixed effects and save for phi estimation next ***
	******************************************************************************

	u `rl`mainclust'allllnT`baseyear_o2'lndpall', clear
	foreach weight in all w0{
	foreach wvar in `allwages'{
	foreach year in `baseyear_o2'{
	foreach tariff in `alltars'{
	foreach model in `allmodels'{
	foreach spec in `allspecs'{
	foreach clust in `allclust'{
	foreach tsamp in `allsamp'{
		cap append using `r`spec'`clust'`tsamp'`model'`tariff'`year'`wvar'`weight''
	}
	}
	}
	}
	}
	}
	}
	}
	duplicates drop
	compress
	saveold "${monopsonies}/dta/coeffs/`outdate'/eta_change_delta_ro.dta", replace

}
