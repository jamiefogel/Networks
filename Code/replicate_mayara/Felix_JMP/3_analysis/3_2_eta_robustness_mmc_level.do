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

local outdate		= 20210802
local premiadate	= 20210802
local baseyear 		= 1991
local baseyear_n 	= 91

local lagyears		= 3
local baseyear_o1 	= `baseyear'+3
local baseyear_o2	= `baseyear'+6
local baseyear_p1	= `baseyear'-3
local baseyear_p2	= `baseyear'-5

local setup 			= 1
local eta_regs 			= 1
local usesample 		= 0			/* Use 10% random sample of firms*/


* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"


local allspecs 		"l"
local alltars 		"lnT"
local allwages 		"lndp"
local allmodels 	"l"			/* b: back to 1985; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"firm cnae95"
local allsamp 		"all up`baseyear_n'mkt"

local mainwage 	"lndp"
local mainclust "firm"
local maintar	"lnT"

* Specification FEs
local labsorb "fe_r"			/* When spec is m, absorb mmc-none */

*************************************
**************** Setup **************
*************************************

if `setup'==1{

  foreach sampfile in /*1 */ 0 {

		u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
		ren cnae10 cnae95
		keep if year==1990 | year==1994
		keep if  !missing(TRAINS)
		keep year cnae95 ibgesubsector TRAINS ErpTRAINS
		sort cnae95 year
		foreach var of varlist TRAINS ErpTRAINS{
			replace `var' = `var'/100
			bys cnae95: gen double chng_ln`var' = ln(1+`var') - ln(1+`var'[_n-1])
		}
		keep if year==`baseyear_o1'
		keep cnae95 ibgesubsector chng*
		gduplicates drop

		tempfile t_change
		sa `t_change'

		u "${monopsonies}/sas/regsfile_mmc_none.dta", clear
		keep if year==`baseyear'
		ren mkt_emp bemp
		keep mmc  ice_dwerp ice_dwtrains bemp
		gduplicates drop
		tempfile market
		sa `market'

		u "${monopsonies}/dta/fakeid_importers_exporters_allyears_20191213.dta", clear
		keep if inrange(year,`baseyear',`baseyear_o1')
		gegen explib = max(exporter), by(fakeid_firm)
		gegen implib = max(importer), by(fakeid_firm)

		gegen bexp = max(cond(year==`baseyear',exporter,.)), by(fakeid_firm)
		gegen bimp = max(cond(year==`baseyear',importer,.)), by(fakeid_firm)

		keep fakeid_firm explib bexp
		gduplicates drop
		tempfile exporters
		sa `exporters'

		u "${monopsonies}/sas/rais_collapsed_firm_mmc_none.dta", clear
		keep if inlist(year,`baseyear_p2',`baseyear_p1',`baseyear',`baseyear_o1',`baseyear_o2')
		
		merge 1:1 fakeid_firm mmc year using "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_`premiadate'.dta", ///
		keepusing(dprem_zr davgw_zr) keep(3) nogen
		
		ren dprem_zr lndp
		ren davgw_zr lndw
		/*
		* Bring in earnings premia
		merge 1:m fakeid_firm mmc year using "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_`premiadate'.dta", ///
		keepusing(dprem_zro davgw_zro) keep(3) nogen
		
		bys fakeid_firm mmc year: egen double den = sum(emp)
		foreach var of varlist dprem_zro davgw_zro{
			gen double `var'temp = `var'*emp
			bys fakeid_firm mmc year: egen double num = sum(`var'temp)
			
			gen double m`var' = num/den
			drop `var'
			ren m`var' `var'
			drop num 
		}
		drop den
		ren dprem_zro lndp
		ren davgw_zro lndw
		
		gduplicates drop fakeid_firm mmc year, force
		*/
		
	
		
		***********************************
		******* Sample restrictions *******
		***********************************

		if `sampfile'==1{
			preserve
				keep fakeid_firm
				gduplicates drop
				sample 10
				tempfile samp
				sa `samp'
			restore
			merge m:1 fakeid_firm using `samp', keep(3) nogen
		}

		* Drop certain mmcs
		drop if mmc==13007 
		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
		drop if mmc_drop==1
		
		* Merge in tariffs so can also restrict to tradables
		merge m:1 cnae95  using `t_change', keep(1 3) nogen		/* Long change in tariffs */

		* Tradable sector dummies (ibgesub only included in TRAINS data at this point)
		gegen ibge = max(ibgesubsector), by(fakeid_firm)
		gen T = (ibge<14 | ibge==25)
		
		* Merge in exporter dummies
		merge m:1 fakeid_firm using `exporters', keep(1 3) nogen
		foreach var of varlist explib bexp{
			replace `var' = 0 if missing(`var')
		}
		
		gen Tnexplib = T==1 & explib==0 
		drop ibgesubsector
		
		replace chng_lnTRAINS 		= 0 if T==0
		replace chng_lnErpTRAINS 	= 0 if T==0
		
		* Compute firm baseline share
		preserve
			 keep if year==`baseyear'
			 gegen double sumearn = sum(totmearn), by(mmc )
			 gen double bwshare = totmearn/sumearn
			 gegen double sumemp = sum(emp), by(mmc )
			 gen double beshare = emp/sumemp

			 gegen double Tsumearn = sum(totmearn) if T==1, by(mmc )
			 gen double bTwshare = totmearn/Tsumearn if T==1
			 gegen double Tsumemp = sum(emp) if T==1, by(mmc )
			 gen double bTeshare = emp/Tsumemp if T==1
			 
			 keep fakeid_firm mmc  beshare bwshare bTeshare bTwshare 
			 tempfile shares
			 sa `shares'
		restore

		* Merge in base shares, replacing with zero if firm was not there
		merge m:1 fakeid_firm mmc  using `shares', keep(1 3) nogen

		* Dummy for whether firm-market pair existed in `baseyear'
		gen double in`baseyear_n'mkt = !missing(beshare)

		* Dummy for whether firm exited in `baseyear'
		gegen in`baseyear_n' = max(in`baseyear_n'mkt), by(fakeid_firm)

		foreach var of varlist beshare bwshare bTeshare bTwshare{
			replace `var' = 0 if missing(`var')
		}

		* Keep firms that were there in `baseyear' (but ok if firms enters new markets)
		keep if in`baseyear_n'==1
		************************************

		* Flag Unique producers of each cnae95 within each market
		preserve
			keep if year==`baseyear' & T==1
			keep fakeid_firm cnae95 mmc 
			bys cnae95 mmc : gegen producers = count(fakeid_firm)
			keep if producers==1
			keep fakeid_firm mmc 
			
			gen up`baseyear_n' = 1
			tempfile unique 
			sa `unique'
		restore	
		merge m:1 fakeid_firm mmc  using `unique', keep(1 3) nogen

		replace up`baseyear_n' = 0 if missing(up`baseyear_n')

		* Market that has unique producer 
		gegen up`baseyear_n'mkt_temp = max(up`baseyear_n'), by(mmc )
		gen double up`baseyear_n'mkt = up`baseyear_n'==1 | (up`baseyear_n'mkt_temp==1 & T==0)
		drop up`baseyear_n'mkt_temp
		
		* Merge in market shocks
		merge m:1 mmc  using `market', keep(3) nogen
		
		* Log employment
		gen double lnemp = ln(emp)
		
		gegen fe_zr = group(fakeid_firm mmc )
	
		di "Long differences"
		preserve
			keep fe_zr year lnemp `allwages'
			xtset fe_zr year
			
			foreach var of varlist lnemp `allwages'{
				
				* lag-year differences
				gen double chng_`var' = `var' - l`lagyears'.`var'
				
				* Difference back to base year
				gegen double `var'`baseyear_n' 	= max(cond(year==`baseyear',`var',.)), by(fe_zr)
				gen double chng`baseyear_n'_`var' = `var' - `var'`baseyear_n'
				
				* Replace with negative if year is before base year
				replace chng`baseyear_n'_`var' 	= - chng`baseyear_n'_`var' if year<`baseyear'
			}
			
			keep if inlist(year,`baseyear_p2',`baseyear_p1',`baseyear',`baseyear_o1',`baseyear_o2')
			keep year fe_zr chng*

			tempfile long
			sa `long'
		restore
		
		di "Keep changes and baseyear info only"
		keep if inlist(year,`baseyear_p2',`baseyear_p1',`baseyear',`baseyear_o1',`baseyear_o2')
		merge m:1 year fe_zr using `long', keep(1 3) nogen
		
		keep 	fakeid_firm mmc  year cnae95 ///
				fe_zr bTwshare bTeshare bexp bwshare bemp beshare explib ice_dwerp ice_dwtrains ///
				chng* up`baseyear_n' up`baseyear_n'mkt T Tnexplib
		
		order	year fe_zr fakeid_firm mmc  cnae95 up`baseyear_n' up`baseyear_n'mkt T Tnexplib explib
				
		compress
		saveold "${monopsonies}/sas/eta_robustness_mmc_changes_regsfile`sampfile'.dta", replace

	} /* Close use sample */
}

***************************************************
***************** Eta regressions *****************
***************************************************	

if `eta_regs'==1{
	
	u "${monopsonies}/sas/eta_robustness_mmc_changes_regsfile`usesample'.dta", clear
	
	* Cross-section FEs
	gen double fe_r = mmc 
		
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
				
				di "Running ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], cluster(`clust') absorb(delta_r =``spec'absorb') "
				qui ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], savefirst saverf cluster(`clust') absorb(delta_r =``spec'absorb') 
			
				preserve
					keep if `tsamp'==1 & year==`year'
					keep fakeid_firm mmc  `lhs' `rhs' delta_r
					keep if !missing(delta_r)

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
				drop delta_r
			}
			else{
				di "Running ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], cluster(`clust') absorb(``spec'absorb') "
				qui ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], savefirst saverf cluster(`clust') absorb(``spec'absorb') 
			}
			
			local obs = e(N)
			unique fakeid_firm if e(sample)
			local firms = `r(unique)'
			unique  mmc if e(sample)
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
	outsheet using "${monopsonies}/csv/`outdate'/eta_robustness_mmc_change_regressions.csv", comma replace
	
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
	saveold "${monopsonies}/dta/coeffs/`outdate'/eta_robustness_mmc_change_delta_r.dta", replace

}
