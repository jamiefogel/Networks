/*
	Simpler theta estimation: to get residual ksis, estimate
	eta regression in levels only adding the FE the levels reg
	calls for, which is market x year
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
local etachangedate = 20210802
local etalevelsdate = 20210802

local baseyear 		= 1991
local baseyear_n 	= 91

local lagyears		= 3
local baseyear_o1 	= `baseyear'+3
local baseyear_o2	= `baseyear'+6
local baseyear_p1	= `baseyear'-3

local getresiduals		= 0
local theta_regs 		= 1
	
* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"

local etaclustmain 	"firm"	/* Code does not allow for multiple */
local etaweights	"all w0"
local etasamp		"all u91m"
local tarvars 		"lnT"

local thetaclust 	"mmc" 
local thetaweights 	"all w0"

local wagevars "lndp"


if `getresiduals'==1{

	u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
	ren cnae10 cnae95
	keep year cnae95 ibgesubsector TRAINS ErpTRAINS
	keep if inlist(year,1990,1994)
	
	keep if !missing(TRAINS)
	foreach var of varlist TRAINS ErpTRAINS{
		replace `var' = `var'
		gen double ln`var' = ln(1+`var'/100)
		gsort cnae95 year
	}
	
	ren lnTRAINS    lnT
	ren lnErpTRAINS lnE
	
	keep year cnae95 ibgesubsector lnT lnE
	
	/* To replicate change regressions in levels:
		- For every outyear use the 1994 tariff
		- For every baseyear use the 1990 tariff
	*/
	
	reshape wide lnT lnE, i(cnae95 ibgesubsector) j(year)
	
	tempfile tariffs
	sa `tariffs'
	
	* Grab firm-market pairs in the eta_change regressions
	preserve
		u "${monopsonies}/sas/eta_robustness_mmc_changes_regsfile0.dta", clear			
		keep fakeid_firm mmc  up`baseyear_n' up`baseyear_n'mkt T bemp
		gduplicates drop 
		tempfile pairs
		sa `pairs'
	restore
	
	u "${monopsonies}/sas/rais_collapsed_firm_mmc_none.dta", clear
	keep if inlist(year,`baseyear',`baseyear_o2')
	
	keep fakeid_firm cnae95 mmc  year emp avgdecearn avgmearn
	merge m:1 fakeid_firm mmc  using `pairs', keep(3) nogen
	
	gen double lnemp = ln(emp)
	
	* Bring in firm earnings premia in mmc
	preserve
		u "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", clear
		keep fakeid_firm mmc cbo942d emp year
		
		merge 1:1 fakeid_firm mmc cbo942d year using "${monopsonies}/sas/rais_lnearn_premia_firm_mmc_cbo942d_`premiadate'.dta", ///
		keepusing(davgw_zro dprem_zro) keep(3) nogen
		
		collapse (mean) davgw_zro dprem_zro [w=emp], by(fakeid_firm mmc year)
		
		ren davgw_zro 		lndw
		ren dprem_zro 		lndp
		
		keep fakeid_firm mmc year lndw lndp
		tempfile firmp
		sa `firmp'
	restore
	
	merge 1:1 fakeid_firm mmc year using `firmp', keep(3) nogen
	
	* Get lnT1994 and lnT1990 for tradables firms
	merge m:1 cnae95 using `tariffs', keep(1 3) nogen
	
	* FEs
	cap gegen fe_rt = group(mmc  year)
	cap gegen fe_zr = group(fakeid_firm mmc )
	
	gen fe_r = mmc
	
	gen double firm = fakeid_firm
	
	gen all = 1
	ren bemp w0
	
	cap ren up`baseyear_n' u`baseyear_n'
	cap ren up`baseyear_n'mkt u`baseyear_n'm
	
	local u91m 		"up91mkt"
	local all 		"all"
	
	foreach weight in `etaweights'{
	foreach wvar in `wagevars'{
	foreach year in `baseyear' `baseyear_o2'{

		foreach esamp in `etasamp'{	
		
			* Remove market fixed effect from LHS
			qui areg `wvar'  if `esamp'==1 & year==`year', absorb(fe_r)
			predict `wvar'res, resid
			
			* Get eta inverse
			
			preserve
				insheet using "${monopsonies}/csv/`etachangedate'/eta_robustness_mmc_change_regressions.csv", clear
				keep if samp=="``esamp''" & spec=="l"  & model=="l" & wagevar=="`wvar'" & tariff=="lnT" & year==`baseyear_o2' & clust=="`etaclustmain'" & weight=="`weight'"
		
				levelsof iv_b, local(eta_inverse)
			restore
			
			* Compute residuals
			preserve
				keep if !missing(`wvar'res)
				gen double lnxi_zrt = `wvar'res - `eta_inverse'*lnemp
				keep fakeid_firm mmc  `tariff' year `wvar' `wvar'res lnemp lnxi_zrt
				
				ren `wvar' 		wagevar
				ren `wvar'res 	wagevarres
				keep if !missing(lnxi_zrt)
				gen samp  	 	= "`esamp'"
				gen wagevartype = "`wvar'"
				gen weight		= "`weight'"
				
				order fakeid_firm mmc  year
				
				compress
				tempfile r`esamp'`year'`wvar'`weight'
				sa `r`esamp'`year'`wvar'`weight''
			restore
			
			drop `wvar'res

			} /* Close sample */
	
	} /* Close year */
	} /* Wage var */
	} /* Close weight */
	

	** Append residuals and export ***
	u `rall`baseyear_o2'lndpall', clear
	foreach weight in `etaweights'{
	foreach wvar in `wagevars'{
	foreach year in `baseyear' `baseyear_o2'{
	foreach tariff in `tarvars'{
	foreach esamp in `etasamp'{
		cap append using `r`esamp'`year'`wvar'`weight''
	}
	}
	}
	}
	}

	duplicates drop
	compress
	saveold "${monopsonies}/dta/coeffs/`outdate'/theta_mmc_robustness_lnxi_zrt.dta", replace
} /* Close eta regressions */


**********************************************************
***************** theta change regressions *****************
**********************************************************

if `theta_regs'==1{

	local lhs 		"delta_r delta_r_d"
	local rhs 		"chng_Lr chng_Lr_d"
	local tariff 	"dwtrains_hf"
	local inst 		"ice_`tariff'"
	local u91m 		"up91mkt"
	local all 		"all"
	
	foreach esamp in `etasamp'{
	if "`esamp'"=="u91m"{
		local thetasamp "u91m all"
	}
	else{
		local thetasamp 	"all"
	}
	
	foreach tsamp in `thetasamp'{
	foreach wage in `wagevars'{
	foreach eweight in `etaweights'{
		
		*** Get eta estimate: always use estimate for unique producers, weighted ***
		insheet using "${monopsonies}/csv/`etachangedate'/eta_robustness_mmc_change_regressions.csv", clear
		keep if samp=="``esamp''" & spec=="l"  & model=="l" & wagevar=="`wage'" & tariff=="lnT" & year==`baseyear_o2' & clust=="`etaclustmain'" & weight=="`eweight'"
		
		levelsof iv_b, local(eta_inverse)
		levelsof iv_se, local(eta_inverse_se)
		local eta = 1/`eta_inverse'

		* Get estimates of xis 
		u "${monopsonies}/dta/coeffs/`etalevelsdate'/theta_mmc_robustness_lnxi_zrt.dta", clear
		keep if samp=="`tsamp'" & wagevartype=="`wage'" & (year==1997 | year== 1991)  & weight=="`eweight'"
	
		keep fakeid_firm mmc  lnemp lnxi_zrt year

		reshape wide ln*, i(fakeid_firm mmc ) j(year)
		 
		foreach y in 1997 1991 {
			gen double emp`y' 			= exp(lnemp`y')
			gen double xi_zr`y' 		= exp(lnxi_zrt`y')
			gen double product`y'		= (emp`y'*xi_zr`y')^((`eta'+1)/`eta')
			gegen double Sum`y'			= sum(product`y'), by(mmc )
		}
		
		gen double chng_Lr = (`eta'/(`eta'+1))*(ln(Sum1997) - ln(Sum1991) )
		
		keep mmc  chng_Lr
		gduplicates drop
		
		tempfile chng_Lr
		sa `chng_Lr'
		
		***** Bring in shocks at market-level *****
		preserve
			u "${monopsonies}/sas/regsfile_mmc_none.dta", clear
			keep mmc  ice_`tariff' 
			
			gduplicates drop
			tempfile market
			sa `market'
		restore
		
		merge 1:1 mmc  using `market', keep(1 3) nogen
		
		****** Get pre-shock trend in employment to de-trend chng_Lr ****
		preserve
			u "${monopsonies}/sas/regsfile_mmc_none.dta", clear
			keep if year==`baseyear'
			keep mmc  ice_`tariff'
			tempfile shock
			sa `shock'
		restore
		preserve
			u "${monopsonies}/sas/regsfile_mmc_none.dta", clear
			keep if inlist(year,`baseyear_p1',`baseyear')
			keep mmc  year mkt_emp
			gen lnmkt_emp = ln(mkt_emp)
			drop mkt_emp
			reshape wide lnmkt_emp, i(mmc ) j(year)
			
			************ Control for fitted linear pre-trend ************	
			gen double chng_Lr =  lnmkt_emp`baseyear_p1' - lnmkt_emp`baseyear'
			keep mmc  chng_Lr
			
			gen period = "pre"
			
			** Add chng_Lr: wage changes for the subsequent years ***
			append using `chng_Lr'
			replace period = "post" if missing(period)
			
			merge m:1 mmc  using `shock', keep(3) nogen
			
			tab period
			sum chng_Lr if period=="post"
	
			** Fit Pre-trend on pre-years ***
			reg chng_Lr ice_`tariff' if period=="pre"
			predict chng_Lr_d, resid		/* Extrapolate to later year long difference */
			
			keep if period == "post"
			keep mmc  chng_Lr_d
			
			sum chng_Lr_d, detail
			
			gduplicates drop
			tempfile trend_emp
			sa `trend_emp'
		restore
		
		* Merge in trend
		merge 1:1 mmc  using `trend_emp', keep(3) nogen
		
		*** Merge in delta_r from changes regression ****
		preserve
			u "${monopsonies}/dta/coeffs/`etachangedate'/eta_robustness_mmc_change_delta_r.dta", clear
			keep if sample=="``esamp''" & model=="l" & spec=="l" & wagevartype=="`wage'" & tariff=="lnT" & outyear==1997 & clust=="`etaclustmain'" & weight=="`eweight'"
				
			keep mmc  delta_r
			
			gduplicates drop
			isid  mmc  
			tempfile resids
			sa `resids'
		restore
		
		merge 1:1 mmc  using `resids', keep(3) nogen
		
		** Keep delta_r for detrending it ***
		preserve
			keep mmc  ice_`tariff' delta_r
			duplicates drop
			gen period = "post"
			
			ren delta_r Ddprem_r
			tempfile delta_r
			sa `delta_r'
		restore
		
		* Get pre-shock trend in wage as well
		local prebase = `baseyear'-1
		preserve
			keep mmc  ice_`tariff'
			gduplicates drop
			merge 1:m mmc  using "${monopsonies}/sas/rais_lnearn_premia_mmc_`premiadate'.dta", keep(3) nogen
			keep mmc  year dprem_r ice_`tariff'
			
			keep if inlist(year,`baseyear_p1',`baseyear')	
			reshape wide dprem_r, i(mmc ) j(year)
			
			************ Control fo fitted linear pre-trend ************	
			gen double Ddprem_r = dprem_r`baseyear_p1' - dprem_r`baseyear'
			keep mmc  ice_`tariff' Ddprem_r
			
			gen period = "pre"
			
			** Add delta_r: wage changes for the subsequent years ***
			append using `delta_r'
			
			** Fit Pre-trend on pre-years ***
			reg Ddprem_r ice_`tariff' if period=="pre"
			predict delta_r_d, resid		/* Extrapolate to later year long difference */
			
			keep if period == "post"
			keep mmc  delta_r_d

			gduplicates drop
			tempfile trend_w
			sa `trend_w'
		restore
		
		* Merge in trend
		merge 1:1 mmc  using `trend_w', keep(3) nogen
		
		foreach shock in `tariff'{
			replace ice_`shock' = 0 if missing(ice_`shock')
			replace ice_`shock' = -ice_`shock'
		}
	
		* Get weights *
		preserve
			u "${monopsonies}/sas/regsfile_mmc_none.dta", clear
			keep if year==`baseyear'
			keep mmc  mkt_emp
			ren mkt_emp w0
			tempfile W
			sa `W'
		restore	
		merge 1:1 mmc  using `W', keep(3) nogen
		
		*************************************
		****** theta change regression ******
		*************************************
		
		* All variables are aggregates at the market level including all firms in the market
		gen all = 1
		local mmc 		"mmc"
		
		tempfile forregs
		sa `forregs'
		
		clear
		
		foreach weight in `thetaweights'{
		foreach chng_Lr in `rhs'{
		foreach pclust in `thetaclust'{
		foreach delta in `lhs'{
		
			if "`delta'"=="delta_r"{
				local d = "d"
			}
			else{
				local d "dd"
			}
			
			if "`chng_Lr'"=="chng_Lr"{
				local c = "c"
			}
			else{
				local c "cd"
			}
		
			u `forregs', clear
			
			ivreg2 `delta' (`chng_Lr' = `inst') [w=`weight'], savefirst saverf cluster(``pclust'') 
			local diff_se = _se[`chng_Lr']
			
			local obs = e(N)
			unique  mmc if e(sample)
			local mkts = `r(unique)'
			
			local m_iv_b = _b[`chng_Lr']
			local m_iv_se = _se[`chng_Lr']
			mat FSstats = e(first)
			local FS_F = FSstats[4,1]
			
			di "`eta_inverse' + `m_iv_b'"
			local theta_inverse_b = `eta_inverse' + `m_iv_b'
			
			di "`theta_inverse_b'"
			local theta_inverse_se = sqrt(`diff_se'^2 - (`eta_inverse_se')^2)
			
			estimates restore _ivreg2_`delta'
			local m_rf_b = _b[`inst']
			local m_rf_se = _se[`inst']
			estimates restore _ivreg2_`chng_Lr'
			local m_fs_b = _b[`inst']
			local m_fs_se = _se[`inst']
			
			
			*OLS 
			reghdfe `delta' `chng_Lr' [w=`weight'], absorb(all) cluster(``pclust'') 
			
			local m_ols_b = _b[`chng_Lr']
			local m_ols_se = _se[`chng_Lr']
			
			***** Compute elasticity (Note this so far ignores the SE in eta_inverse estimation) *****
			
			mat coeffs = (	`m_ols_b',`m_ols_se',`m_iv_b',`m_iv_se', ///
							`m_rf_b',`m_rf_se',`m_fs_b',`m_fs_se', ///
							`FS_F',`theta_inverse_b', `theta_inverse_se',`obs',`mkts')
		
			**** Export residuals and coefficients *****
			clear
			svmat coeffs
			gen year 		= 1997
			gen tariff  	= "`tariff'"
			gen etaclust 	= "`etaclustmain'"
			gen etaweight	= "`eweight'"
			gen thetaclust 	= "`pclust'"
			gen esamp		= "`esamp'"
			gen tsamp  		= "`tsamp'"
			gen wagevar 	= "`wage'"
			gen deltatype	= "`delta'"
			gen chng_Lrtype = "`chng_Lr'"
			gen weight		= "`weight'"
			
			ren coeffs1 ols_b
			ren coeffs2 ols_se
			ren coeffs3 iv_b
			ren coeffs4 iv_se
			ren coeffs5 rf_b
			ren coeffs6 rf_se
			ren coeffs7 fs_b
			ren coeffs8 fs_se
			ren coeffs9 fs_F
			ren coeffs10 theta_inverse_b
			ren coeffs11 theta_inverse_se
			ren coeffs12 obs
			ren coeffs13 markets
			keep if !missing(ols_b)
			
			di "Now saving c`d'`pclust'`wage'`esamp'`tsamp'`c'`eweight'`weight'"
			tempfile  c`d'`pclust'`wage'`esamp'`tsamp'`c'`eweight'`weight'
			sa `c`d'`pclust'`wage'`esamp'`tsamp'`c'`eweight'`weight''

			
			} /* Close delta option */
			} /* Close chng_Lr */
			} /* Close pclust */
			} /* close weight */
		} /* Close eweight */
		} /* Close wage */
		} /* Close tsamp */
		} /* Close esamp */
		
		***************** Append all *****************
	
		u `cdmmclndpallallcallall', clear
		
		foreach weight in `thetaweights'{
		foreach chng_Lr in c cd{
		foreach esamp in `etasamp'{
		foreach tsamp in `thetasamp'{
		foreach wage in `wagevars'{
		foreach eweight in `etaweights'{
		foreach pclust in `thetaclust'{
		foreach delta in d dd{
			cap append using `c`delta'`pclust'`wage'`esamp'`tsamp'`chng_Lr'`eweight'`weight''
		}
		}
		}
		}
		}
		}
		}
		}
		
		outsheet using "${monopsonies}/csv/`outdate'/theta_robustness_mmc_change_regressions_simpler.csv", comma replace
		
}

