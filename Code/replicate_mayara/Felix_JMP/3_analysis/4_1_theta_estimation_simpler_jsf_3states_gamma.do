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

else if c(username)=="p13861161" & c(os)=="Windows" {
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara"
	global dictionaries		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara"
	global dictionaries		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdictionaries/harmonized"
	global deIDrais			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdeidentified"
	global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies"
	global public			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/publicdata"
}


do "${encrypted}/Felix_JMP/3_analysis/specs_config.do"
args spec
if "`spec'"=="" local spec "3states_original"
di "`spec'"

if "`spec'" == "" {
    display as error "Error: No spec provided. Please pass a spec (e.g., gamma, original, gamma_2)."
    exit 1
}

cap log close
local date = subinstr("`c(current_date)'", " ", "_", .)
local time = subinstr("`c(current_time)'", ":", "_", .)
log using "${encrypted}/logs/4_1_theta_estimation_`spec'_`date'_`time'.log", replace



// Retrieve the market variables and file suffix based on the spec
local mkt "${s_`spec'_mv}"
local path "${s_`spec'_fs}"

display "Using market variables: `mkt'"
display "Using path suffix: `path'"





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

local getresiduals		= 1
local theta_regs 		= 1
	
* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"

local etaclustmain 	"firm"	/* Code does not allow for multiple */
local etaweights	"all" // w0"
local etasamp		"all" // u91m"
local tarvars 		"lnT"

local thetaclust 	" fe_ro" // 2way
local thetaweights 	"all" // w0"

local wagevars " lndp" // lndw


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
		u "${monopsonies}/sas/eta_changes_regsfile_`path'.dta", clear			
		keep fakeid_firm `mkt' up`baseyear_n' up`baseyear_n'mkt T //bemp
		cap drop if inlist(cbo942d,31,22,37)
		gduplicates drop 
		tempfile pairs
		sa `pairs'
	restore
	
	u "${monopsonies}/sas/rais_collapsed_firm_`path'.dta", clear
	keep if inlist(year,`baseyear',`baseyear_o2')
	
	keep fakeid_firm cnae95 `mkt' year emp avgdecearn avgmearn
	merge m:1 fakeid_firm `mkt' using `pairs', keep(3) nogen
		
	* Bring in earnings premia
	merge 1:1 fakeid_firm `mkt' year using "${monopsonies}/sas/rais_lnearn_premia_firm_`path'_`premiadate'.dta", ///
	keepusing(davgw_zro dprem_zro) keep(3) nogen
	
	ren davgw_zro 		lndw
	ren dprem_zro 		lndp
	gen double lnemp = ln(emp)
	
	* Get lnT1994 and lnT1990 for tradables firms
	merge m:1 cnae95 using `tariffs', keep(1 3) nogen
	
	* FEs
	cap gegen fe_rot = group(`mkt' year)
	cap gegen fe_zro = group(fakeid_firm `mkt')
	
	gegen double fe_ro = group(`mkt')
	
	gen double firm = fakeid_firm
	
	gen all = 1
	//ren bemp w0
	
	cap ren up`baseyear_n' u`baseyear_n'
	cap ren up`baseyear_n'mkt u`baseyear_n'm
	
	local u91m 		"up91mkt"
	local all 		"all"
	
	foreach weight in `etaweights'{
	foreach wvar in `wagevars'{
	foreach year in `baseyear' `baseyear_o2'{

		foreach esamp in `etasamp'{	
		
			* Remove market fixed effect from LHS
			qui areg `wvar'  if `esamp'==1 & year==`year', absorb(fe_ro)
			predict `wvar'res, resid
			
			* Get eta inverse
			
			preserve
				insheet using "${monopsonies}/csv/`etachangedate'/eta_change_regressions_`path'.csv", clear
				keep if samp=="``esamp''" & spec=="l"  & model=="l" & wagevar=="`wvar'" & tariff=="lnT" & year==`baseyear_o2' & clust=="`etaclustmain'" & weight=="`weight'"
		
				levelsof iv_b, local(eta_inverse)
			restore
			
			* Compute residuals
			preserve
				keep if !missing(`wvar'res)
				gen double lnxi_zrot = `wvar'res - `eta_inverse'*lnemp

				keep fakeid_firm `mkt' year `wvar' `wvar'res lnemp lnxi_zrot //XX error  `tariff'
				
				ren `wvar' 		wagevar
				ren `wvar'res 	wagevarres
				keep if !missing(lnxi_zrot)
				gen samp  	 	= "`esamp'"
				gen wagevartype = "`wvar'"
				gen weight		= "`weight'"
				
				order fakeid_firm `mkt' year
				
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
	saveold "${monopsonies}/dta/coeffs/`outdate'/lnxi_zrot_`path'.dta", replace
} /* Close eta regressions */


**********************************************************
***************** theta change regressions *****************
**********************************************************

if `theta_regs'==1{

	local lhs 		"delta_ro " // delta_ro_d
	local rhs 		"chng_Lro" // chng_Lro_d
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
		insheet using "${monopsonies}/csv/`etachangedate'/eta_change_regressions_`path'.csv", clear
		keep if samp=="``esamp''" & spec=="l"  & model=="l" & wagevar=="`wage'" & tariff=="lnT" & year==`baseyear_o2' & clust=="`etaclustmain'" & weight=="`eweight'"
		
		levelsof iv_b, local(eta_inverse)
		levelsof iv_se, local(eta_inverse_se)
		local eta = 1/`eta_inverse'

		* Get estimates of xis 
		u "${monopsonies}/dta/coeffs/`etalevelsdate'/lnxi_zrot_`path'.dta", clear
		keep if samp=="`tsamp'" & wagevartype=="`wage'" & (year==1997 | year== 1991)  & weight=="`eweight'"
	
		keep fakeid_firm `mkt' lnemp lnxi_zrot year

		reshape wide ln*, i(fakeid_firm `mkt') j(year)
		 
		foreach y in 1997 1991 {
			gen double emp`y' 			= exp(lnemp`y')
			gen double xi_zro`y' 		= exp(lnxi_zrot`y')
			gen double product`y'		= (emp`y'*xi_zro`y')^((`eta'+1)/`eta')
			gegen double Sum`y'			= sum(product`y'), by(`mkt')
		}
		
		gen double chng_Lro = (`eta'/(`eta'+1))*(ln(Sum1997) - ln(Sum1991) )
		
		keep `mkt' chng_Lro
		gduplicates drop
		
		tempfile chng_Lro
		sa `chng_Lro'
		
		***** Bring in shocks at market-level *****
		preserve
			u "${monopsonies}/sas/regsfile_`path'.dta", clear
			ren ice_dwTRAINS_Hf ice_dwtrains_hf // XX
			keep `mkt' ice_`tariff' 
			
			gduplicates drop
			tempfile market
			sa `market'
		restore
		
		merge 1:1 `mkt' using `market', keep(1 3) nogen
		
		****** Get pre-shock trend in employment to de-trend chng_Lro ****
		preserve
			u "${monopsonies}/sas/regsfile_`path'.dta", clear
			ren ice_dwTRAINS_Hf ice_dwtrains_hf // XX
			keep if year==`baseyear'
			keep `mkt' ice_`tariff'
			tempfile shock
			sa `shock'
		restore
		preserve
			u "${monopsonies}/sas/regsfile_`path'.dta", clear
			ren ice_dwTRAINS_Hf ice_dwtrains_hf // XX
			keep if inlist(year,`baseyear_p1',`baseyear')
			keep `mkt' year mkt_emp
			gen lnmkt_emp = ln(mkt_emp)
			drop mkt_emp
			reshape wide lnmkt_emp, i(`mkt') j(year)
			
			************ Control for fitted linear pre-trend ************	
			gen double chng_Lro =  lnmkt_emp`baseyear_p1' - lnmkt_emp`baseyear'
			keep `mkt' chng_Lro
			
			gen period = "pre"
			
			** Add chng_Lro: wage changes for the subsequent years ***
			append using `chng_Lro'
			replace period = "post" if missing(period)
			
			merge m:1 `mkt' using `shock', keep(3) nogen
			
			tab period
			sum chng_Lro if period=="post"
	
			** Fit Pre-trend on pre-years ***
			reg chng_Lro ice_`tariff' if period=="pre"
			predict chng_Lro_d, resid		/* Extrapolate to later year long difference */
			
			keep if period == "post"
			keep `mkt' chng_Lro_d
			
			sum chng_Lro_d, detail
			
			gduplicates drop
			tempfile trend_emp
			sa `trend_emp'
		restore
		
		* Merge in trend
		merge 1:1 `mkt' using `trend_emp', keep(3) nogen
		
		*** Merge in delta_ro from changes regression ****
		preserve
			u "${monopsonies}/dta/coeffs/`etachangedate'/eta_change_delta_ro_`path'.dta", clear
			keep if sample=="``esamp''" & model=="l" & spec=="l" & wagevartype=="`wage'" & tariff=="lnT" & outyear==1997 & clust=="`etaclustmain'" & weight=="`eweight'"
				
			keep `mkt' delta_ro
			
			gduplicates drop
			isid  `mkt' 
			tempfile resids
			sa `resids'
		restore
		
		merge 1:1 `mkt' using `resids', keep(3) nogen
		
		** Keep delta_ro for detrending it ***
		preserve
			keep `mkt' ice_`tariff' delta_ro
			duplicates drop
			gen period = "post"
			
			ren delta_ro Ddprem_ro
			tempfile delta_ro
			sa `delta_ro'
		restore
		
		* Get pre-shock trend in wage as well
		local prebase = `baseyear'-1
		preserve
			keep `mkt' ice_`tariff'
			gduplicates drop
			merge 1:m `mkt' using "${monopsonies}/sas/rais_lnearn_premia_`path'_`premiadate'.dta", keep(3) nogen
			keep `mkt' year dprem_ro ice_`tariff'
			
			keep if inlist(year,`baseyear_p1',`baseyear')	
			reshape wide dprem_ro, i(`mkt') j(year)
			
			************ Control fo fitted linear pre-trend ************	
			gen double Ddprem_ro = dprem_ro`baseyear_p1' - dprem_ro`baseyear'
			keep `mkt' ice_`tariff' Ddprem_ro
			
			gen period = "pre"
			
			** Add delta_ro: wage changes for the subsequent years ***
			append using `delta_ro'
			
			** Fit Pre-trend on pre-years ***
			reg Ddprem_ro ice_`tariff' if period=="pre"
			predict delta_ro_d, resid		/* Extrapolate to later year long difference */
			
			keep if period == "post"
			keep `mkt' delta_ro_d

			gduplicates drop
			tempfile trend_w
			sa `trend_w'
		restore
		
		* Merge in trend
		merge 1:1 `mkt' using `trend_w', keep(3) nogen
		
		foreach shock in `tariff'{
			replace ice_`shock' = 0 if missing(ice_`shock')
			replace ice_`shock' = -ice_`shock'
		}
	
		* Get weights *
		preserve
			u "${monopsonies}/sas/regsfile_`path'.dta", clear
			keep if year==`baseyear'
			keep `mkt' mkt_emp
			ren mkt_emp w0
			tempfile W
			sa `W'
		restore	
		merge 1:1 `mkt' using `W', keep(3) nogen
		
		*************************************
		****** theta change regression ******
		*************************************
		
		* All variables are aggregates at the market level including all firms in the market
		gen all = 1
		gegen double fe_ro = group(`mkt')
		local 2way 		"gamma"
		local mmc 		"mmc"
		local cbo942d  	"cbo942d"
		local fe_ro 	"fe_ro"
		
		tempfile forregs
		sa `forregs'
		
		clear
		
		foreach weight in `thetaweights'{
		foreach chng_Lro in `rhs'{
		foreach pclust in `thetaclust'{
		foreach delta in `lhs'{
			local i = `i'+1
			if "`delta'"=="delta_ro"{
				local d = "d"
			}
			else{
				local d "dd"
			}
			
			if "`chng_Lro'"=="chng_Lro"{
				local c = "c"
			}
			else{
				local c "cd"
			}
		
			u `forregs', clear
			
			
			
			di "*****************************************************************"
			di "*****************************************************************"
			di "This coefficient is 1/theta - 1/eta"
			di "*****************************************************************"
			di "*****************************************************************"
			di "iteration: `i'"
			di "tsamp: `tsamp'"
			di "wage: `wage'"  
			di "eweight: `eweight'"
			di "weight: `weight'"
			di "chng_Lro: `chng_Lro'"
			di "pclust: `pclust'"
			di "delta: `delta'"
			/*
		
Looks like we get 17637 obs when using chng_Lro and delta_ro. If either is _d we get 15418
		
iteration: 1
tsamp: all
wage: lndp
eweight: all
weight: all
chng_Lro: chng_Lro
pclust: fe_ro
delta: delta_ro


iteration: 2
tsamp: all
wage: lndp
eweight: all
weight: all
chng_Lro: chng_Lro
pclust: fe_ro
delta: delta_ro_d

iteration: 3
tsamp: all
wage: lndp
eweight: all
weight: all
chng_Lro: chng_Lro_d
pclust: fe_ro
delta: delta_ro


iteration: 4
tsamp: all
wage: lndp
eweight: all
weight: all
chng_Lro: chng_Lro_d
pclust: fe_ro
delta: delta_ro_d

*/
	
			
			noi ivreg2 delta_ro (chng_Lro = ice_dwtrains_hf) [w=all], savefirst saverf cluster(fe_ro) 
			local diff_se = _se[chng_Lro]

		* XX why are these not the same? What is being dropped?
		count if e(sample)
		count
		
			local obs = e(N)
			unique `mkt' if e(sample)
			local mkts = `r(unique)'
			
			local m_iv_b = _b[chng_Lro]
			local m_iv_se = _se[chng_Lro]
			mat FSstats = e(first)
			local FS_F = FSstats[4,1]
			
			di "eta_inverse"
			di "`eta_inverse'"
			
			di "eta_inverse + m_iv_b"
			di "`eta_inverse' + `m_iv_b'"
			local theta_inverse_b = `eta_inverse' + `m_iv_b'
			
			di "theta_inverse_b"
			di "`theta_inverse_b'"
			local theta_inverse_se = sqrt(`diff_se'^2 - (`eta_inverse_se')^2)
			
			estimates restore _ivreg2_delta_ro
			local m_rf_b = _b[ice_dwtrains_hf]
			local m_rf_se = _se[ice_dwtrains_hf]
			estimates restore _ivreg2_chng_Lro
			local m_fs_b = _b[ice_dwtrains_hf]
			local m_fs_se = _se[ice_dwtrains_hf]
			
			
			*OLS 
			reghdfe  delta_ro chng_Lro [w=all], absorb(all) cluster(fe_ro) 
			
			local m_ols_b = _b[chng_Lro]
			local m_ols_se = _se[chng_Lro]
			
			***** Compute elasticity (Note this so far ignores the SE in eta_inverse estimation) *****
			
			mat coeffs = (	`m_ols_b',`m_ols_se',`m_iv_b',`m_iv_se', ///
							`m_rf_b',`m_rf_se',`m_fs_b',`m_fs_se', ///
							`FS_F',`theta_inverse_b', `theta_inverse_se',`obs',`mkts')
		
			**** Export residuals and coefficients *****
			clear
			svmat coeffs
		
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
			
			outsheet using "${monopsonies}/csv/`outdate'/theta_change_regressions_simpler_clean_`path'.csv", comma replace
			outsheet using "${monopsonies}/csv/`outdate'/theta_change_regressions_simpler_clean_`path'_`date'_`time'.csv", comma replace

			} /* Close delta option */
			} /* Close chng_Lro */
			} /* Close pclust */
			} /* close weight */
		} /* Close eweight */
		} /* Close wage */
		} /* Close tsamp */
		} /* Close esamp */
		
		***************** Append all *****************

		
		
		
}

log close

/*
eta_inverse
1.532351970672607
eta_inverse + m_iv_b
1.532351970672607 + -.9615015686163621
theta_inverse_b
.5708504020562449

*/
