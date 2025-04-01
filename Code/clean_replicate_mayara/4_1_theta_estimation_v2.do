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


else if c(username)=="p13861161" & c(os)=="Windows" {
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\clean_replicate_mayara"
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\clean_replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/clean_replicate_mayara"
	global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/clean_replicate_mayara/monopsonies"
	global public			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/publicdata"
}


/*
do "${encrypted}/Felix_JMP/3_analysis/specs_config.do"
args spec
if "`spec'"=="" local spec "3states_gamma"
di "`spec'"

if "`spec'" == "" {
    display as error "Error: No spec provided. Please pass a spec (e.g., gamma, original, gamma_2)."
    exit 1
}
*/
cap log close
local date = subinstr("`c(current_date)'", " ", "_", .)
local time = subinstr("`c(current_time)'", ":", "_", .)
log using "${encrypted}/logs/4_1_theta_estimation_`spec'_`date'_`time'.log", replace



// Retrieve the market variables and file suffix based on the spec
local mkt "${s_`spec'_mv}"
local path "${s_`spec'_fs}"


local mkt "mmc cbo942d"
local path "mmc_cbo942d"

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

local getresiduals		= 0
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
	/*
	* XX Don't think we ever use these here
	u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
	isid year cnae10
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
	isid cnae95
	tempfile tariffs
	sa `tariffs'
	* contains vars cnae95 ibgesubsector lnT1990 lnE1990 lnT1994 lnE1994
	*/
	
	* Grab firm-market pairs in the eta_change regressions
	preserve
		u "${monopsonies}/sas/eta_changes_regsfile_`path'.dta", clear	
		isid year cnpj_raiz mmc cbo942d
		keep cnpj_raiz `mkt'  T //bemp up`baseyear_n' up`baseyear_n'mkt
		drop if inlist(cbo942d,31,22,37)
		gduplicates drop 
		tempfile pairs
		sa `pairs'
		* 4,113,686 firm-market pairs in this data
		* 28,573 unique markets
		* 792,688 unique firms
		* This is quite a bit more than actually end up in the eta regressions because in the eta regressions we 
		*   (i)   restrict to year==1997 and all==1
		*	(ii)  implicitly drop missing values of chng91_lndp, chng91_lnemp, and chng_lnT in the ivreg
		*	(iii) drop singletons in the regression
		* 	(iv)  drop if inlist(cbo942d,31,22,37)
		
	restore
	
	u "${monopsonies}/sas/rais_collapsed_firm_`path'.dta", clear
	keep if inlist(year,`baseyear',`baseyear_o2')
	
	* XXJSF This merge drops firm-market pairs that do not exist in the eta regressions data set. 
	*	Note that this does not drop firm-market pairs dropped by 	drop if inlist(cbo942d,31,22,37)
	*	or by singletons or by missing values of chng91_lndp, chng91_lnemp, and chng_lnT. However, we
	* 	seem to be doing ok b/c we are getting the same number of markets in the eta and theta regressions
	* XX Theoretically, are we sure we want to make this restriction? Stick with it for now.
	keep cnpj_raiz cnae95 `mkt' year emp avgdecearn avgmearn
	merge m:1 cnpj_raiz `mkt' using `pairs', keep(3) nogen
	

	* XXJSF Bring in firm-market average earnings and earnings premia created in 1_earnings_premia_combined.do. 
	*	Earnings premia (dprem_zro) computed by regressing log earnings on gender, age, education, and firm-market FEs 
	*	Average wages (davgw_zro) computed by regressing log earnings on firm-market FEs. We never actually use these (I think they are for robustness checks)
	merge 1:1 cnpj_raiz `mkt' year using "${monopsonies}/sas/rais_lnearn_premia_firm_`path'_`premiadate'.dta", ///
	keepusing(davgw_zro dprem_zro) keep(3) nogen
	
	ren davgw_zro 		lndw
	ren dprem_zro 		lndp
	gen double lnemp = ln(emp)
	
	/*
	* Get lnT1994 and lnT1990 for tradables firms
	merge m:1 cnae95 using `tariffs', keep(1 3) nogen
	*/
	* FEs
	/* XX Never used
	cap gegen fe_rot = group(`mkt' year)
	cap gegen fe_zro = group(cnpj_raiz `mkt')
	*/
	gegen double fe_ro = group(`mkt')
	
	gen double firm = cnpj_raiz
	
	gen all = 1
	//ren bemp w0
	
	/*
	cap ren up`baseyear_n' u`baseyear_n'
	cap ren up`baseyear_n'mkt u`baseyear_n'm
	local u91m 		"up91mkt"
	*/
	local all 		"all"
	

	//foreach weight in `etaweights'{
	//foreach wvar in `wagevars'{
	* XXJSF: I think what's happening here is:
	*	1. we started above with the set of firm-market wage premia that correspond to firm-market pairs used 
	*		in the eta regressions (not exactly tho b/c some more were subsequently dropped as discussed above).
	*	2. using these wage premia we residualize off market-year FEs to compute lndpres 
	* 	3. generate lnxi_zrot as the residualized wage premia minus (1/eta)*log employment. I think this is related
	*		to Mayara's eq (18) and will be an input to delta ln l_zm
	foreach year in 1991 1997 {

		//foreach esamp in `etasamp'{	
		
			* Get eta inverse
			preserve
				insheet using "${monopsonies}/csv/`etachangedate'/eta_change_regressions_`path'.csv", clear
				keep if samp=="all" & spec=="l"  & model=="l" & wagevar=="lndp" & tariff=="lnT" & year==`baseyear_o2' & clust=="firm" & weight=="all"
				levelsof iv_b, local(eta_inverse)
			restore
			
			gen double lhs = lndp - `eta_inverse'*lnemp
			
			* Remove market fixed effect from LHS
			qui areg lhs if all==1 & year==`year', absorb(fe_ro)
			predict lnxi_zrot, resid
			
			
			* Compute residuals
			preserve
				// XX keep if !missing(lndpres)
				keep if !missing(lnxi_zrot)
				
				keep cnpj_raiz `mkt' year lndp  lnemp lnxi_zrot //XX error  dwtrains_hf lndpres
				
				ren lndp 		wagevar
				//ren lndpres 	wagevarres
				keep if !missing(lnxi_zrot)
				gen samp  	 	= "all"
				gen wagevartype = "lndp"
				gen weight		= "all"
				
				order cnpj_raiz `mkt' year
				
				compress
				tempfile rall`year'lndpall
				sa `rall`year'lndpall'
			restore
			drop lhs lnxi_zrot

			//} /* Close sample */
	
	} /* Close year */
	//} /* Wage var */
	//} /* Close weight */
	

	** Append residuals and export ***
	u `rall`baseyear_o2'lndpall', clear
	foreach year in `baseyear' `baseyear_o2'{

		cap append using `rall`year'lndpall'
	}
	duplicates drop
	compress
	isid cnpj_raiz mmc cbo942d year
	saveold "${monopsonies}/dta/coeffs/`outdate'/lnxi_zrot_`path'.dta", replace
} /* Close eta regressions */


**********************************************************
***************** theta change regressions *****************
**********************************************************

if `theta_regs'==1{

	local lhs 		"delta_ro " // delta_ro_d
	local rhs 		"chng_Lro" // chng_Lro_d
	local tariff 	"dwtrains_hf"
	local inst 		"ice_dwtrains_hf"
	local u91m 		"up91mkt"
	local all 		"all"
	
	local thetasamp 	"all"

	//foreach tsamp in `thetasamp'{
	//foreach wage in `wagevars'{
	//foreach eweight in `etaweights'{
		
		
		*********************************************
		*********************************************
		* Compute chng_Lro (delta ln L_m), which is RHS for theta regs
		*********************************************
		*********************************************
		
		*** Get eta estimate: always use estimate for unique producers, weighted ***
		* Load and save eta estimates because will later estimate 1/theta-1/eta and back out theta
		insheet using "${monopsonies}/csv/`etachangedate'/eta_change_regressions_`path'.csv", clear
		keep if samp=="all" & spec=="l"  & model=="l" & wagevar=="lndp" & tariff=="lnT" & year==`baseyear_o2' & clust=="firm" & weight=="all"
		
		levelsof iv_b, local(eta_inverse)
		levelsof iv_se, local(eta_inverse_se)
		local eta = 1/`eta_inverse'

		* Get estimates of xis from above
		u "${monopsonies}/dta/coeffs/`etalevelsdate'/lnxi_zrot_`path'.dta", clear
		keep if samp=="all" & wagevartype=="lndp" & (year==1997 | year== 1991)  & weight=="all"
	
		keep cnpj_raiz `mkt' lnemp lnxi_zrot year

		reshape wide ln*, i(cnpj_raiz `mkt') j(year)
		
		* XXJSF I think this block is computing delta ln L_m as defined at the top of page 26 of Mayara's JMP. We 
		*	call this chng_Lro and it is the RHS var that is instrumented in the theta regression 
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
			keep `mkt' year ice_dwtrains_hf sum_e sum_e2 mkt_emp mkt_temp hf*
			keep if year==1991
			gduplicates drop
			tempfile market
			sa `market'
		restore
		
		merge 1:1 `mkt' using `market', keep(1 3) nogen
		pause
		****** Get pre-shock trend in employment to de-trend chng_Lro ****
		* XX I don't think we use this
		/*
		DELETED DETRENDING STUFF
		*/
		
		*** Merge in delta_ro from changes regression ****
		preserve
			u "${monopsonies}/dta/coeffs/`etachangedate'/eta_change_delta_ro_`path'.dta", clear
			keep if sample=="all" & model=="l" & spec=="l" & wagevartype=="lndp" & tariff=="lnT" & outyear==1997 & clust=="firm" & weight=="all"
				
			keep `mkt' delta_ro
			
			gduplicates drop
			isid  `mkt' 
			tempfile resids
			sa `resids'
		restore
		
		merge 1:1 `mkt' using `resids', keep(3) nogen

	/*
	DELETED DETRENDING STUFF
	*/
		replace ice_dwtrains_hf = 0 if missing(ice_dwtrains_hf)
		replace ice_dwtrains_hf = -ice_dwtrains_hf
	
	
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
		
			u `forregs', clear
			
			
			di "*****************************************************************"
			di "*****************************************************************"
			di "This coefficient is 1/theta - 1/eta"
			di "*****************************************************************"
			di "*****************************************************************"
			di "tsamp: all"
			di "wage: lndp"  
			di "eweight: all"
			di "weight: all"
			di "chng_Lro: chng_Lro"
			di "pclust: fe_ro"
			di "delta: delta_ro"
			
			
			noi ivreg2 delta_ro (chng_Lro = ice_dwtrains_hf) [w=all], savefirst saverf cluster(fe_ro) 
			local diff_se = _se[chng_Lro]

			pause
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
