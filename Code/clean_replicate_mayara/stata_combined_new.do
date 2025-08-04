version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154

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
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies"
	global public			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/publicdata"
}

* Set ${outdate} macro
global outdate = 20250804

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/${outdate}"
cap mkdir "${monopsonies}/eps/${outdate}"
cap mkdir "${monopsonies}/dta/coeffs/${outdate}"

* XXBMS -- not sure what this does, but we need to be able to specify $mkt = gamma and $mkt = mmc cbo942d ... 
do "${encrypted}/Felix_JMP/3_analysis/specs_config.do"
args spec
di "`spec'"
if "`spec'"=="" local spec "3states_original"
di "`spec'"

if "`spec'" == "" {
    display as error "Error: No spec provided. Please pass a spec (e.g., gamma, original, gamma_2)."
    exit 1
}

cap log close
local date = subinstr("`c(current_date)'", " ", "_", .)
local time = subinstr("`c(current_time)'", ":", "_", .)
log   using "${encrypted}/logs/stata_combined_new_`spec'_`date'_`time'.log", replace

* Retrieve the market variables and file suffix based on the spec
global mkt 		"${s_`spec'_mv}"
global path 		"${s_`spec'_fs}"
global g3states 	"${s_`spec'_3s}"
local tool 		""

display "Using market variables: ${mkt}"
display "Using path suffix: ${path}"

* XXBMS can we export this file "${monopsonies}/sas/regsfile_${path}.dta",
* 	with all defs of delta_ice, denominators, and HHI by market? 
* XXBMS Also need to see if we can re-create the "two-way collapse" with counts  
*	of employees/firms by gamma, mmc, and cbo942d



******************************************************************************** 
*** 1_3_earnings_premia_gamma_firm.do
******************************************************************************** 

	foreach y in 1988 1991 1997 {		
		u "${monopsonies}/sas/rais_for_earnings_premia`y'_gamma${g3states}.dta", clear
		isid fakeid_worker
		drop if mmc==13007 | mmc==23014
		
		* Keep if education category is well-defined
		keep if !missing(educ)
		
		* Keep only onbs with non-zero dec earnings
		keep if earningsdecmw>0
		
		/* Use same control and ranges as DK */
		gen age1 = (agegroup==3)
		gen age2 = (agegroup==4)
		gen age3 = (agegroup==5)
		gen age4 = (agegroup==6)
		gen age5 = (agegroup==7)
		
		gen double lndecearn = ln(earningsdecmw)
		
		* Use same categories as DK
		forvalues i=1/8{
			gen educ`i' = (educ == `i')
		}
		gen educ9 = (educ >=9)
		
		
		
		gegen double fe_ro = group(`mkt')
		
		****************** MMC premia (incl. industry) ****************
		reghdfe lndecearn female age2-age5 educ2-educ9, absorb(dprems_ro=fe_ro ibgesubsector) noconstant
		
		****************** MMC average wage ****************
		reghdfe lndecearn, absorb(davgw_ro=fe_ro) noconstant
		
		****************** MMC premia (excl. industry) ****************
		reghdfe lndecearn female age2-age5 educ2-educ9, absorb(dprem_ro=fe_ro) noconstant
			

		preserve
		keep if !missing(dprem_ro) 
		keep ${mkt} dprem_ro davgw_ro
		gen year = `y'

		gduplicates drop

		compress
		tempfile mkt`y'
		sa `mkt`y''
		restore
		
		gegen double fe_zro = group(fakeid_firm ${mkt})
		
		****************** MMC average wage ****************
		reghdfe lndecearn, absorb(davgw_zro=fe_zro) noconstant keepsingletons
		
		****************** MMC premia (excl. industry) ****************
		reghdfe lndecearn female age2-age5 educ2-educ9 , absorb(dprem_zro=fe_zro) noconstant keepsingletons
			
		keep if !missing(dprem_zro) 
		keep fakeid_firm ${mkt} dprem_zro davgw_zro
		gen year = `y'

		gduplicates drop

		compress
		tempfile firm`y'
		sa `firm`y''
	} 
	
	*********** Append across years *******	
	u 			 `firm1988', clear
	append using `firm1991'
	append using `firm1997'
	
	gduplicates drop
	compress
	saveold "${monopsonies}/sas/rais_lnearn_premia_firm_${path}_${outdate}.dta", replace
	
	u 			 `mkt1988', clear
	append using `mkt1991'
	append using `mkt1997'
	
	gduplicates drop
	compress
	saveold "${monopsonies}/sas/rais_lnearn_premia_${path}_${outdate}.dta", replace
	

	
******************************************************************************** 
*** 3_1_eta_estimation_gamma.do
******************************************************************************** 

	u "${public}/Tariffs/tariffs_maindataset_long.dta", clear
	ren cnae10 cnae95
	keep if year==1990 | year==1994
	keep if !missing(TRAINS)
	keep year cnae95 ibgesubsector TRAINS ErpTRAINS
	sort cnae95 year
	foreach var of varlist TRAINS ErpTRAINS{
		replace `var' = `var'/100
		bys cnae95: gen double chng_ln`var' = ln(1+`var') - ln(1+`var'[_n-1])
	}
	keep if year==1994
	keep cnae95 ibgesubsector chng*
	gduplicates drop
	tempfile t_change
	sa `t_change'
	sa t_change, replace

	u "${monopsonies}/sas/regsfile_${path}.dta", clear
	isid ${mkt} year
	keep if year==1991
	ren mkt_emp bemp
	ren (ice_dwErpTRAINS ice_dwTRAINS)(ice_dwerp ice_dwtrains)
	keep ${mkt} ice_dwerp ice_dwtrains bemp
	gduplicates drop
	tempfile market
	sa `market'

	u "${monopsonies}/sas/rais_collapsed_firm_${path}.dta", clear
	 destring ibgesubsector , replace
	isid fakeid_firm ${mkt} year
	keep if inlist(year,1991,1997)
	
	* Bring in earnings premia
	merge 1:1 fakeid_firm ${mkt} year using "${monopsonies}/sas/rais_lnearn_premia_firm_${path}_${outdate}.dta", ///
				keepusing(dprem_zro davgw_zro) keep(3) nogen
	isid 	  fakeid_firm ${mkt} year

	ren dprem_zro lndp
	ren davgw_zro lndw
	
	* Sample restrictions 

	* Drop certain mmcs
	// XX this doesn't seem to actually do anything?
	//drop if mmc==13007 
	//merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
	//drop if mmc_drop==1
	
	* Merge in tariffs so can also restrict to tradables
	merge m:1 cnae95 using `t_change', keep(1 3) 
	* XX My hypothesis is that most non-tradable sectors are excluded from the data and thus we need to impute these zeroes. 
	//replace chng_lnTRAINS = 0 if _merge==1
	
	* Tradable sector dummies (ibgesub only included in TRAINS data at this point)
	gegen 	ibge				= max(ibgesubsector), by(fakeid_firm)
	gen 	T 					= (ibge<14 | ibge==25)	
	replace chng_lnTRAINS 		= 0 if T==0
	replace chng_lnErpTRAINS 	= 0 if T==0
		
	drop ibgesubsector _merge
		
		
	* Compute firm baseline share
	preserve
		 keep if year == 1991
		 gegen double sumearn  = sum(totmearn), 			by(${mkt})
		 gen   double bwshare  = totmearn/sumearn
		 gegen double sumemp   = sum(emp), 					by(${mkt})
		 gen   double beshare  = emp/sumemp

		 gegen double Tsumearn = sum(totmearn) 	   if T==1, by(${mkt})
		 gen   double bTwshare = totmearn/Tsumearn if T==1
		 gegen double Tsumemp  = sum(emp)	 	   if T==1, by(${mkt})
		 gen   double bTeshare = emp/Tsumemp       if T==1
		 
		 keep fakeid_firm ${mkt} beshare bwshare bTeshare bTwshare 
		 tempfile shares
		 sa `shares'
	restore

	* Merge in base shares, replacing with zero if firm was not there
	merge m:1 fakeid_firm ${mkt} using `shares', keep(1 3) nogen

	* Dummy for whether firm-market pair existed in 1991
	gen double in91mkt = !missing(beshare)

	* Dummy for whether firm exited in 1991
	gegen in91 = max(in91mkt), by(fakeid_firm)

	foreach var of varlist beshare bwshare bTeshare bTwshare {
		replace `var' = 0 if missing(`var')
	}

	* Keep firms that were there in 1991 (but ok if firms enters new markets)
	keep if in91==1 // XXBMS this seems like an important keep statement 

	* Flag Unique producers of each cnae95 within each market
	preserve
		keep if year == 1991 & T == 1
		keep fakeid_firm cnae95 ${mkt}
		bys cnae95 ${mkt}: gegen producers = count(fakeid_firm)
		keep if producers == 1
		keep fakeid_firm ${mkt}
		
		gen up91 = 1
		tempfile unique 
		sa `unique'
	restore	
	merge m:1 fakeid_firm ${mkt} using `unique', keep(1 3) nogen
	replace up91 = 0 if missing(up91)

	* Market that has unique producer 
	gegen up91mkt_temp = max(up91), by(${mkt})
	gen double up91mkt = up91==1 | (up91mkt_temp==1 & T==0)
	drop up91mkt_temp
	
	* Merge in market shocks
	merge m:1 ${mkt} using `market', keep(3) nogen

	* Log employment
	gen double lnemp = ln(emp)
	gegen fe_zro = group(fakeid_firm ${mkt})

	di "Long differences"
	preserve
		keep  fe_zro year lnemp lndp lndw
		xtset fe_zro year
		
		foreach var of varlist lnemp lndp lndw { 
			* Difference back to base year
			gegen double `var'91 	= max(cond(year==1991,`var',.)), by(fe_zro)
			gen double chng91_`var' = `var' - `var'91
		}
		
		keep if inlist(year,1991,1997)
		keep year fe_zro chng*

		tempfile long
		sa `long'
	restore
	
	di "Keep changes and baseyear info only"
	keep if inlist(year,1991,1997)
	merge m:1 year fe_zro using `long', keep(1 3) nogen
	
	keep 	fakeid_firm ${mkt} year cnae95 ///
			fe_zro bTwshare bTeshare  bwshare  beshare   ///
			chng* up91 up91mkt T bemp ice_dwtrains
	
	order	year fe_zro fakeid_firm ${mkt} cnae95 up91 up91mkt T bemp 
			
	compress
	isid fakeid_firm year ${mkt}
	saveold "${monopsonies}/sas/eta_changes_regsfile_${path}_${outdate}.dta", replace 
	
    * run the regressions for eta
	u "${monopsonies}/sas/eta_changes_regsfile_${path}_${outdate}.dta", clear
	keep if year == 1997

	//drop if inlist(cbo942d,31,22,37) // XXBMS do we want to drop these here? 
	
	* Cross-section FEs
	gegen fe_ro = group(${mkt})
	
	ren 	chng_lnTRAINS 	 chng_lnT
	replace chng_lnT  	 = - chng_lnT
	replace ice_dwtrains = - ice_dwtrains
	
	gen double firm = fakeid_firm
	gen all		 	= 1
	ren bemp w0
	
	foreach weight in all w0 {

		ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) [w=`weight'], ///
						savefirst saverf cluster(firm) absorb(delta_ro = fe_ro) 
		gen eta_sample = e(sample)

		* Store fixed effects for theta estimation in next step
		preserve
			keep fakeid_firm ${mkt} chng91_lndp chng91_lnemp delta_ro
			keep if !missing(delta_ro)

			ren chng91_lndp  chng_wagevar
			ren chng91_lnemp chng_lnemp
			gen weight		= "`weight'"
			
			tempfile r`weight'
			sa 		`r`weight''
		restore
		drop delta_ro
	
		local  obs 			= e(N)
		unique fakeid_firm    if e(sample)
		local  firms 		= `r(unique)'
		unique ${mkt} 		  if e(sample)
		local  mkts 		= `r(unique)'
		local m_iv_b 		=  _b[chng91_lnemp]
		local m_iv_se 		= _se[chng91_lnemp]
		mat first 			= e(first)
		estimates restore _ivreg2_chng91_lndp
		local m_rf_b 		=  _b[chng_lnT]
		local m_rf_se 		= _se[chng_lnT]
		estimates restore _ivreg2_chng91_lnemp
		local m_fs_b 		=  _b[chng_lnT]
		local m_fs_se 		= _se[chng_lnT]
		local FS_F 			= first[4,1]
		macro list 	

		reghdfe chng91_lndp chng91_lnemp [w=`weight'], vce(cluster firm) absorb(fe_ro) 
		local m_ols_b  =  _b[chng91_lnemp]
		local m_ols_se = _se[chng91_lnemp]
		
		mat coeffs = (`m_ols_b',`m_ols_se',`m_iv_b',`m_iv_se', ///
					   `m_rf_b',`m_rf_se' ,`m_fs_b',`m_fs_se', ///
					  `FS_F',`obs',`firms',`mkts')
		preserve
			clear
			svmat coeffs
			gen weight	 = "`weight'"
			ren coeffs1  ols_b
			ren coeffs2  ols_se
			ren coeffs3  iv_b
			ren coeffs4  iv_se
			ren coeffs5  rf_b
			ren coeffs6  rf_se
			ren coeffs7  fs_b
			ren coeffs8  fs_se
			ren coeffs9  fs_F
			ren coeffs10 obs
			ren coeffs11 firms
			ren coeffs12 markets
			keep if !missing(ols_b)
	
			tempfile k`weight'
			sa 		`k`weight''
		restore		
		estimates clear
		
		
		* Save market-level regression statistics for collapse 
		preserve 
			keep   $mkt  eta_sample `weight' chng91_lndp chng91_lnemp chng_lnT
			bysort $mkt: egen mkt_eta_sample 	  = total(eta_sample)
			bysort $mkt: egen mkt_tot_wgt 	  	  = total(eta_sample*`weight')
			gen               wgt				  = eta_sample* `weight'/mkt_tot_wgt
			bysort $mkt: egen mkt_avg_lndp 	  	  = total(wgt * chng91_lndp )
			bysort $mkt: egen mkt_avg_lnemp    	  = total(wgt * chng91_lnemp)
			bysort $mkt: egen mkt_avg_lnT    	  = total(wgt * chng_lnT    )
			bysort $mkt: egen mkt_var_lndp 	  	  = total(wgt * (chng91_lndp  - mkt_avg_lndp )^2)
			bysort $mkt: egen mkt_var_lnemp    	  = total(wgt * (chng91_lnemp - mkt_avg_lnemp)^2)
			bysort $mkt: egen mkt_var_lnT    	  = total(wgt * (chng_lnT     - mkt_avg_lnT  )^2)
			bysort $mkt: egen mkt_cov_lndp_lnemp  = total(wgt * (chng91_lndp  - mkt_avg_lndp ) * (chng91_lnemp - mkt_avg_lnemp))
			bysort $mkt: egen mkt_cov_lndp_lnT    = total(wgt * (chng91_lndp  - mkt_avg_lndp ) * (chng_lnT     - mkt_avg_lnT  ))
			bysort $mkt: egen mkt_cov_lnemp_lnT   = total(wgt * (chng91_lnemp - mkt_avg_lnemp) * (chng_lnT     - mkt_avg_lnT  ))

			keep   $mkt mkt_* 
			ren    mkt_* * 
			duplicates drop
			outsheet using "${monopsonies}/csv/${outdate}/eta_reg_collapse_ew_`weight'_${path}.csv", comma replace
		restore
		drop eta_sample 
	} 
	
	** Append all main regression coefficients and export to csv ***
	u `kall', clear
	cap append using `kw0'
	duplicates drop
	outsheet using "${monopsonies}/csv/${outdate}/eta_change_regressions_${path}.csv", comma replace

	** Append all main regression coefficients and export to csv ***
	u `rall', clear
	cap append using `rw0'
	duplicates drop
	compress
	saveold "${monopsonies}/dta/coeffs/${outdate}/eta_change_delta_ro_${path}.dta", replace



******************************************************************************** 
*** 4_1_theta_estimation_simpler_jsf_3states_gamma.do
******************************************************************************** 

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
	
	keep 	year cnae95 ibgesubsector lnT lnE
	reshape wide lnT lnE, i(cnae95 ibgesubsector) j(year)
	
	tempfile tariffs
	sa `tariffs'
	
	* Grab firm-market pairs in the eta_change regressions
	preserve
		u "${monopsonies}/sas/eta_changes_regsfile_${path}_${outdate}.dta", clear			
		keep fakeid_firm ${mkt} up91 up91mkt T bemp
		cap drop if inlist(cbo942d,31,22,37) // XXBMS these cbos not dropped above for eta regressions... 
		gduplicates drop 
		tempfile pairs
		sa `pairs'
	restore
	
	u "${monopsonies}/sas/rais_collapsed_firm_${path}.dta", clear
	keep if   inlist(year,1991,1997)
	keep      fakeid_firm ${mkt} cnae95 year emp avgdecearn avgmearn
	merge m:1 fakeid_firm ${mkt} using `pairs', keep(3) nogen
		
	* Bring in earnings premia
	merge 1:1 fakeid_firm ${mkt} year using "${monopsonies}/sas/rais_lnearn_premia_firm_${path}_${outdate}.dta", ///
			keepusing(davgw_zro dprem_zro) keep(3) nogen
	
	ren davgw_zro 	lndw
	ren dprem_zro 	lndp
	gen double		lnemp = ln(emp)
	
	* Get lnT1994 and lnT1990 for tradables firms
	merge m:1 cnae95 using `tariffs', keep(1 3) nogen
	
	* FEs
	cap gegen 	fe_rot = group(${mkt} year)
	cap gegen 	fe_zro = group(fakeid_firm ${mkt})
	gegen double fe_ro = group(${mkt})
	
	gen double firm = fakeid_firm
	gen all = 1
	ren bemp w0
	
	foreach weight in all w0 {
		
		* Get eta inverse
		preserve
			insheet using "${monopsonies}/csv/${outdate}/eta_change_regressions_${path}.csv", clear
			keep if weight == "`weight'"
			levelsof iv_b, local(eta_inverse)
		restore
			
		foreach year in 1991 1997 {

			* Remove market fixed effect from LHS
			qui areg lndp if year==`year', absorb(fe_ro)
			predict  lndpres, resid

			* Compute residuals
			preserve
				keep if !missing(lndpres)
				gen double lnxi_zrot = lndpres - `eta_inverse'*lnemp

				keep fakeid_firm ${mkt} year lndp lndpres lnemp lnxi_zrot 
				
				ren lndp 		wagevar
				ren lndpres 	wagevarres
				keep if !missing(lnxi_zrot)
				gen weight		= "`weight'"
				order fakeid_firm ${mkt} year
				
				compress
				tempfile r`year'`weight'
				sa 		`r`year'`weight''
			restore
			drop lndpres
		} 
	} 
	
	** Append residuals and export ***
	u 			 `r1991all', clear
	append using `r1991w0'
	append using `r1997all'
	append using `r1997w0'
	duplicates drop
	compress
	saveold "${monopsonies}/dta/coeffs/${outdate}/lnxi_zrot_${path}.dta", replace

	* Construct data for theta regressions
	foreach eweight in all w0 {
		
		*** Get eta estimate: always use estimate for unique producers, weighted ***
		insheet using "${monopsonies}/csv/${outdate}/eta_change_regressions_${path}.csv", clear
		keep if weight=="`eweight'"
		
		levelsof iv_b,  local(eta_inverse)
		levelsof iv_se, local(eta_inverse_se)
		local 	 eta = 1/`eta_inverse'

		* Get estimates of xis 
		u "${monopsonies}/dta/coeffs/${outdate}/lnxi_zrot_${path}.dta", clear
		keep if (year==1997 | year== 1991) & weight=="`eweight'"
		keep    fakeid_firm ${mkt} lnemp lnxi_zrot year
		reshape wide ln*, i(fakeid_firm ${mkt}) j(year)
		
		foreach y in 1997 1991 {
			gen   double emp`y' 	= exp(lnemp`y')
			gen   double xi_zro`y' 	= exp(lnxi_zrot`y')
			gen   double product`y'	= (emp`y'*xi_zro`y')^((`eta'+1)/`eta')
			gegen double Sum`y'		= sum(product`y'), by(${mkt})
		}
		gen double chng_Lro = (`eta'/(`eta'+1))*(ln(Sum1997) - ln(Sum1991))
		
		keep ${mkt} chng_Lro
		gduplicates drop
		tempfile chng_Lro
		sa		`chng_Lro'
		
		***** Bring in shocks at market-level *****
		preserve
			u "${monopsonies}/sas/regsfile_${path}.dta", clear
			ren ice_dwTRAINS_Hf ice_dwtrains_hf 
			keep ${mkt} ice_dwtrains_hf 
			gduplicates drop
			tempfile market
			sa `market'
		restore
		merge 1:1 ${mkt} using `market', keep(1 3) nogen
		
		****** Get pre-shock trend in employment to de-trend chng_Lro ****
		preserve
			u "${monopsonies}/sas/regsfile_${path}.dta", clear
			ren ice_dwTRAINS_Hf ice_dwtrains_hf 
			keep if year==1991
			keep ${mkt} ice_dwtrains_hf
			tempfile shock
			sa `shock'
		restore
		preserve
			u "${monopsonies}/sas/regsfile_${path}.dta", clear
			ren ice_dwTRAINS_Hf ice_dwtrains_hf 
			keep if inlist(year,1988,1991)
			keep ${mkt} year mkt_emp
			gen lnmkt_emp = ln(mkt_emp)
			drop mkt_emp
			reshape wide lnmkt_emp, i(${mkt}) j(year)
			
			************ Control for fitted linear pre-trend ************	
			gen double chng_Lro =  lnmkt_emp1988 - lnmkt_emp1991
			keep ${mkt} chng_Lro
			gen period = "pre"
			
			** Add chng_Lro: wage changes for the subsequent years ***
			append using `chng_Lro'
			replace period = "post" if missing(period)
			merge m:1 ${mkt} using `shock', keep(3) nogen
	
			** Fit Pre-trend on pre-years ***
			reg 	chng_Lro ice_dwtrains_hf if period=="pre"
			predict chng_Lro_d, resid		/* Extrapolate to later year long difference */
			
			keep if period == "post"
			keep ${mkt} chng_Lro_d
						
			gduplicates drop
			tempfile trend_emp
			sa `trend_emp'
		restore
		merge 1:1 ${mkt} using `trend_emp', keep(3) nogen // XXBMS this is a keep(3) - do we drop anything here? 
		
		*** Merge in delta_ro from changes regression ****
		preserve
			u "${monopsonies}/dta/coeffs/${outdate}/eta_change_delta_ro_${path}.dta", clear
			keep if weight=="`eweight'"
			keep ${mkt} delta_ro
			gduplicates drop
			isid  ${mkt} 
			tempfile resids
			sa `resids'
		restore
		merge 1:1 ${mkt} using `resids', keep(3) nogen
		
		* Get pre-shock trend in wage as well
		preserve
			keep ${mkt} ice_dwtrains_hf delta_ro
			duplicates drop
			gen period = "post"
			ren delta_ro Ddprem_ro
			tempfile delta_ro
			sa 		`delta_ro'
		restore
		preserve
			keep ${mkt} ice_dwtrains_hf
			gduplicates drop
			merge 1:m ${mkt} using "${monopsonies}/sas/rais_lnearn_premia_${path}_${outdate}.dta", keep(3) nogen
			keep ${mkt} year dprem_ro ice_dwtrains_hf
			
			keep if inlist(year,1988,1991)	
			reshape wide dprem_ro, i(${mkt}) j(year)
			
			************ Control fo fitted linear pre-trend ************	
			gen double Ddprem_ro = dprem_ro1988 - dprem_ro1991
			keep ${mkt} ice_dwtrains_hf Ddprem_ro
			
			gen period = "pre"
			
			** Add delta_ro: wage changes for the subsequent years ***
			append using `delta_ro'
			
			** Fit Pre-trend on pre-years ***
			reg Ddprem_ro ice_dwtrains_hf if period=="pre"
			predict delta_ro_d, resid	
			
			keep if period == "post"
			keep ${mkt} delta_ro_d

			gduplicates drop
			tempfile trend_w
			sa 		`trend_w'
		restore
		merge 1:1 ${mkt} using `trend_w', keep(3) nogen // XXBMS this is a keep(3) - do we drop anything here? 
		
		* Recode ICE shock 
		replace ice_dwtrains_hf = 0 if missing(ice_dwtrains_hf)
		replace ice_dwtrains_hf = -ice_dwtrains_hf
	
		* Get weights *
		preserve
			u "${monopsonies}/sas/regsfile_${path}.dta", clear
			keep if year==1991
			keep ${mkt} mkt_emp
			ren mkt_emp w0
			tempfile W
			sa `W'
		restore	
		merge 1:1 ${mkt} using `W', keep(3) nogen	// XXBMS this is a keep(3) - do we drop anything here? 
		
		* All variables are aggregates at the market level including all firms in the market
		gen all = 1
		gegen double fe_ro = group(${mkt})
		tempfile forregs
		sa `forregs'
		outsheet using "${monopsonies}/csv/${outdate}/theta_est_ew_`eweight'_${path}.csv", comma replace
		
		* Estimate theta 
		foreach weight in all w0 {

			u `forregs', clear
			
			noi ivreg2 delta_ro (chng_Lro = ice_dwtrains_hf) [w=`weight'], savefirst saverf cluster(fe_ro) 

			local obs 	  = e(N)
			unique ${mkt}    if e(sample)
			local mkts    = `r(unique)'
			local m_iv_b  =  _b[chng_Lro]
			local m_iv_se = _se[chng_Lro]
			mat FSstats   = e(first)
			local FS_F 	  = FSstats[4,1]
			
			di  "eta_inverse"
			di "`eta_inverse'"
			
			di  "eta_inverse + m_iv_b"
			di "`eta_inverse' + `m_iv_b'"
			local theta_inverse_b = `eta_inverse' + `m_iv_b'
			
			di  "theta_inverse_b"
			di "`theta_inverse_b'"
			local theta_inverse_se = sqrt(`m_iv_se'^2 - (`eta_inverse_se')^2)
			
			estimates restore _ivreg2_delta_ro
			local m_rf_b 	=  _b[ice_dwtrains_hf]
			local m_rf_se 	= _se[ice_dwtrains_hf]
			estimates restore _ivreg2_chng_Lro
			local m_fs_b 	=  _b[ice_dwtrains_hf]
			local m_fs_se 	= _se[ice_dwtrains_hf]
			
			reghdfe delta_ro chng_Lro [w=`weight'], absorb(all) cluster(fe_ro) 
			
			local m_ols_b  =  _b[chng_Lro]
			local m_ols_se = _se[chng_Lro]
						
			mat coeffs = (`m_ols_b',`m_ols_se',`m_iv_b',`m_iv_se', ///
						  `m_rf_b' ,`m_rf_se' ,`m_fs_b',`m_fs_se', ///
						  `FS_F',`theta_inverse_b', `theta_inverse_se',`obs',`mkts')
		
			**** Export coefficients *****
			preserve
				clear
				svmat coeffs
			
				gen eta_weight	 = "`eweight'"
				gen theta_weight = "`weight'"

				ren coeffs1  ols_b
				ren coeffs2  ols_se
				ren coeffs3  iv_b
				ren coeffs4  iv_se
				ren coeffs5  rf_b
				ren coeffs6  rf_se
				ren coeffs7  fs_b
				ren coeffs8  fs_se
				ren coeffs9  fs_F
				ren coeffs10 theta_inverse_b
				ren coeffs11 theta_inverse_se
				ren coeffs12 obs
				ren coeffs13 markets
				keep if !missing(ols_b)
		
				tempfile k`eweight'`weight'
				sa 		`k`eweight'`weight''
			restore
			
		} 
	} 

	** Append all main regression coefficients and export to csv ***
	u 				 `kallall', clear
	cap append using `kallw0'
	cap append using `kw0all'
	cap append using `kw0w0'

	duplicates drop
	outsheet using "${monopsonies}/csv/${outdate}/theta_change_regressions_${path}.csv", comma replace
	
cap log close
