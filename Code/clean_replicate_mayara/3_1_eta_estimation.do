/*
	Firm-market level regressions
*/
version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154



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

/*
local tarvars 		"lnT"
local allspecs 		"l"
local wagevars 		"lndp"
local allmodels 	"l"			/* b: back to 1985; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"cnae95"
local allsamp 		"all up`baseyear_n'mkt"

local allspecs 		"l"
local alltars 		"lnT"  //lnE
local allwages 		"lndp" //lndw
local allmodels 	"l"			/* b: back to 1985; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"firm " //  cnae95 fe_ro
local allsamp 		"all" //T up`baseyear_n'mkt up`baseyear_n' explib Tnexplib"
*/

local mainwage 	"lndp"
local mainclust "firm" // XX cnae95
local maintar	"lnT"

* Specification FEs
local labsorb "fe_ro"			/* When spec is m, absorb mmc-cbo942d */

*************************************
**************** Setup **************
*************************************

/* 
* XX This allows us to specify different specs but I haven't configured it for the rewrite of Mayara's code
do "${encrypted}/Felix_JMP/3_analysis/specs_config.do"
args spec
di "`spec'"
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
log using "${encrypted}/logs/3_1_eta_estimation_`spec'_`date'_`time'.log", replace

/*
// Retrieve the market variables and file suffix based on the spec
local mkt "${s_`spec'_mv}"
local path "${s_`spec'_fs}"
*/

local mkt "mmc cbo942d"
local path "mmc_cbo942d"

display "Using market variables: `mkt'"
display "Using path suffix: `path'"




	
if `setup'==1{
        ****************************
        * 1. Process tariff dataset
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
		sa t_change, replace


        ********************************************************
        * 2. Process market-level import competition exposure data set
        * XX This is the data set created by _050 and loads the ICE variables: mkt_emp  ice_dwErpTRAINS  ice_dwTRAINS  ice_dwTRAINS_Hf. How are these different than the chng_ln_TRAINS computed above?
		u "${monopsonies}/sas/regsfile_`path'.dta", clear
		isid `mkt' year
		keep if year==`baseyear'
		ren mkt_emp bemp
		* XX ice_dwerp ice_dwtrains    these variables don't exist in the inputdata set. I think it should be the ones renamed below, but that's a guess
		ren (ice_dwErpTRAINS ice_dwTRAINS)(ice_dwerp ice_dwtrains)
		keep `mkt' ice_dwerp ice_dwtrains bemp
		gduplicates drop
		tempfile market
		sa `market'

		* XX This is where Mayara loaded the importer/exports data set that she used in robustness checks but we don't have access to

        ********************************************************
        * 3. Process firm level data and merge on firm earnings premia
		u "${monopsonies}/sas/rais_collapsed_firm_`path'.dta", clear
		isid cnpj_raiz `mkt' year
		keep if inlist(year,`baseyear_p2',`baseyear_p1',`baseyear',`baseyear_o1',`baseyear_o2') // XX This should be unnecessary; data set will only contain base year (1991)
		merge 1:1 cnpj_raiz `mkt' year using "${monopsonies}/sas/rais_lnearn_premia_firm_`path'_`premiadate'.dta", ///
		    keepusing(dprem_zro davgw_zro) keep(3) nogen
		isid cnpj_raiz `mkt' year
		ren dprem_zro lndp
		ren davgw_zro lndw
		
        ********************************************************
        * 4. Optional: write summary counts by cbo942d and mmc
		local obsmktslist=1
		if inlist("`version'", "original", "3states") {
			preserve
				gen obs=1
				keep obs cbo942d
				collapse (sum) obs, by(cbo942d)
				
				outsheet using "${monopsonies}/csv/`outdate'/etaregs_cbo942ds_`path'.csv", comma replace
			restore
			
			preserve
				gen obs=1
				keep obs mmc
				collapse (sum) obs, by(mmc)
				
				outsheet using "${monopsonies}/csv/`outdate'/etaregs_mmcs_`path'.csv", comma replace
			restore
		}
		
		***********************************
		******* Sample restrictions *******
		***********************************

		* Drop certain mmcs
        * XX Not sure if we actually want to drop these or not. Mayara does drop them I believe so not sure why we wouldn't too. But should they be dropped upstream, e.g. in 13007? 
		drop if mmc==13007 
		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
		drop if mmc_drop==1
		
		* Merge in tariffs so can also restrict to tradables
		merge m:1 cnae95  using `t_change', keep(1 3) /* XX nogen*/		/* Long change in tariffs */
		* XX My hypothesis is that most non-tradable sectors are excluded from the data and thus we need to impute these zeroes. 
		*replace tradable=0 if _merge==1
		*replace chng_lnTRAINS = 0 if _merge==1
		drop _merge
		* Tradable sector dummies (ibgesub only included in TRAINS data at this point)
		gegen ibge = max(ibgesubsector), by(cnpj_raiz)
		gen T = (ibge<14 | ibge==25)


		*  XX This is where Mayara merges in exporter dummies

		replace chng_lnTRAINS 		= 0 if T==0
		//replace chng_lnErpTRAINS 	= 0 if T==0
		
		* Compute firm baseline share
        * XX This is a at the firm level. In _050 we do this at the market level
		preserve
			 keep if year==`baseyear'
			 gegen double sumearn = sum(totmearn), by(`mkt')
			 gen double bwshare = totmearn/sumearn
			 gegen double sumemp = sum(emp), by(`mkt')
			 gen double beshare = emp/sumemp

			 gegen double Tsumearn = sum(totmearn) if T==1, by(`mkt')
			 gen double bTwshare = totmearn/Tsumearn if T==1
			 gegen double Tsumemp = sum(emp) if T==1, by(`mkt')
			 gen double bTeshare = emp/Tsumemp if T==1
			 
			 keep cnpj_raiz `mkt' beshare bwshare bTeshare bTwshare 
			 tempfile shares
			 sa `shares'
		restore
        * XX I can't see where any of these shares are actually used here or in 4_1. 

		* Merge in base shares, replacing with zero if firm was not there
		merge m:1 cnpj_raiz `mkt' using `shares', keep(1 3) nogen

		* Dummy for whether firm-market pair existed in `baseyear'
		gen double in`baseyear_n'mkt = !missing(beshare)

		* Dummy for whether firm exited in `baseyear'
		gegen in`baseyear_n' = max(in`baseyear_n'mkt), by(cnpj_raiz)

		foreach var of varlist beshare bwshare bTeshare bTwshare{
			replace `var' = 0 if missing(`var')
		}

		* Keep firms that were there in `baseyear' (but ok if firms enters new markets)
		keep if in`baseyear_n'==1


		************************************

		* Flag Unique producers of each cnae95 within each market
        /* 
        * XX I think this can all be deleted because it flags unique producers within each market but the resulting flag is only used for robustness checks
		preserve
			keep if year==`baseyear' & T==1
			keep cnpj_raiz cnae95 `mkt'
			bys cnae95 `mkt': gegen producers = count(cnpj_raiz)
			keep if producers==1
			keep cnpj_raiz `mkt'
			
			gen up`baseyear_n' = 1
			tempfile unique 
			sa `unique'
		restore	
		merge m:1 cnpj_raiz `mkt' using `unique', keep(1 3) nogen

		replace up`baseyear_n' = 0 if missing(up`baseyear_n')

		* Market that has unique producer 
		gegen up`baseyear_n'mkt_temp = max(up`baseyear_n'), by(`mkt')
		gen double up`baseyear_n'mkt = up`baseyear_n'==1 | (up`baseyear_n'mkt_temp==1 & T==0)
		drop up`baseyear_n'mkt_temp
		*/

		* Merge in market shocks
		merge m:1 `mkt' using `market', keep(3) nogen
		
		* Log employment
		gen double lnemp = ln(emp)
		
		gegen fe_zro = group(cnpj_raiz `mkt')
	
		di "Long differences"
		preserve
			keep fe_zro year lnemp lndp lndw
			xtset fe_zro year
			
			foreach var of varlist lnemp lndp lndw{
				
				* lag-year differences
				gen double chng_`var' = `var' - l`lagyears'.`var'
				
				* Difference back to base year
				gegen double `var'`baseyear_n' 	= max(cond(year==`baseyear',`var',.)), by(fe_zro)
				gen double chng`baseyear_n'_`var' = `var' - `var'`baseyear_n'
				
				* Replace with negative if year is before base year
				replace chng`baseyear_n'_`var' 	= - chng`baseyear_n'_`var' if year<`baseyear'
			}
			
			keep if inlist(year,`baseyear_p2',`baseyear_p1',`baseyear',`baseyear_o1',`baseyear_o2')
			keep year fe_zro chng*

			tempfile long
			sa `long'
		restore 
		
		* XX Saving data set for analysis with Ben
		preserve
				merge m:1 year fe_zro using `long', keep(1 3) nogen
				saveold "${monopsonies}/sas/eta_changes_regsfile_`path'_keepyearsvars.dta", replace
				pause off
				pause
		restore
		
		di "Keep changes and baseyear info only"
		keep if inlist(year,`baseyear_p2',`baseyear_p1',`baseyear',`baseyear_o1',`baseyear_o2')
		merge m:1 year fe_zro using `long', keep(1 3) nogen
		
		keep 	cnpj_raiz `mkt' year cnae95 ///
				fe_zro bTwshare bTeshare  bwshare  beshare   ///
				chng*  T bemp // ice_dwerp ice_dwtrains Tnexplib bexp //up`baseyear_n' up`baseyear_n'mkt
		
		order	year fe_zro cnpj_raiz `mkt' cnae95 T bemp //Tnexplib explib  explib   // up`baseyear_n' up`baseyear_n'mkt
				
		compress
		isid cnpj_raiz year `mkt'
		saveold "${monopsonies}/sas/eta_changes_regsfile_`path'.dta", replace

 /* Close use sample */
}

***************************************************
***************** Eta regressions *****************
***************************************************	


if `eta_regs'==1{
	
	u "${monopsonies}/sas/eta_changes_regsfile_`path'.dta", clear
	* CBO codes: https://concla.ibge.gov.br/images/concla/documentacao/ibgexcbo94.xls 
	drop if inlist(cbo942d,31,22,37)
	* XX drop 21? 
	
	* Cross-section FEs
	gegen fe_ro = group(`mkt')
		
	ren chng_lnTRAINS chng_lnT
	//ren chng_lnErpTRAINS chng_lnE
	
	/* Flip sign for easier interpretation */
	replace chng_lnT = - chng_lnT
	//replace chng_lnE = - chng_lnE
	//replace ice_dwtrains = - ice_dwtrains
	
	gen double firm = cnpj_raiz
	gen all = 1
	ren bemp w0
	
	//ren ice_dwtrains 	ice
	//ren bexp 		bex
	ren bwshare     bws
	ren beshare		bes
	
	
	* We lose a pretty signficant number of observations here because regression variables are missing or there are singleton observations
	di "HERE"
	di "Running ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) if all==1 & year==1997 [w=all], cluster(firm) absorb(delta_ro = fe_ro) "
	/*qui*/ ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) if all==1 & year==1997 [w=all], savefirst saverf cluster(firm) absorb(delta_ro = fe_ro)
	* XX Ben thinks we should be weighting by initial employment. But weighting by bemp makes 1/eta larger, and thus farther from Mayara's estimte
	*	- weighting by bes gives 0.69
	*	- weighting by bws gives 0.84
	preserve 
	gen esample = e(sample)
	keep if esample
	keep esample cnpj_raiz mmc cbo942d
	duplicates drop
	save "${monopsonies}/dta/coeffs/`outdate'/eta_regs_esample.dta", replace
	restore

	preserve
	keep if all==1 & year==1997
	keep cnpj_raiz `mkt' chng91_lndp chng91_lnemp delta_ro
	keep if !missing(delta_ro)

	ren chng91_lndp  chng_wagevar
	ren chng91_lnemp chng_lnemp
	
	gen outyear 	= 1997
	gen spec    	= "l"
	gen model   	= "l"
	gen tariff  	= "lnT"
	gen cluster 	= "firm"
	gen sample  	= "all"
	gen wagevartype = "lndp"
	gen weight		= "all"
	
	tempfile rlfirmallllnT1997lndpall
	sa `rlfirmallllnT1997lndpall'
restore
drop delta_ro

local obs = e(N)
unique cnpj_raiz if e(sample)
local firms = `r(unique)'
unique `mkt' if e(sample)
local mkts = `r(unique)'

local m_iv_b = _b[chng91_lnemp]
local m_iv_se = _se[chng91_lnemp]

mat first = e(first)

estimates restore _ivreg2_chng91_lndp
local m_rf_b = _b[chng_lnT]
local m_rf_se = _se[chng_lnT]
estimates restore _ivreg2_chng91_lnemp
local m_fs_b = _b[chng_lnT]
local m_fs_se = _se[chng_lnT]
local FS_F = first[4,1]
*macro list 
pause off
pause


di "Running OLS: reghdfe `lhs' `rhs' if `tsamp'==1 & year==`year' [w=`weight'], vce(cluster `clust') absorb(``spec'absorb') "
/*qui*/ reghdfe chng91_lndp chng91_lnemp if all==1 & year==1997 [w=all], vce(cluster firm) absorb(fe_ro) 
local m_ols_b = _b[chng91_lnemp]
local m_ols_se = _se[chng91_lnemp]

mat coeffs = (	`m_ols_b',`m_ols_se',`m_iv_b',`m_iv_se', ///
			`m_rf_b',`m_rf_se',`m_fs_b',`m_fs_se', ///
			`FS_F',`obs',`firms',`mkts')
preserve
	clear
	svmat coeffs
	gen year 	= 1997
	gen spec    = "l"
	gen model   = "l"
	gen tariff  = "lnT"
	gen clust = "firm"
	gen samp  = "all"
	gen wagevar = "lndp"
	gen weight	= "all"
	
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

	tempfile klfirmallllnT1997lndpall
	sa `klfirmallllnT1997lndpall'

restore
	
estimates clear
	
	
	*********************************************
	************* Outsheet results  *************
	*********************************************
	
	** Append all main regression coefficients and export to csv ***
	u `klfirmallllnT1997lndpall', clear
	
	duplicates drop
	outsheet using "${monopsonies}/csv/`outdate'/eta_change_regressions_`path'.csv", comma replace
	outsheet using "${monopsonies}/csv/`outdate'/eta_change_regressions_`path'_`date'_`time'.csv", comma replace

	
	******************************************************************************
	** Append all fixed effects and save for phi estimation next ***
	******************************************************************************

	u `rlfirmallllnT1997lndpall', clear
	
	duplicates drop
	compress
	saveold "${monopsonies}/dta/coeffs/`outdate'/eta_change_delta_ro_`path'.dta", replace

}


log close 
