*version 14.2
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
	*global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara"
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\clean_replicate_mayara"
	global dictionaries		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	*global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\monopsonies"
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\clean_replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/clean_replicate_mayara"
	global dictionaries		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdictionaries/harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	*global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies"
	global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/clean_replicate_mayara/monopsonies"
	global public			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/publicdata"
}

***************************
** SETUP				 **
***************************

* Set firm & market IDs
global firmid "cnpj_raiz"
global mkt 	  "mmc cbo942d"

* Read in data 
u using "${monopsonies}/sas/worker_level.dta", clear

* Keep if education category is well-defined
keep if !missing(educ)

* Keep only onbs with non-zero dec earnings
keep if earningsdecmw > 0
gen lndecearn  = ln(earningsdecmw)

* Define tradable sector 
gen 				   T 	  = (ibgesubsector<14 | ibgesubsector==25) //  T is not constant within cnpj_raiz b/c firms can change ibgessubsector over time
bysort cnpj_raiz: egen T_1991 = max(T*(year==1991))

* Re-code tariff shock
replace chng_lnTRAINS = 0 if T==0				// XXBMS -- should we change this only for firms s.t. ~mi(chng_lnTRAINS)? 
count if mi(chng_lnTR) 							// 2 million obs that are missing chng_lnTRAINS but are allegedly tradable. Should these be missing or 0?
replace chng_lnTRAINS = 0 if mi(chng_lnTRAINS) 	// This gets us to ~18,000 markets but doesn't really change eta_hat

* Compute firm-market-year earnings premia, also save residualized wages  
gegen fe_zrot  = group($firmid mmc cbo942d year)
reghdfe lndecearn i.year##i.female i.year##i.agegroup i.year##i.educ, absorb(lndpt=fe_zrot) noconstant keepsingletons
gegen fe_zrot_gamma  = group($firmid gamma year)
reghdfe lndecearn i.year##i.female i.year##i.agegroup i.year##i.educ, absorb(lndpt_gamma=fe_zrot_gamma) noconstant keepsingletons

reg     lndecearn i.year##i.female i.year##i.agegroup i.year##i.educ, 					    noconstant 
predict res_lndecearn, r 

compress

* Save 
save precollapse_bs, replace

* Collapse to firm-market year level
if "$mkt"=="gamma"{
	replace lndpt = lndpt_gamma
	drop lndpt_gamma
}
gen  		   firm_mkt_emp = 1
collapse (sum) firm_mkt_emp firm_mkt_tot_earndec = earningsdecmw  (mean) res_lndecearn (firstnm) lndpt chng_lnTRAINS T_1991, by($firmid $mkt    year)
reshape wide   firm_mkt_emp firm_mkt_tot_earndec  						 res_lndecearn           lndpt chng_lnTRAINS, 		  i($firmid $mkt) j(year)

*XXBMS Not sure we should drop firms missing chng_lnTRAINS explicitly -- will affect the construction of market-level labor supply indicies 
*drop if chng_lnTRAINS1991 != chng_lnTRAINS1997
ren 	 chng_lnTRAINS1991    chng_lnTRAINS

local strmkt = subinstr("${mkt}", " ", "", .)
save collapsed_reshaped_bs_`strmkt', replace


***************************
** ETA					 **
***************************

* Generate long differences 
gen chng91_lndp  = 			 lndpt1997  - 			 lndpt1991
gen chng91_lnres =    res_lndecearn1997  -    res_lndecearn1991
gen chng91_lnemp = log(firm_mkt_emp1997) - log(firm_mkt_emp1991)
gen chng_lnT 	= chng_lnTRAINS*-1

* Estimate 1/eta
gegen fe_ro 	    = group($mkt)
gen   eta_weight = firm_mkt_emp1991 // 1
ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) [w=eta_weight], savefirst saverf cluster($firmid) absorb(delta_ro = fe_ro)
* XXBMS -- interested to try with chng91_lnres as wage variable too -- could save time by residualizing wages once up top 

* Report results
local eta_inverse = _b[chng91_lnemp]
local eta 		  = 1/`eta_inverse'
local obs 		  = e(N)
unique $firmid if   e(sample)
local firms 	  = `r(unique)'
unique $mkt    if   e(sample)
local mkts  	  = `r(unique)'
di "Eta = `eta', Obs = `obs', Firms = `firms', Markets = `mkts'"

* Save market-level regression statistics for collapse 
gen 			  		  eta_sample 		  = e(sample)
bysort $mkt: gegen mkt_eta_sample 	  = total(eta_sample)
bysort $mkt: gegen mkt_tot_wgt 	  	  = total(eta_sample*eta_weight)
gen               		  wgt				  = eta_sample* eta_weight/mkt_tot_wgt
bysort $mkt: gegen mkt_avg_lndp 	  	  = total(wgt * chng91_lndp )
bysort $mkt: gegen mkt_avg_lnres    	  = total(wgt * chng91_lnres)
bysort $mkt: gegen mkt_avg_lnemp    	  = total(wgt * chng91_lnemp)
bysort $mkt: gegen mkt_avg_lnT    	  = total(wgt * chng_lnT    )
bysort $mkt: gegen mkt_var_lndp 	  	  = total(wgt * (chng91_lndp  - mkt_avg_lndp )^2)
bysort $mkt: gegen mkt_var_lnres    	  = total(wgt * (chng91_lnres - mkt_avg_lnres)^2)
bysort $mkt: gegen mkt_var_lnemp    	  = total(wgt * (chng91_lnemp - mkt_avg_lnemp)^2)
bysort $mkt: gegen mkt_var_lnT    	  = total(wgt * (chng_lnT     - mkt_avg_lnT  )^2)
bysort $mkt: gegen mkt_cov_lndp_lnres  = total(wgt * (chng91_lndp  - mkt_avg_lndp ) * (chng91_lnres - mkt_avg_lnres))
bysort $mkt: gegen mkt_cov_lndp_lnemp  = total(wgt * (chng91_lndp  - mkt_avg_lndp ) * (chng91_lnemp - mkt_avg_lnemp))
bysort $mkt: gegen mkt_cov_lndp_lnT    = total(wgt * (chng91_lndp  - mkt_avg_lndp ) * (chng_lnT     - mkt_avg_lnT  ))
bysort $mkt: gegen mkt_cov_lnres_lnemp = total(wgt * (chng91_lnres - mkt_avg_lnres) * (chng91_lnemp - mkt_avg_lnemp))
bysort $mkt: gegen mkt_cov_lnres_lnT   = total(wgt * (chng91_lnres - mkt_avg_lnres) * (chng_lnT     - mkt_avg_lnT  ))
bysort $mkt: gegen mkt_cov_lnemp_lnT   = total(wgt * (chng91_lnemp - mkt_avg_lnemp) * (chng_lnT     - mkt_avg_lnT  ))
drop wgt 

** XXBMS -- not convinced we should do this drop! 
*drop if !e(sample)


***************************
** THETA				 **
***************************

* Save market-level averages/counts/HHI, construct "taste-adjusted" labor supply indices 
local eta = 1/`eta_inverse' // 1.36
foreach year in 1991 1997 {
	
	bysort $mkt: gegen mkt_emp`year'	  	    = total(firm_mkt_emp`year')
	bysort $mkt: gegen mkt_n_firm`year' 	    = total(firm_mkt_emp`year' > 0)
	bysort $mkt: gegen mkt_tot_earndec`year' = total(firm_mkt_tot_earndec`year') 
	bysort $mkt: gegen mkt_hhi`year'		    = total((firm_mkt_tot_earndec`year'/mkt_tot_earndec`year')^2)

	gen   lhs`year' 			  = lndpt`year' - `eta_inverse'*log(firm_mkt_emp`year')
	qui areg     lhs`year', absorb(fe_ro)
	predict      lnxi_zrot`year', resid
	
	bysort $mkt: gegen mkt_LSind`year' 	    = total((firm_mkt_emp`year'*exp(lnxi_zrot`year'))^((`eta'+1)/`eta'))	
	replace 				  mkt_LSind`year' 	    = (`eta'/(`eta'+1))*ln(mkt_LSind`year')
	
	bysort $mkt: gegen mkt_LSind_noxi`year'  = total((firm_mkt_emp`year'					  )^((`eta'+1)/`eta'))	
	replace 	 			  mkt_LSind_noxi`year'  = (`eta'/(`eta'+1))*ln(mkt_LSind_noxi`year')
	
	drop lhs`year' lnxi_zrot`year'
}

* Compute Delta ICE measures, share tradable, and the long difference of labor supply indices 
gen			  	  s_zm 				  = firm_mkt_tot_earndec1991/mkt_tot_earndec1991
gen			  	  s_zm_sq 			  = s_zm^2
gen		      	  num   		      = s_zm_sq * chng_lnT
gen		     	  num_alt 			  = s_zm    * chng_lnT
bysort $mkt: gegen mkt_sh_tradable 	  = total(s_zm     * T_1991)
bysort $mkt: gegen mkt_sq_sh_tradable  = total(s_zm_sq  * T_1991)
bysort $mkt: gegen delta_ice_hf_m 	  = total((num     * T_1991)/mkt_sq_sh_tradable)
replace 		  		  delta_ice_hf_m 	  = 0 if missing(delta_ice_hf_m)
bysort $mkt: gegen delta_ice_alt_m 	  = total((num_alt * T_1991)/mkt_sh_tradable)
replace 		   		  delta_ice_alt_m 	  = 0 if missing(delta_ice_alt_m)
gen 	 			  delta_Lro 		  = mkt_LSind1997 - mkt_LSind1991
gen 	 			  delta_Lro_noxi 	= mkt_LSind_noxi1997 - mkt_LSind_noxi1991
drop s_zm s_zm_sq num num_alt 

* Identify the first value within each market with non-missing delta_ro and run the regression only on these obs rather than collapsing to the market level
bysort $mkt ( delta_ro ): gen n = _n

* Estimate 1/eta - 1/theta 
ivreg2 delta_ro (delta_Lro = delta_ice_hf_m) if n==1, savefirst saverf cluster(fe_ro) 

//local eta_inverse = .4
//local eta = 1/`eta_inverse'
* Report theta & eta
local theta_inverse = _b[delta_Lro] + `eta_inverse'
local theta = 1/`theta_inverse'
di "Eta = `eta', Theta = `theta'"


***************************
** MARKET-LEVEL COLLAPSE **
***************************

preserve 

keep ${mkt} mkt_* delta_* 
duplicates drop 
*XXBMS not sure this is enough to force data to be one obs per market, doesn't really matter since all kept data is at the market level anyway
local strmkt = strtrim(${mkt})

save 		     collapse_`strmkt',     replace 
export delimited collapse_`strmkt'.csv, replace 



