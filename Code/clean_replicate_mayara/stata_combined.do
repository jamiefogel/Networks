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
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara"
	*global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\clean_replicate_mayara"
	global dictionaries		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\monopsonies"
	*global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\clean_replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/clean_replicate_mayara"
	global dictionaries		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdictionaries/harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies"
	*global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/clean_replicate_mayara/monopsonies"
	global public			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/publicdata"
}

global firmid "cnpj_raiz"
global mkt "mmc cbo942d"

u using "${monopsonies}/sas/worker_level.dta", clear

* Keep if education category is well-defined
keep if !missing(educ)

* Keep only onbs with non-zero dec earnings
keep if earningsdecmw>0

gen double lndecearn = ln(earningsdecmw)

gegen double fe_zrot = group($firmid $mkt year)


* Compute firm-market-year earnings premia
reghdfe lndecearn i.year##i.female i.year##i.agegroup i.year##i.educ , absorb(dprem_zrot=fe_zrot) noconstant keepsingletons
ren dprem_zrot lndpt

gen T = (ibgesubsector<14 | ibgesubsector==25)
replace chng_lnTRAINS 		= 0 if T==0


// XX Still 2 million obs that are missing chng_lnTRAINS but are allegedly tradable. Should these be missing or 0?
count if mi(chng_lnTRAINS )
// replace chng_lnTRAINS 		= 0 if mi(chng_lnTRAINS)
// If we do the replace above it gets us to ~18,000 markets but doesn't really change eta_hat


* Collapse to firm-market year level
keep pis cnpj_raiz earningsdecmw year jid ibgesubsector cnae95 chng_lnTRAINS lndecearn lndpt lndpt T mmc cbo942d
gen firm_mkt_emp = 1
collapse (sum) firm_mkt_tot_earndec = earningsdecmw firm_mkt_emp (firstnm) lndpt chng_lnTRAINS, by($firmid $mkt year)


reshape wide firm_mkt_tot_earndec firm_mkt_emp lndpt, i($firmid $mkt chng_lnTRAINS) j(year)

save collapsed_reshaped, replace

gen chng91_lndp = lndpt1997 - lndpt1991
gen chng91_lnemp = log(firm_mkt_emp1997) - log(firm_mkt_emp1991)
gen chng_lnT = chng_lnTRAINS*-1
gegen fe_ro = group($mkt)

ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) , savefirst saverf cluster($firmid) absorb(delta_ro = fe_ro)

local eta_inverse =  _b[chng91_lnemp]
local eta = 1/`eta_inverse'
di "Eta = `eta'"
local obs = e(N)
di `obs'
unique $firmid if e(sample)
local firms = `r(unique)'
unique $mkt if e(sample)
local mkts = `r(unique)'






**************************
* THETA

bysort $mkt: egen mkt_tot_earndec1991 = total(firm_mkt_tot_earndec1991)
gen s_zm = firm_mkt_tot_earndec1991/mkt_tot_earndec1991
gen s_zm_sq = s_zm^2

* Note I am not yet restricting to tradables only
bysort $mkt: egen denom = total(s_zm_sq)
gen num = s_zm_sq * chng_lnTRAINS
bysort $mkt: egen delta_ice_hf_m = total(num/denom)

replace delta_ice_hf_m = 0 if missing(delta_ice_hf_m)
replace delta_ice_hf_m = -delta_ice_hf_m
	
	

foreach year in 1991 1997{

	gen log_firm_mkt_emp`year' = log(firm_mkt_emp`year')
	gen lhs`year' = lndpt`year' - `eta_inverse' * log_firm_mkt_emp`year'
	qui areg lhs`year', absorb(fe_ro)
	predict lnxi_zrot`year', resid
	
	gen double emp`year' 			= exp(log_firm_mkt_emp`year')
	gen double xi_zro`year' 		= exp(lnxi_zrot`year')
	gen double product`year'		= (emp`year'*xi_zro`year')^((`eta'+1)/`eta')
	gegen double Sum`year'			= sum(product`year'), by($mkt)		
}
gen double chng_Lro = (`eta'/(`eta'+1))*(ln(Sum1997) - ln(Sum1991) )

* Identify the first value within each market with non-missing delta_ro and run the regression only on these obs rather than collapsing to the market level
bysort $mkt ( delta_ro ): gen n = _n
ivreg2 delta_ro (chng_Lro = delta_ice_hf_m) if n==1, savefirst saverf cluster(fe_ro) 


local theta_inverse = _b[chng_Lro] + `eta_inverse'
di `theta_inverse'
local theta = `theta_inverse'

di `eta'
di `theta'
