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
	global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/clean_replicate_mayara/monopsonies"
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

* Collapse to firm-market year level

collapse (sum) firm_mkt_tot_earndec = earningsdecmw (count) firm_mkt_emp=cnpj_raiz (firstnm) lndpt chng_lnTRAINS, by($firmid $mkt year)


reshape wide firm_mkt_tot_earndec firm_mkt_emp lndpt, i($firmid $mkt chng_lnTRAINS) j(year)

gen chng91_lndp = lndpt1997 - lndpt1991
gen chng91_lnemp = log(firm_mkt_emp1997) - log(firm_mkt_emp1991)
gen chng_lnT = chng_lnTRAINS*-1
gegen fe_ro = group($mkt)

ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) , savefirst saverf cluster($firmid) absorb(delta_ro = fe_ro)


