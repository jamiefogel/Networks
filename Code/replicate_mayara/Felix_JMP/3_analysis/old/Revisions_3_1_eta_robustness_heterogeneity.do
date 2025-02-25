/*
	Firm-market level regressions
	agegroup	age	workers
		3	21	5946493
		4	27	5047986
		5	34	8482670
		6	44	5187124
		7	55	2443825
		8	68	205238
	
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

local outdate		= 20220623
local premiadate	= 20210802
local baseyear 		= 1991
local baseyear_n 	= 91

local eta_regs 			= 1

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"


local tarvars 		"lnT"
local allspecs 		"l"
local wagevars 		"lndp"
local allmodels 	"l"			/* b: back to 1985; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"cnae95"
local allsamp 		"all"


local mainwage 	"lndp"
local mainclust "cnae95"
local maintar	"lnT"

* Specification FEs
local labsorb "fe_ro"			/* When spec is m, absorb mmc-cbo942d */



***************************************************
***************** Eta regressions *****************
***************************************************	

if `eta_regs'==1{

	* Baseline employment at unions
	u "${monopsonies}/sas/rais_collapsed_firm_mmc_cbo942d.dta", clear
	keep if year==1991
	gen double sindicate = cnae95==91200
	sum sindicate
	gen double empsindicate = emp*sindicate
	collapse (sum) empsindicate emp, by(mmc cbo942d)
	gen double bUnion_eshare = empsindicate/emp
	keep bUnion_eshare mmc cbo942d
	tempfile unionemp
	sa `unionemp'

	* Baseline formal sector market characteristics
	u "${monopsonies}/sas/regsfile_mmc_cbo942d.dta", clear
	keep if year==1991
	keep mmc cbo942d hf_wdbill mkt_emp explib_firms
	ren hf_wdbill 		bhf_wdbill
	ren mkt_emp 	 	bmkt_emp
	ren explib_firms 	bexplib_firms
	tempfile llmX
	sa `llmX'

	* Baseline MMC-Ibgesubsector characteristic: informality and self-employment
	u "${monopsonies}/sas/mmc_ibgesub_descriptives_1991.dta", clear 

	gen double informal_not_self = nonformemp - selfemployed /* Informal but working for firm */
	gen double binformal_notself = informal_not_self/employed
	gen double bselfemployed 	 = selfemployed/employed

	keep mmc ibgesubsector_grouped mmc binformal bselfemployed
	sum binformal bselfemployed, detail

	tempfile mmcIBGEX
	sa `mmcIBGEX'

	* Baseline MMC characteristics: unemployment, rural
	u "${monopsonies}/sas/mmc_ibgesub_descriptives_1991.dta", clear 
 	
 	* Note: pop18_64 = employed + unemp + nilf
	collapse (sum) nonformemp selfemployed unemp employed pop18_64 urban, by(mmc)
	
	gen double bMMCinformal = (nonformemp - selfemployed)/employed
	gen double bMMCselfemployed = selfemployed/employed

	gen double bMMCunemployed = unemp/(employed+unemp)
	gen double bMMCrural = (pop18_64 - urban)/pop18_64

	keep mmc bMMC*

	sum bMMC*, detail

	tempfile mmcX
	sa `mmcX'

	* Firm-cbo-mmc baseline worker composition
	u "${monopsonies}/sas/rais_firm_mmc_cbo942d_baseline_demos.dta", clear
	cap drop if inlist(cbo942d,31,22,37) |  mmc==13007 | mmc==23014

	isid fakeid_firm mmc cbo942d
	ren total_college total_col
	
	gen double bfemale = total_female/total_workers
	gen double bNoHS = (total_workers-total_hs)/total_workers
	gen double bHS = (total_workers-total_col-bNoHS)/total_workers
	gen double bCol = total_col/total_workers
	gen double bOlder = (total_agegroup6 + total_agegroup7)/total_workers

	* Majority female, majority low education, majority older
	foreach x in bfemale bNoHS bHS bCol bOlder{
		gen `x'_gt50 = (`x'>0.50)
	}

	tempfile baseX
	sa `baseX'

	************ Compute some stats for easier interpretation ************
	local outstats = 0

	if `outstats'==1{
	
	* Total
	collapse (sum) total*
	outsheet using "${monopsonies}/csv/`outdate'/worker_baselineX.csv", comma replace

	* By occupation
	u `baseX'
	collapse (sum) total*, by(cbo942d)
	gen double bfemale = total_female/total_workers
	gen double bNoHS = (total_workers-total_hs)/total_workers
	gen double bHS = (total_workers-total_col-bNoHS)/total_workers
	gen double bCol = total_col/total_workers
	gen double bOlder = (total_agegroup6 + total_agegroup7)/total_workers

	collapse (mean) mean_bfemale = bfemale ///
					mean_bNoHS = bNoHS ///
					mean_bHS = bHS ///
					mean_bCol = bCol ///
					mean_bOlder = bOlder ///
			 (p25)	p25_bfemale = bfemale ///
			 		p25_bNoHS = bNoHS ///
					p25_bHS = bHS ///
					p25_bCol = bCol ///
					p25_bOlder = bOlder ///
			 (p50)	p50_bfemale = bfemale ///
					p50_bNoHS = bNoHS ///
					p50_bHS = bHS ///
					p50_bCol = bCol ///
					p50_bOlder = bOlder ///
			 (p75)	p75_bfemale = bfemale ///
			 		p75_bNoHS = bNoHS ///
					p75_bHS = bHS ///
					p75_bCol = bCol ///
					p75_bOlder = bOlder

	gen aggregatedby = "cbo942d"
	tempfile byocup
	sa `byocup'

	* By firm
	u `baseX'
	collapse (sum) total*, by(fakeid_firm)
	gen double bfemale = total_female/total_workers
	gen double bNoHS = (total_workers-total_hs)/total_workers
	gen double bHS = (total_workers-total_col-bNoHS)/total_workers
	gen double bCol = total_col/total_workers
	gen double bOlder = (total_agegroup6 + total_agegroup7)/total_workers

	collapse (mean) mean_bfemale = bfemale ///
					mean_bNoHS = bNoHS ///
					mean_bHS = bHS ///
					mean_bCol = bCol ///
					mean_bOlder = bOlder ///
			 (p25)	p25_bfemale = bfemale ///
			 		p25_bNoHS = bNoHS ///
					p25_bHS = bHS ///
					p25_bCol = bCol ///
					p25_bOlder = bOlder ///
			 (p50)	p50_bfemale = bfemale ///
					p50_bNoHS = bNoHS ///
					p50_bHS = bHS ///
					p50_bCol = bCol ///
					p50_bOlder = bOlder ///
			 (p75)	p75_bfemale = bfemale ///
			 		p75_bNoHS = bNoHS ///
					p75_bHS = bHS ///
					p75_bCol = bCol ///
					p75_bOlder = bOlder
	gen aggregatedby = "fakeid_firm"

	tempfile byfirm
	sa `byfirm'

	* Firm-market
	* By firm
	u `baseX'
	collapse (mean) mean_bfemale = bfemale ///
					mean_bNoHS = bNoHS ///
					mean_bHS = bHS ///
					mean_bCol = bCol ///
					mean_bOlder = bOlder ///
			 (p25)	p25_bfemale = bfemale ///
			 		p25_bNoHS = bNoHS ///
					p25_bHS = bHS ///
					p25_bCol = bCol ///
					p25_bOlder = bOlder ///
			 (p50)	p50_bfemale = bfemale ///
					p50_bNoHS = bNoHS ///
					p50_bHS = bHS ///
					p50_bCol = bCol ///
					p50_bOlder = bOlder ///
			 (p75)	p75_bfemale = bfemale ///
			 		p75_bNoHS = bNoHS ///
					p75_bHS = bHS ///
					p75_bCol = bCol ///
					p75_bOlder = bOlder
	gen aggregatedby = "firm_mmc_cbo942d"

	append using `byocup'
	append using `byfirm'

	order aggregatedby

	outsheet using "${monopsonies}/csv/`outdate'/worker_baselineX_byvars.csv", comma replace
	} /* Close outstats */
	************************************************************************************

	u "${monopsonies}/sas/eta_changes_regsfile0.dta", clear
	keep if year==1997

	cap drop if inlist(cbo942d,31,22,37)

	* Merge in firm-mmc-cbo942d baseline demo composition
		merge m:1 fakeid_firm mmc cbo942d using `baseX', keep(1 3) nogen keepusing(ibgesubsector bfemale bNoHS bHS bCol bOlder)

	* Merge in mmc-cbo942d characteristics
		 merge m:1 mmc cbo942d using `llmX', keep(3) nogen
		 merge m:1 mmc cbo942d using `unionemp', keep(1 3) nogen

	* Merge in mmc-ibgesubsector baseline characteristics
		gen double ibgesubsector_grouped = ibgesubsector
		replace ibgesubsector_grouped=777 if inlist(ibgesubsector, 21,22,23)

		* Informality is at mmc-ibge level
		merge m:1 mmc ibgesubsector_grouped using `mmcIBGEX', keep(1 3) nogen

		* Others are mmc level
		merge m:1 mmc using `mmcX', keep(1 3) nogen

	* Cross-section FEs
	gegen fe_ro = group(mmc cbo942d)
		
	ren chng_lnTRAINS chng_lnT

	/* Flip sign for easier interpretation */
	replace chng_lnT = - chng_lnT
	replace chng_lnE = - chng_lnE
	
	gen double firm = fakeid_firm
	gen all = 1
	ren bemp w0
	
	local lhs chng91_lndp
	local rhs chng91_lnemp
	local inst chng_lnT

	* Interactions with baseline share characteristics (express as share*100 for easier interpretation of reg coeff)
	foreach x of varlist bfemale bNoHS bCol bOlder bMMC* bUnion_eshare{
		replace `x' = `x'*100
		gen double chng91_lnempX`x'= chng91_lnemp*`x'
		gen double chng_lnTX`x'= chng_lnT*`x'
	}
	
	* Replicate base - all good
	ivreghdfe chng91_lndp (chng91_lnemp = chng_lnT) if all==1 & year==1997 [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
	eststo IVhomo
	estimates restore _ivreg2_chng91_lndp
	eststo RFhomo

	* First stage
	estimates restore _ivreg2_chng91_lnemp
	eststo FShomo

	************* Heterogeneity by worker demographics *************
	* Now add interactions: female, uneducated, highly educated, old
	foreach demo in bCol bOlder bfemale{
			ivreghdfe chng91_lndp (chng91_lnemp ///
			chng91_lnempX`demo' = chng_lnT  ///
			chng_lnTX`demo' ) if all==1 & year==1997 [w=w0], ///
			savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
			eststo IVhet`demo'

		* Reduced form
		estimates restore _ivreg2_chng91_lndp
		eststo RFhet`demo'

		* First stage
		estimates restore _ivreg2_chng91_lnemp
		eststo FShet`demo'
	}

	* Full
	ivreghdfe chng91_lndp (chng91_lnemp ///
		chng91_lnempXbfemale chng91_lnempXbCol chng91_lnempXbOlder = chng_lnT  ///
		chng_lnTXbfemale chng_lnTXbCol chng_lnTXbOlder ) if all==1 & year==1997 [w=w0], ///
		savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
	eststo IVhet

	* Reduced form
	estimates restore _ivreg2_chng91_lndp
	eststo RFhet

	* First stage
	estimates restore _ivreg2_chng91_lnemp
	eststo FShet

	esttab IVhomo IVhetbfemale IVhetbCol IVhetbOlder IVhet using "${monopsonies}/csv/`outdate'/eta_iv_het_workerX", rtf replace se label

	esttab RFhomo FShomo RFhetbfemale FShetbfemale RFhetbCol FShetbCol RFhetbOlder FShetbOlder RFhet FShet using "${monopsonies}/csv/`outdate'/eta_rf_fs_het_workerX", rtf replace se label

	************* Heterogeneity by market characteristics *************

	* Informality (except for self-employment)
		ivreghdfe chng91_lndp 		(chng91_lnemp ///
									 chng91_lnempXbMMCinformal  ///
									   ///
									 = chng_lnT  ///
									 chng_lnTXbMMCinformal  ///
									  ) ///
			if all==1 & year==1997 [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
		eststo IVhetinf

		* Reduced form
		estimates restore _ivreg2_chng91_lndp
		eststo RFhetinf

		* First stage
		estimates restore _ivreg2_chng91_lnemp
		eststo FShetinf

	* Self-employment
		ivreghdfe chng91_lndp 		(chng91_lnemp ///
									 chng91_lnempXbMMCselfemployed  ///
									   ///
									 = chng_lnT  ///
									 chng_lnTXbMMCselfemployed  ///
									  ) ///
			if all==1 & year==1997 [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
		eststo IVhetself

		* Reduced form
		estimates restore _ivreg2_chng91_lndp
		eststo RFhetself

		* First stage
		estimates restore _ivreg2_chng91_lnemp
		eststo FShetself

	* Union emp share
	 	ivreghdfe chng91_lndp 		(chng91_lnemp ///
									   ///
									 chng91_lnempXbUnion_eshare  ///
									 = chng_lnT  ///
									   ///
									 chng_lnTXbUnion_eshare ) ///
			if all==1 & year==1997 [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
		eststo IVhetUni

		* Reduced form
		estimates restore _ivreg2_chng91_lndp
		eststo RFhetUni

		* First stage
		estimates restore _ivreg2_chng91_lnemp
		eststo FShetUni

	* Unemployment
	ivreghdfe chng91_lndp 		(chng91_lnemp ///
									   ///
									  chng91_lnempXbMMCunemployed ///
									 = chng_lnT  ///
									   ///
									  chng_lnTXbMMCunemployed) ///
			if all==1 & year==1997 [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
		eststo IVhetUnem

		* Reduced form
		estimates restore _ivreg2_chng91_lndp
		eststo RFhetUnem

		* First stage
		estimates restore _ivreg2_chng91_lnemp
		eststo FShetUnem

	* Full
		ivreghdfe chng91_lndp 		(chng91_lnemp ///
									 chng91_lnempXbMMCinformal chng91_lnempXbMMCselfemployed ///
									 chng91_lnempXbUnion_eshare chng91_lnempXbMMCunemployed ///
									 = chng_lnT  ///
									 chng_lnTXbMMCinformal chng_lnTXbMMCselfemployed ///
									 chng_lnTXbUnion_eshare chng_lnTXbMMCunemployed) ///
			if all==1 & year==1997 [w=w0], savefirst saverf cluster(fakeid_firm) absorb(fe_ro) 
		eststo IVhetfull

		* Reduced form
		estimates restore _ivreg2_chng91_lndp
		eststo RFhetfull

		* First stage
		estimates restore _ivreg2_chng91_lnemp
		eststo FShetfull

	* Save

	esttab IVhomo IVhetinf IVhetself IVhetUni IVhetUnem IVhetfull using "${monopsonies}/csv/`outdate'/eta_iv_het_marketX", rtf replace se label

	esttab RFhomo FShomo RFhetinf FShetinf RFhetself FShetself RFhetUni FShetUni RFhetUnem FShetUnem RFhetfull FShetfull using "${monopsonies}/csv/`outdate'/eta_rf_fs_het_marketX", rtf replace se label


}
