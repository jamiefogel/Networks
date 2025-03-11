
local path 3states_gamma
local mkt "gamma" 


u "${monopsonies}/sas/eta_changes_regsfile_`path'.dta", clear

//cap drop if inlist(cbo942d,31,22,37)

* Cross-section FEs
gegen fe_ro = group(`mkt')
	
ren chng_lnTRAINS chng_lnT
//ren chng_lnErpTRAINS chng_lnE

/* Flip sign for easier interpretation */
replace chng_lnT = - chng_lnT
//replace chng_lnE = - chng_lnE
//replace ice_dwtrains = - ice_dwtrains

gen double firm = fakeid_firm
gen all = 1
ren bemp w0

//ren ice_dwtrains 	ice
//ren bexp 		bex
ren bwshare     bws
ren beshare		bes

local outdate		= 20210802
local premiadate	= 20210802
local baseyear 		= 1991
local baseyear_n 	= 91

local lagyears		= 3
local baseyear_o1 	= `baseyear'+3
local baseyear_o2	= `baseyear'+6
local baseyear_p1	= `baseyear'-3
local baseyear_p2	= `baseyear'-5


local allsamp 		"all" //T up`baseyear_n'mkt up`baseyear_n' explib Tnexplib"

local allspecs 		"l"
local alltars 		"lnT"  //lnE
local allwages 		"lndp" //lndw
local allmodels 	"l"			/* b: back to 1985; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"firm " //  cnae95 fe_ro
local allsamp 		"all" //T up`baseyear_n'mkt up`baseyear_n' explib Tnexplib"

local mainwage 	"lndp"
local mainclust "firm" // XX cnae95
local maintar	"lnT"

* Specification FEs
local labsorb "fe_ro"			/* When spec is m, absorb mmc-cbo942d */


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
foreach weight in all /*w0*/{
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
			di "HERE"
			di "Running ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], cluster(`clust') absorb(delta_ro =``spec'absorb') "
			/*qui*/ ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], savefirst saverf cluster(`clust') absorb(delta_ro =``spec'absorb') 
		
			preserve
				keep if `tsamp'==1 & year==`year'
				keep fakeid_firm `mkt' `lhs' `rhs' delta_ro
				keep if !missing(delta_ro)

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
			drop delta_ro
		}
		else{
			di "Running ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], cluster(`clust') absorb(``spec'absorb') "
			/*qui*/ ivreghdfe `lhs' (`rhs' = `inst') if `tsamp'==1 & year==`year' [w=`weight'], savefirst saverf cluster(`clust') absorb(``spec'absorb') 
			* I think this is the preferred spec with all of the above macros evaluated
			*ivreghdfe  chng91_lndp (chng91_lnemp = chng_lnT) if all==1 & year==1997 [w=all], cluster(firm) absorb(fe_ro)
			/* . count if all==1 & year==1997
			1,704,525
			keep if all==1 & year==1997
			. count if !mi(chng91_lndp)
			  923,047
			-> . count if !mi(chng91_lnemp)
			  923,047
			-> . count if !mi(chng_lnT)
			  397,120
			-> . count if !mi(chng91_lndp, chng91_lnemp, chng_lnT)
			  217,523
			* Mayara's sample size in Table 2 is 854,068. So I think the missing tariff obs is a big problem here. 
			*/
			
		}
}
}
}
}
}
}
}
}
