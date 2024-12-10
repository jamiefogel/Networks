version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154

* ssc install vcemway

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


global minyear = 1986
global maxyear = 2000
local minyear = ${minyear}
local maxyear = ${maxyear}
local baseyear = 1991

local regressions	= 1
local premiadate	= 20210802
local regsdate		= 20210802		

local allice "iceH iceW iceE iceWErp"
*local allice "iceH"
local icemain "iceH"

* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`regsdate'"
cap mkdir "${monopsonies}/eps/`regsdate'"
********************************************************************************
******** LLM-level regressions on earnings, employment and concentration *******
********************************************************************************

if `regressions'==1{

	foreach group in none cbo942d {
	
		u "${monopsonies}/sas/regsfile_mmc_`group'.dta", clear
		drop if year==1985
		
		cap drop tot_* wavg* ewavg* wavg*
		cap gen none = 1
		
		keep if inrange(year,${minyear},${maxyear})
		
		* Drop manaus and other mmcs dropped by DK (2017)
		drop if inlist(mmc,13007)
		
		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_drop.dta", keep(3) nogen
		drop if mmc_drop==1
		
		cap drop if inlist(cbo942d,31,22,37)
		
		* Bring in mesoregion for SEs
		merge m:1 mmc using "${public}/other/DK (2017)/ReplicationFiles/Data_other/mmc_1991_2010_to_c_mesoreg.dta", keep(3) nogen
		ren c_mesoreg mesoreg

		* Merge in earnings premia
		if "`group'"=="none"{
			merge 1:1 mmc year using "${monopsonies}/sas/rais_lnearn_premia_mmc_`premiadate'.dta", keep(1 3) nogen 
			*gen double mmc_main =mmc
			*merge 1:1 mmc_main year using "${monopsonies}/sas/rais_lnearn_premia_wSE_mmc_`premiadate'.dta", keep(1 3) nogen 
			*drop mmc_main
		}
		else{
			merge 1:1 mmc `group' year using "${monopsonies}/sas/rais_lnearn_premia_mmc_`group'_`premiadate'.dta", keep(1 3) nogen 
		}
		
		* Merge in wage premia Herfindahl
		if "`group'"=="none"{
			merge 1:1 mmc year using "${monopsonies}/sas/earnings_premia_Herfindhal_mmc_`group'.dta", keep(1 3) nogen
		}
		else{
			merge 1:1 mmc `group' year using "${monopsonies}/sas/earnings_premia_Herfindhal_mmc_`group'.dta", keep(1 3) nogen
		}
		
		* Keep balanced panel
		*keep if mkt_emp>=10
		bys mmc `group': `tool'egen totyears = count(year)
		egen double maxy = max(totyears)

		keep if totyears==maxy
		drop maxy totyears
		
		* Baseline employment as weight
		gen double weight0  = 1
		local baseyear = 1991
		`tool'egen weight1 		= mean(cond(year==`baseyear',mkt_emp,.)), by(mmc `group')
		
		* Non-tradables employment
		gen double mkt_ntemp = mkt_emp - mkt_temp
		
		* Non-exporter tradables employment
		gen double mkt_nexplibTemp = mkt_temp - explib_emp
		
		* All except exporter employment
		gen double mkt_nexplibemp = mkt_emp - explib_emp	
		
		********* Shares *********
		gen double expshare 		= explib_emp/mkt_emp
		gen double nexpshare 		= 1-expshare
		
		gen double Tnexpshare		= mkt_nexplibTemp/mkt_emp
		gen double NTshare			= mkt_ntemp/mkt_emp
		
		* Reverse sign of ICE so can more easily interpret, if no ICE (no tradables) replace with zero
		foreach var of varlist ice*{
			replace `var' = 0 if missing(`var')
			replace `var' = - `var'
		}
		
		* Log
		foreach var of varlist mkt_avgdearn *emp* *bill* *firms*{
				if regexm("`var'","hf_")>0 {
				
					*  Scale Herfindahl before making inverse hyp sign
					qui gen `var'scaled = `var'*100
					gen double ln`var' = ln(`var'scaled + sqrt(`var'scaled^2 +1))
					drop `var'scaled
				}
				else{
					qui replace `var' = ln(`var'+ sqrt(`var'^2 +1))
					ren `var' ln`var'
				}
			} 
			
			
		* Shorten names
		renvars lntop25_1991_emp lnmid2550_1991_emp lnmid5075_1991_emp lnbot25_1991_emp ///
			lntop25_t_1991_emp lnmid2550_t_1991_emp lnmid5075_t_1991_emp lnbot25_t_1991_emp ///
			lntop5_1991_emp lnbot95_1991_emp lntop10_1991_emp lnbot90_1991_emp ///
			lngt100_1991_emp lnlt100_1991_emp lngt1000_1991_emp lnlt1000_1991_emp ///
			lntop5_t_1991_emp lnbot95_t_1991_emp lntop10_t_1991_emp lnbot90_t_1991_emp ///
			lngt100_t_1991_emp lnlt100_t_1991_emp lngt1000_t_1991_emp lnlt1000_t_1991_emp, ///
			subst("1991_emp" "91e")
		
		ds /* b_main  b_noind */ ///
			lnmkt_avgdearn lnmkt_emp davg* dprem* ///
			hf_wdbill hf_pdbill hf_emp  ///
			lnhf_wdbill lnhf_pdbill lnhf_emp ///
			lnmkt_ntemp lnmkt_temp lnmkt_firms lnexplib_firms ///
			lnexplib_emp lnmkt_nexplibTemp lnmkt_nexplibemp ///
			expshare nexpshare Tnexpshare NTshare ///
			lntop25_91e lnmid2550_91e lnmid5075_91e lnbot25_91e ///
			lntop25_t_91e lnmid2550_t_91e lnmid5075_t_91e lnbot25_t_91e ///
			lntop5_91e lnbot95_91e lntop10_91e lnbot90_91e ///
			lngt100_91e lnlt100_91e lngt1000_91e lnlt1000_91e ///
			lntop5_t_91e lnbot95_t_91e lntop10_t_91e lnbot90_t_91e ///
			lngt100_t_91e lnlt100_t_91e lngt1000_t_91e lnlt1000_t_91e 
		
		local regvars "`r(varlist)'"
		keep mmc mesoreg `group' year ice* weight* `regvars' /* se_main se_noind */
		
		* Long differences
		foreach var of varlist `regvars'{
			bys mmc `group': egen double `var'`baseyear' = max(cond(year==`baseyear',`var',.))
			
			gen double D`var' = (`var' - `var'`baseyear')
			replace D`var' = -D`var' if year<`baseyear'
			drop `var'`baseyear'
		}
		
		ren ice_bdwtrains		icebW
		ren ice_btrains			icebE
		ren icet_dwtrains 		iceWt
		ren ice_dwtrains 		iceW
		ren ice_dwerptrains 	iceWErp
		ren ice_trains  		iceE
		ren ice_dwtrains_hf 	iceH
		
		ren mesoreg meso
		
		tempfile regsfile
		sa `regsfile'
		
		
		local clustnone   "mmc meso"
		local clustcbo942d "llm 2way "
		
		local clustnonemain "mmc"
		local clustcbo942dmain "llm"
		
		local specsnone "y ny"
		*local specscbo942d "y ny Ty Tny"
		local specscbo942d "y ny"
		
		local 2way 		"mmc cbo942d"
		local llm  		"llm"
		local meso 		"meso"
		local mmc		"mmc"

		************************************
		foreach clust in `clust`group''{
		foreach w in 0 1{
		foreach ice in `allice'{
			
			u `regsfile', clear
			
			* Interactions with year
			forvalues y=1986/2000{
			
				gen double fe_`y' = (year==`y')
				gen double ICE_`y' = `ice'*(year==`y')
			}
			
			* Ommit base year
			renvars ICE_`baseyear' fe_`baseyear', pref("om_")
			
			egen double llm = group(mmc `group')
			
			sum weight1, detail
			gen double ICEX = `ice'*(year>1994)
			gen double ICEtrend =`ice'*(`baseyear' - year)
			
			foreach x in `regvars'{
			
				* De-trending
				reghdfe D`x' ICEtrend [w=weight`w'] if year<=1989, absorb(llm year) vce(cluster ``clust'')

				gen double nres`x' = D`x'
				replace nres`x' = D`x' - _b[ICEtrend]*`ice'*(`baseyear' - year) if year>1989

				foreach var in i_llm i_year fe_llm fe_year res pred{
					cap drop `var'
				}
				
				* Annual specification in stacked mode
					* Levels
						reghdfe D`x' ICE_* fe_* [w=weight`w'], absorb(llm) vce(cluster ``clust'')
						
						mat b  = r(table)[1,1..14]
						mat se = r(table)[2,1..14]
						mat ll = r(table)[5,1..14]
						mat ul = r(table)[6,1..14]
						mat y`x' =(b \ se \ ll \ ul)
						count if e(N)
						local Ny`x' = `r(N)'
						
					* De-trended
						* Adjust dof to account for estimated coefficient used
						reghdfe nres`x' ICE_* fe_* [w=weight`w'], absorb(llm) vce(cluster ``clust'')
						local ndf = `e(df_r)'-1
						local adj = `e(df_r)'/`ndf'
						mat v1 = e(V)
						mat V = `adj'*v1
						mat se = vecdiag(cholesky(diag(vecdiag(V))))
						
						mat b = r(table)
						mat b  = b[1,1..14]
						mat se = se[1,1..14]
						
						mat ll = b - 1.96*se
						mat ul = b + 1.96*se
						
						mat ny`x' = (b \ se \ ll \ ul)
						count if e(N)
						local Nny`x' = `r(N)'
			/*			
				* Annual specification in stacked mode including mmc#year and cbo#year trends
					if "`group'"=="cbo942d" & "`ice'"=="`icemain'"{
					* Levels
						reghdfe D`x' ICE_* [w=weight`w'], absorb(llm mmc#year `group'#year) vce(cluster `clust`group'')
			
						mat b  = r(table)[1,1..14]
						mat se = r(table)[2,1..14]
						mat ll = r(table)[5,1..14]
						mat ul = r(table)[6,1..14]
						mat Ty`x' = (b \ se \ ll \ ul)
						count if e(N)
						local NTy`x' = `r(N)'
						
					* De-trended
						* Adjust dof to account for estimated coefficient used
						local adjdof = `dof'-1
						reghdfe D`x' ICE_* [w=weight`w'], absorb(llm mmc#year `group'#year) vce(cluster `clust`group'')
						
						local ndf = `e(df_r)'-1
						local adj = `e(df_r)'/`ndf'
						mat v1 = e(V)
						mat V = `adj'*v1
						mat se = vecdiag(cholesky(diag(vecdiag(V))))
						
						mat b  = r(table)[1,1..14]
						mat se = se[1,1..14]
						
						mat ll = b - 1.96*se
						mat ul = b - 1.96*se
						
						mat Tny`x' = (b \ se \ ll \ ul)
						count if e(N)
						local NTny`x' = `r(N)'
					}
				*/
			} /* Close regvars */

			
			clear
			foreach x in `regvars'{
				foreach stype in `specs`group''{
					
					clear
					svmat `stype'`x'
					gen outcome = "D`x'"
					gen weight = "`w'"
					gen ice = "`ice'"
					gen stat = ""
					gen clust = "`clust'"
					replace stat = "b" if _n==1
					replace stat = "se" if _n==2
					/*
					replace stat = "t"  if _n==3
					replace stat = "pvalue" if _n==4
					*/
					replace stat = "ll" if _n==3
					replace stat = "ul" if _n==4
					gen double N = `N`stype'`x''
					
					keep if !missing(stat)
					
					ren `stype'`x'1 y1986
					ren `stype'`x'2 y1987
					ren `stype'`x'3 y1988
					ren `stype'`x'4 y1989
					ren `stype'`x'5 y1990
					ren `stype'`x'6 y1992
					ren `stype'`x'7 y1993
					ren `stype'`x'8 y1994
					ren `stype'`x'9 y1995
					ren `stype'`x'10 y1996
					ren `stype'`x'11 y1997
					ren `stype'`x'12 y1998
					ren `stype'`x'13 y1999
					ren `stype'`x'14 y2000
					
					order weight ice outcome stat clust N
					
					tempfile `ice'`stype'`x'`w'`clust'
					sa ``ice'`stype'`x'`w'`clust''
				}
			}
		} /* Close ice */
		} /* Close weight */
		} /* Close clust */
	
	u ``icemain'ylnmkt_emp1`clust`group'main'', clear
	foreach x in `regvars'{
		foreach clust in `clust`group''{
		foreach w in 0 1{
		foreach ice in `allice'{
		append using ``ice'y`x'`w'`clust''
		}
		}
		}
	}
	duplicates drop
	
	outsheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_mktregs_`group'.csv", comma replace
	
	u ``icemain'nylnmkt_emp1`clust`group'main'', clear
	foreach w in 0 1{
		foreach clust in `clust`group''{
		foreach x in `regvars'{
		foreach ice in `allice'{
		append using ``ice'ny`x'`w'`clust''
		}
		}
	}
	}
	duplicates drop
	
	outsheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_residual_mktregs_`group'.csv", comma replace
	/*
	if "`group'"=="cbo942d"{
		u ``icemain'Tylnmkt_emp1', clear
		foreach w in 0 1{
		foreach x in `regvars'{
			foreach ice in `icemain'{
			append using ``ice'Ty`x'`w''
			}
		}
		}
		duplicates drop
		
		outsheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_moy_mktregs_`group'.csv", comma replace
		
		u ``icemain'Tnylnmkt_emp1', clear
		foreach w in 0 1{
		foreach x in `regvars'{
			foreach ice in `icemain'{
			append using ``ice'Tny`x'`w''
			}
		}
		}
		duplicates drop
		
		outsheet using "${monopsonies}/csv/`regsdate'/DD_stacked_annual_residual_moy_mktregs_`group'.csv", comma replace
	}
	*/
	
	} /* close group */
}	
