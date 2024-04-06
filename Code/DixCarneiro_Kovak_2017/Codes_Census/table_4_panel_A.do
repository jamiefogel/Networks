******************************************************************************
* table_4_panel_A.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates Table 4 Panel A, using Census data to measure the effects of
* regional tariff reductions on regional informal employment.
*
* Output: table_4_panel_A.xls - regression output for Table 4 Panel A
******************************************************************************

cd "${root}Codes_Census"
log using table_4_panel_A.txt, text replace

*********************************************
* Calculate employment categories in each Census year

****************
* 1970

use ../Data_Census/code_sample_1970, clear

* restrict sample
keep if employed==1 // employed
drop if indlinkn==98 // drop public admin
keep if indlinkn < . // valid industry code
		
* variable setup
drop if mmc1970 == 26019 // drop Fernando de Noronha
codebook mmc1970 // 412 regions
tab mmc1970, gen(mmc1970flag)
gen all = 1

* calculate regional totals for each group and associated standard errors
foreach v in all {
  gen z_`v' = xweighti * `v'
  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
  gen se_element_`v' = (z_`v' - zbar_`v')^2
}
gen one = 1
collapse (sum) all ///
         (rawsum) se_sum_all = se_element_all ///
	              obs=one ///
         [pw=xweighti], by(mmc1970 year)
foreach v in all {
  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
  drop se_sum_`v'
}
drop obs

* save counts and standard errors
reshape wide all ///
             se_all ///
			 , i(mmc1970) j(year)
sort mmc1970
save employment_1970, replace

****************
* 1980

use ../Data_Census/code_sample_1980, clear

* restrict sample
keep if employed==1 // employed
drop if indlinkn==98 // drop public admin
keep if indlinkn < . // valid industry code
		
* variable setup
* Fernando de Noronha does not appear in 1980
codebook mmc1970 // 412 regions
tab mmc1970, gen(mmc1970flag)
gen all = 1

* calculate regional totals for each group and associated standard errors
foreach v in all prev_formemp prev_nonformemp {
  gen z_`v' = xweighti * `v'
  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
  gen se_element_`v' = (z_`v' - zbar_`v')^2
}
gen one = 1
collapse (sum) all prev_formemp prev_nonformemp ///
         (rawsum) se_sum_all = se_element_all ///
	              se_sum_prev_formemp=se_element_prev_formemp ///
	              se_sum_prev_nonformemp=se_element_prev_nonformemp ///
	              obs=one ///
         [pw=xweighti], by(mmc1970 year)
foreach v in all prev_formemp prev_nonformemp {
  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
  drop se_sum_`v'
}
drop obs

* save counts and standard errors
reshape wide all prev_formemp prev_nonformemp ///
             se_all se_prev_formemp se_prev_nonformemp ///
			 , i(mmc1970) j(year)
sort mmc1970
save employment_1980, replace

****************
* 1991

use ../Data_Census/code_sample if year==1991, clear

* restrict sample
keep if employed==1 // employed
drop if indlinkn==98 // drop public admin
keep if indlinkn < . // valid industry code
		
* variable setup
drop if mmc1970 == 26019 // Fernando de Noronha - ensures same # of MMC1970 in 1980 and later
codebook mmc1970 // 412 regions
tab mmc1970, gen(mmc1970flag)
gen all = 1

* calculate regional totals for each group and associated standard errors
foreach v in all prev_formemp prev_nonformemp formemp nonformemp {
  gen z_`v' = xweighti * `v'
  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
  gen se_element_`v' = (z_`v' - zbar_`v')^2
}
gen one = 1
collapse (sum) all prev_formemp prev_nonformemp formemp nonformemp ///
         (rawsum) se_sum_all = se_element_all ///
	              se_sum_prev_formemp=se_element_prev_formemp ///
	              se_sum_prev_nonformemp=se_element_prev_nonformemp ///
	              se_sum_formemp=se_element_formemp ///
	              se_sum_nonformemp=se_element_nonformemp ///
	              obs=one ///
         [pw=xweighti], by(mmc1970 year)
foreach v in all prev_formemp prev_nonformemp formemp nonformemp {
  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
  drop se_sum_`v'
}
drop obs

* save counts and standard errors
reshape wide all prev_formemp prev_nonformemp formemp nonformemp ///
             se_all se_prev_formemp se_prev_nonformemp se_formemp se_nonformemp ///
			 , i(mmc1970) j(year)
sort mmc1970
save employment_1991, replace

****************
* 2000, 2010

foreach yr in 2000 2010 {

	use ../Data_Census/code_sample if year==`yr', clear

	* restrict sample
	keep if employed==1 // employed
	drop if indlinkn==98 // drop public admin
	keep if indlinkn < . // valid industry code
			
	* variable setup
	drop if mmc1970 == 26019 // Fernando de Noronha - ensures same # of MMC1970 in 1980 and later
	codebook mmc1970 // 412 regions
	tab mmc1970, gen(mmc1970flag)
	gen all = 1

	* calculate regional totals for each group and associated standard errors
	foreach v in all formemp nonformemp {
	  gen z_`v' = xweighti * `v'
	  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
	  gen se_element_`v' = (z_`v' - zbar_`v')^2
	}
	gen one = 1
	collapse (sum) all formemp nonformemp ///
			 (rawsum) se_sum_all = se_element_all ///
					  se_sum_formemp=se_element_formemp ///
					  se_sum_nonformemp=se_element_nonformemp ///
					  obs=one ///
			 [pw=xweighti], by(mmc1970 year)
	foreach v in all formemp nonformemp {
	  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
	  drop se_sum_`v'
	}
	drop obs

	* save counts and standard errors
	reshape wide all formemp nonformemp ///
				 se_all se_formemp se_nonformemp ///
				 , i(mmc1970) j(year)
	sort mmc1970
	save employment_`yr', replace
}

*********************************************
* Calculate changes in log employment and associated standard errors

* merge all employment files
use employment_1970
foreach yr in 1980 1991 2000 2010 {
	merge 1:1 mmc1970 using employment_`yr'
	drop _merge // all perfect match
}

* calculate log employment
foreach yr in 1970 1980 1991 2000 2010 {
	gen ln_all`yr' = ln(all`yr')
}
foreach yr in 1980 1991 {
	gen ln_prev_formemp`yr' = ln(prev_formemp`yr')
	gen ln_prev_nonformemp`yr' = ln(prev_nonformemp`yr')
}
foreach yr in 1991 2000 2010 {
	gen ln_formemp`yr' = ln(formemp`yr')
	gen ln_nonformemp`yr' = ln(nonformemp`yr')
}


* calculate changes and associated standard errors

* 1970-1980
gen dln_emp_all_70_80 = ln_all1980 - ln_all1970
gen dln_empse_all_70_80 = sqrt( (1/all1980)^2 * se_all1980^2 + (1/all1970)^2 * se_all1970^2 )

* 1980-1991
gen dln_emp_all_80_91 = ln_all1991 - ln_all1980
gen dln_empse_all_80_91 = sqrt( (1/all1991)^2 * se_all1991^2 + (1/all1980)^2 * se_all1980^2 )
gen dln_emp_prev_formemp_80_91 = ln_prev_formemp1991 - ln_prev_formemp1980
gen dln_empse_prev_formemp_80_91 = sqrt( (1/prev_formemp1991)^2 * se_prev_formemp1991^2 + (1/prev_formemp1980)^2 * se_prev_formemp1980^2 )
gen dln_emp_prev_nonformemp_80_91 = ln_prev_nonformemp1991 - ln_prev_nonformemp1980
gen dln_empse_prev_nonformemp_80_91 = sqrt( (1/prev_nonformemp1991)^2 * se_prev_nonformemp1991^2 + (1/prev_nonformemp1980)^2 * se_prev_nonformemp1980^2 )

* 1991-2000
gen dln_emp_all_91_00 = ln_all2000 - ln_all1991
gen dln_empse_all_91_00 = sqrt( (1/all2000)^2 * se_all2000^2 + (1/all1991)^2 * se_all1991^2 )
gen dln_emp_formemp_91_00 = ln_formemp2000 - ln_formemp1991
gen dln_empse_formemp_91_00 = sqrt( (1/formemp2000)^2 * se_formemp2000^2 + (1/formemp1991)^2 * se_formemp1991^2 )
gen dln_emp_nonformemp_91_00 = ln_nonformemp2000 - ln_nonformemp1991
gen dln_empse_nonformemp_91_00 = sqrt( (1/nonformemp2000)^2 * se_nonformemp2000^2 + (1/nonformemp1991)^2 * se_nonformemp1991^2 )

* 1991-2010
gen dln_emp_all_91_10 = ln_all2010 - ln_all1991
gen dln_empse_all_91_10 = sqrt( (1/all2010)^2 * se_all2010^2 + (1/all1991)^2 * se_all1991^2 )
gen dln_emp_formemp_91_10 = ln_formemp2010 - ln_formemp1991
gen dln_empse_formemp_91_10 = sqrt( (1/formemp2010)^2 * se_formemp2010^2 + (1/formemp1991)^2 * se_formemp1991^2 )
gen dln_emp_nonformemp_91_10 = ln_nonformemp2010 - ln_nonformemp1991
gen dln_empse_nonformemp_91_10 = sqrt( (1/nonformemp2010)^2 * se_nonformemp2010^2 + (1/nonformemp1991)^2 * se_nonformemp1991^2 )

* output results
sort mmc1970
keep mmc1970 dln* ln_*
save dln_employment_mmc1970, replace

* remove intermediate data files
foreach yr in 1970 1980 1991 2000 2010 {
	erase  employment_`yr'.dta
}

*********************************************
* Employment regressions for Table 4 Panel A

**************
* assemble data

clear

* tariff shocks
use ../Data/rtc_kume_mmc1970
sort mmc1970
gen rtr_kume_main = -1 * rtc_kume_main

* mesoregions for clustering
merge 1:1 mmc1970 using ../Data_Other/mmc1970_to_c_mesoreg1970
drop _merge // perfect match

* state fixed effects
gen state = floor(mmc1970/1000)
tab state, gen(stflag)

* sample restriction
merge 1:1 mmc using ../Data_Other/mmc1970_drop
drop _merge // perfect match

* employment outcomes and pretrends
merge 1:1 mmc1970 using dln_employment_mmc1970
drop _merge // perfect match

* restrict sample
drop if mmc1970_drop==1
codebook mmc1970 // 405 observations

* summary statistics
sum dln_emp_nonformemp_91_00
sum dln_emp_nonformemp_91_10

**************
* regressions

* 2000

reg dln_emp_nonformemp_91_00 rtr_kume_main dln_emp_prev_nonformemp_80_91 stflag2-stflag27 ///
    [aw=dln_empse_nonformemp_91_00^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_4_panel_A.xls, replace ctitle("2000") sortvar(rtr_kume_main dln_emp_prev_nonformemp_80_91 dln_emp_all_70_80)

reg dln_emp_nonformemp_91_00 rtr_kume_main dln_emp_all_70_80 stflag2-stflag27 ///
    [aw=dln_empse_nonformemp_91_00^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_4_panel_A.xls, append ctitle("2000") sortvar(rtr_kume_main dln_emp_prev_nonformemp_80_91 dln_emp_all_70_80)

reg dln_emp_nonformemp_91_00 rtr_kume_main dln_emp_prev_nonformemp_80_91 ///
	dln_emp_all_70_80 stflag2-stflag27 ///
    [aw=dln_empse_nonformemp_91_00^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_4_panel_A.xls, append ctitle("2000") sortvar(rtr_kume_main dln_emp_prev_nonformemp_80_91 dln_emp_all_70_80)

* 2010

reg dln_emp_nonformemp_91_10 rtr_kume_main dln_emp_prev_nonformemp_80_91 stflag2-stflag27 ///
    [aw=dln_empse_nonformemp_91_10^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_4_panel_A.xls, append ctitle("2010") sortvar(rtr_kume_main dln_emp_prev_nonformemp_80_91 dln_emp_all_70_80)

reg dln_emp_nonformemp_91_10 rtr_kume_main dln_emp_all_70_80 stflag2-stflag27 ///
    [aw=dln_empse_nonformemp_91_10^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_4_panel_A.xls, append ctitle("2010") sortvar(rtr_kume_main dln_emp_prev_nonformemp_80_91 dln_emp_all_70_80)

reg dln_emp_nonformemp_91_10 rtr_kume_main dln_emp_prev_nonformemp_80_91 ///
	dln_emp_all_70_80 stflag2-stflag27 ///
    [aw=dln_empse_nonformemp_91_10^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_4_panel_A.xls, append ctitle("2010") sortvar(rtr_kume_main dln_emp_prev_nonformemp_80_91 dln_emp_all_70_80)



log close
cd "${root}"
