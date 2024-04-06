******************************************************************************
* dln_employment.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates the change in log formal and overall employment by consistent
* microregions spanning 1970-2010.
*
* Output: /Data/dln_employment.dta
******************************************************************************

cd "${root}Codes_Census"
log using dln_employment.txt, text replace

*********************************************
* Calculate regional employment in each Census year

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
foreach v in all prev_formemp {
  gen z_`v' = xweighti * `v'
  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
  gen se_element_`v' = (z_`v' - zbar_`v')^2
}
gen one = 1
collapse (sum) all prev_formemp ///
         (rawsum) se_sum_all = se_element_all ///
	              se_sum_prev_formemp=se_element_prev_formemp ///
	              obs=one ///
         [pw=xweighti], by(mmc1970 year)
foreach v in all prev_formemp {
  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
  drop se_sum_`v'
}
drop obs

* save counts and standard errors
reshape wide all prev_formemp ///
             se_all se_prev_formemp ///
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
foreach v in all prev_formemp formemp {
  gen z_`v' = xweighti * `v'
  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
  gen se_element_`v' = (z_`v' - zbar_`v')^2
}
gen one = 1
collapse (sum) all prev_formemp formemp ///
         (rawsum) se_sum_all = se_element_all ///
	              se_sum_prev_formemp=se_element_prev_formemp ///
	              se_sum_formemp=se_element_formemp ///
	              obs=one ///
         [pw=xweighti], by(mmc1970 year)
foreach v in all prev_formemp formemp {
  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
  drop se_sum_`v'
}
drop obs

* save counts and standard errors
reshape wide all prev_formemp formemp ///
             se_all se_prev_formemp se_formemp ///
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
	foreach v in all formemp {
	  gen z_`v' = xweighti * `v'
	  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
	  gen se_element_`v' = (z_`v' - zbar_`v')^2
	}
	gen one = 1
	collapse (sum) all formemp ///
			 (rawsum) se_sum_all = se_element_all ///
					  se_sum_formemp=se_element_formemp ///
					  obs=one ///
			 [pw=xweighti], by(mmc1970 year)
	foreach v in all formemp {
	  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
	  drop se_sum_`v'
	}
	drop obs

	* save counts and standard errors
	reshape wide all formemp ///
				 se_all se_formemp ///
				 , i(mmc1970) j(year)
	sort mmc1970
	save employment_`yr', replace
}

*********************************************
* Calculate changes in employment and associated standard errors

* merge all employment files
use employment_1970, clear
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
}
foreach yr in 1991 2000 2010 {
	gen ln_formemp`yr' = ln(formemp`yr')
}


*********
* calculate changes and associated standard errors

* 1970-1980
gen dln_emp_all_70_80 = ln_all1980 - ln_all1970
gen dln_empse_all_70_80 = sqrt( (1/all1980)^2 * se_all1980^2 + (1/all1970)^2 * se_all1970^2 )

* 1980-1991
gen dln_emp_all_80_91 = ln_all1991 - ln_all1980
gen dln_empse_all_80_91 = sqrt( (1/all1991)^2 * se_all1991^2 + (1/all1980)^2 * se_all1980^2 )
gen dln_emp_prev_formemp_80_91 = ln_prev_formemp1991 - ln_prev_formemp1980
gen dln_empse_prev_formemp_80_91 = sqrt( (1/prev_formemp1991)^2 * se_prev_formemp1991^2 + (1/prev_formemp1980)^2 * se_prev_formemp1980^2 )

* 1991-2000
gen dln_emp_all_91_00 = ln_all2000 - ln_all1991
gen dln_empse_all_91_00 = sqrt( (1/all2000)^2 * se_all2000^2 + (1/all1991)^2 * se_all1991^2 )
gen dln_emp_formemp_91_00 = ln_formemp2000 - ln_formemp1991
gen dln_empse_formemp_91_00 = sqrt( (1/formemp2000)^2 * se_formemp2000^2 + (1/formemp1991)^2 * se_formemp1991^2 )

* 1991-2010
gen dln_emp_all_91_10 = ln_all2010 - ln_all1991
gen dln_empse_all_91_10 = sqrt( (1/all2010)^2 * se_all2010^2 + (1/all1991)^2 * se_all1991^2 )
gen dln_emp_formemp_91_10 = ln_formemp2010 - ln_formemp1991
gen dln_empse_formemp_91_10 = sqrt( (1/formemp2010)^2 * se_formemp2010^2 + (1/formemp1991)^2 * se_formemp1991^2 )

*********
* output results

sort mmc1970
keep mmc1970 dln* ln_*
save ../Data/dln_employment, replace

*********
* clean up intermediate files

foreach yr in 1970 1980 1991 2000 2010 {
	erase employment_`yr'.dta
}


log close
cd "${root}"
