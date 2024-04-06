******************************************************************************
* table_3.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates Table 3, using Census data to measure the effects of regional
* tariff reductions on regional working-age population.
*
* Output: table_3.xls - regression output for Table 3
******************************************************************************

cd "${root}Codes_Census"
log using table_3.txt, text replace

*********************************************
* Calculate working-age population in each Census year
* - code_sample files already restrict the sample to working-age individuals

****************
* 1970

use ../Data_Census/code_sample_1970, clear

* variable setup
drop if mmc1970 == 26019 // drop Fernando de Noronha
codebook mmc1970 // 412 regions
gen pop = 1

* calculate regional totals for each group and associated standard errors
foreach v in pop {
  gen z_`v' = xweighti * `v'
  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
  gen se_element_`v' = (z_`v' - zbar_`v')^2
}
gen one = 1
collapse (sum) pop ///
         (rawsum) se_sum_pop = se_element_pop ///
	              obs=one ///
         [pw=xweighti], by(mmc1970 year)
foreach v in pop {
  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
  drop se_sum_`v'
}
drop obs

* save counts and standard errors
reshape wide pop ///
             se_pop ///
			 , i(mmc1970) j(year)
sort mmc1970
save population_mmc1970_1970, replace

****************
* 1980

use ../Data_Census/code_sample_1980, clear

* variable setup
* Fernando de Noronha does not appear in 1980
codebook mmc1970 // 412 regions
gen pop = 1

* calculate regional totals for each group and associated standard errors
foreach v in pop {
  gen z_`v' = xweighti * `v'
  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
  gen se_element_`v' = (z_`v' - zbar_`v')^2
}
gen one = 1
collapse (sum) pop ///
         (rawsum) se_sum_pop = se_element_pop ///
	              obs=one ///
         [pw=xweighti], by(mmc1970 year)
foreach v in pop {
  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
  drop se_sum_`v'
}
drop obs

* save counts and standard errors
reshape wide pop ///
             se_pop ///
			 , i(mmc1970) j(year)
sort mmc1970
save population_mmc1970_1980, replace

****************
* 1991

use ../Data_Census/code_sample if year==1991, clear

* variable setup
drop if mmc1970 == 26019 // Fernando de Noronha - ensures same # of MMC1970 in 1980 and later
codebook mmc1970 // 412 regions
gen pop = 1

* calculate regional totals for each group and associated standard errors
foreach v in pop {
  gen z_`v' = xweighti * `v'
  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
  gen se_element_`v' = (z_`v' - zbar_`v')^2
}
gen one = 1
collapse (sum) pop ///
         (rawsum) se_sum_pop = se_element_pop ///
	              obs=one ///
         [pw=xweighti], by(mmc1970 year)
foreach v in pop {
  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
  drop se_sum_`v'
}
drop obs

* save counts and standard errors
reshape wide pop ///
             se_pop ///
			 , i(mmc1970) j(year)
sort mmc1970
save population_mmc1970_1991, replace

****************
* 2000, 2010

foreach yr in 2000 2010 {

	use ../Data_Census/code_sample if year==`yr', clear

	* variable setup
	drop if mmc1970 == 26019 // Fernando de Noronha - ensures same # of MMC1970 in 1980 and later
	codebook mmc1970 // 412 regions
	gen pop = 1

	* calculate regional totals for each group and associated standard errors
	foreach v in pop {
	  gen z_`v' = xweighti * `v'
	  bysort year mmc1970: egen zbar_`v' = mean(z_`v')
	  gen se_element_`v' = (z_`v' - zbar_`v')^2
	}
	gen one = 1
	collapse (sum) pop ///
			 (rawsum) se_sum_pop = se_element_pop ///
					  obs=one ///
			 [pw=xweighti], by(mmc1970 year)
	foreach v in pop {
	  gen se_`v' = sqrt((obs/(obs-1))*se_sum_`v')
	  drop se_sum_`v'
	}
	drop obs

	* save counts and standard errors
	reshape wide pop ///
				 se_pop ///
				 , i(mmc1970) j(year)
	sort mmc1970
	save population_mmc1970_`yr', replace
}

*********************************************
* Calculate changes in log population and associated standard errors

* merge all population files
use population_mmc1970_1970
foreach yr in 1980 1991 2000 2010 {
	merge 1:1 mmc1970 using population_mmc1970_`yr'
	drop _merge // pop perfect match
}

* calculate log population
foreach yr in 1970 1980 1991 2000 2010 {
	gen ln_pop`yr' = ln(pop`yr')
}

* calculate changes and associated standard errors

* 1970-1980
gen dln_pop_70_80 = ln_pop1980 - ln_pop1970
gen dln_popse_70_80 = sqrt( (1/pop1980)^2 * se_pop1980^2 + (1/pop1970)^2 * se_pop1970^2 )

* 1980-1991
gen dln_pop_80_91 = ln_pop1991 - ln_pop1980
gen dln_popse_80_91 = sqrt( (1/pop1991)^2 * se_pop1991^2 + (1/pop1980)^2 * se_pop1980^2 )

* 1991-2000
gen dln_pop_91_00 = ln_pop2000 - ln_pop1991
gen dln_popse_91_00 = sqrt( (1/pop2000)^2 * se_pop2000^2 + (1/pop1991)^2 * se_pop1991^2 )

* 1991-2010
gen dln_pop_91_10 = ln_pop2010 - ln_pop1991
gen dln_popse_91_10 = sqrt( (1/pop2010)^2 * se_pop2010^2 + (1/pop1991)^2 * se_pop1991^2 )

* output results
sort mmc1970
keep mmc1970 dln* ln_*
save dln_population_mmc1970, replace

* remove intermediate data files
foreach yr in 1970 1980 1991 2000 2010 {
	erase  population_mmc1970_`yr'.dta
}

*********************************************
* Population regressions for Table 3

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

* earnings outcomes and pretrends
merge 1:1 mmc1970 using dln_population_mmc1970
drop _merge // perfect match

* restrict sample
drop if mmc1970_drop==1
codebook mmc1970 // 405 observations

* summary statistics
sum dln_pop_91_00
sum dln_pop_91_10

**************
* regressions

* 2000

reg dln_pop_91_00 rtr_kume_main dln_pop_80_91 stflag2-stflag27 ///
    [aw=dln_popse_91_00^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_3.xls, replace ctitle("2000") sortvar(rtr_kume_main dln_pop_80_91 dln_pop_70_80)

reg dln_pop_91_00 rtr_kume_main dln_pop_70_80 stflag2-stflag27 ///
    [aw=dln_popse_91_00^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_3.xls, append ctitle("2000") sortvar(rtr_kume_main dln_pop_80_91 dln_pop_70_80)

reg dln_pop_91_00 rtr_kume_main dln_pop_80_91 dln_pop_70_80 stflag2-stflag27 ///
    [aw=dln_popse_91_00^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_3.xls, append ctitle("2000") sortvar(rtr_kume_main dln_pop_80_91 dln_pop_70_80)
		
* 2010

reg dln_pop_91_10 rtr_kume_main dln_pop_80_91 stflag2-stflag27 ///
    [aw=dln_popse_91_10^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_3.xls, append ctitle("2010") sortvar(rtr_kume_main dln_pop_80_91 dln_pop_70_80)

reg dln_pop_91_10 rtr_kume_main dln_pop_70_80 stflag2-stflag27 ///
    [aw=dln_popse_91_10^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_3.xls, append ctitle("2010") sortvar(rtr_kume_main dln_pop_80_91 dln_pop_70_80)

reg dln_pop_91_10 rtr_kume_main dln_pop_80_91 dln_pop_70_80 stflag2-stflag27 ///
    [aw=dln_popse_91_10^-2], cluster(c_mesoreg)
outreg2 using ../Results/CensusOther/table_3.xls, append ctitle("2010") sortvar(rtr_kume_main dln_pop_80_91 dln_pop_70_80)



log close
cd "${root}"
