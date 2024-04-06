******************************************************************************
* dln_earnings.do
* Dix-Carneiro and Kovak AER replication files
*
* Calculates the change in log formal and overall earnings premia and
* associated standard errors using Census data.
*
* Output: /Data/dln_earnings.dta
******************************************************************************

cd "${root}Codes_Census"
log using dln_earnings.txt, text replace
set matsize 10000

*********************************************
* Calculate earnings premia in each Census year

****************
* 1970

use ../Data_Census/code_sample_1970, clear

* restrict sample
keep if employed==1 // employed
drop if indlinkn==98 // drop public admin
keep if indlinkn < . // valid industry code
		
* variable setup
gen lnrymain = ln(rymain)
drop if mmc1970 == 26019 // drop Fernando de Noronha
codebook mmc1970 // 412 regions
tab mmc1970, gen(mmc1970flag)

* premium regression (all workers)
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female edflag2-edflag17 ///
             indlinknflag2-indlinknflag20 ///
			 [pw=xweighti], robust noc
est save earnings_premia_1970, replace

* Extract MMC1970 coefficients and standard errors from regression
do mmc1970_coef_extract
rename var earn_all_1970
rename se earnse_all_1970
save earn_all_1970, replace  
erase earnings_premia_1970.ster

****************
* 1980

use ../Data_Census/code_sample_1980, clear

* restrict sample
keep if employed==1 // employed
drop if indlinkn==98 // drop public admin
keep if indlinkn < . // valid industry code

* variable setup
gen lnrymain = ln(rymain)
* Fernando de Noronha does not appear in 1980
codebook mmc1970 // 412 regions
tab mmc1970, gen(mmc1970flag)
gen prev_selfemp = (prev_nonformemp == 1 & prev_anonformemp == 0)

* premium regression (all workers)
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female edflag2-edflag18 ///
             indlinknflag2-indlinknflag20 ///
			 prev_anonformemp prev_selfemp ///
			 [pw=xweighti], robust noc
est save earnings_premia_all_1980, replace

* Extract MMC1970 coefficients and standard errors from regression
preserve
do mmc1970_coef_extract
rename var earn_all_1980
rename se earnse_all_1980
save earn_all_1980, replace  
erase earnings_premia_all_1980.ster
restore 

* premium regression (formal (previdencia) workers)
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female edflag2-edflag18 ///
             indlinknflag2-indlinknflag20 ///
			 if prev_formemp==1 [pw=xweighti], robust noc
est save earnings_premia_prev_formemp_1980, replace

* Extract MMC1970 coefficients and standard errors from regression
preserve
do mmc1970_coef_extract
rename var earn_prev_formemp_1980
rename se earnse_prev_formemp_1980
save earn_prev_formemp_1980, replace  
erase earnings_premia_prev_formemp_1980.ster
restore

****************
* 1991

use ../Data_Census/code_sample if year==1991, clear

* restrict sample
keep if employed==1 // employed
drop if indlinkn==98 // drop public admin
keep if indlinkn < . // valid industry code

* variable setup
gen lnrymain = ln(rymain)
drop if mmc1970 == 26019 // Fernando de Noronha - ensures same # of MMC1970 in 1980 and later
codebook mmc1970 // 412 regions
tab mmc1970, gen(mmc1970flag)
gen selfemp = (nonformemp == 1 & anonformemp == 0)
gen prev_selfemp = (prev_nonformemp == 1 & prev_anonformemp == 0)

* premium regression (all workers)
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female edflag2-edflag18 ///
             indlinknflag2-indlinknflag20 ///
			 anonformemp selfemp ///
			 [pw=xweighti], robust noc
est save earnings_premia_all_1991, replace

* Extract MMC1970 coefficients and standard errors from regression
preserve
do mmc1970_coef_extract
rename var earn_all_1991
rename se earnse_all_1991
save earn_all_1991, replace  
erase earnings_premia_all_1991.ster
restore 

* premium regression (formal (previdencia) workers)
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female edflag2-edflag18 ///
             indlinknflag2-indlinknflag20 ///
			 if prev_formemp==1 [pw=xweighti], robust noc
est save earnings_premia_prev_formemp_1991, replace

* Extract MMC1970 coefficients and standard errors from regression
preserve
do mmc1970_coef_extract
rename var earn_prev_formemp_1991
rename se earnse_prev_formemp_1991
save earn_prev_formemp_1991, replace  
erase earnings_premia_prev_formemp_1991.ster
restore

* premium regression (formal (carteira) workers)
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female edflag2-edflag18 ///
             indlinknflag2-indlinknflag20 ///
			 if formemp==1 [pw=xweighti], robust noc
est save earnings_premia_formemp_1991, replace

* Extract MMC1970 coefficients and standard errors from regression
preserve
do mmc1970_coef_extract
rename var earn_formemp_1991
rename se earnse_formemp_1991
save earn_formemp_1991, replace  
erase earnings_premia_formemp_1991.ster
restore

****************
* 2000, 2010

foreach yr in 2000 2010 {

	use ../Data_Census/code_sample if year==`yr', clear

	* restrict sample
	keep if employed==1 // employed
	drop if indlinkn==98 // drop public admin
	keep if indlinkn < . // valid industry code

	* variable setup
	gen lnrymain = ln(rymain)
	drop if mmc1970 == 26019 // Fernando de Noronha - ensures same # of MMC1970 in 1980 and later
	codebook mmc1970 // 412 regions
	tab mmc1970, gen(mmc1970flag)
	gen selfemp = (nonformemp == 1 & anonformemp == 0)
	gen prev_selfemp = (prev_nonformemp == 1 & prev_anonformemp == 0)

	* premium regression (all workers)
	reg lnrymain mmc1970flag1-mmc1970flag412 ///
				 ageflag2-ageflag5 female edflag2-edflag18 ///
				 indlinknflag2-indlinknflag20 ///
				 anonformemp selfemp ///
				 [pw=xweighti], robust noc
	est save earnings_premia_all_`yr', replace

	* Extract MMC1970 coefficients and standard errors from regression
	preserve
	do mmc1970_coef_extract
	rename var earn_all_`yr'
	rename se earnse_all_`yr'
	save earn_all_`yr', replace  
	erase earnings_premia_all_`yr'.ster
	restore 

	* premium regression (formal (carteira) workers)
	reg lnrymain mmc1970flag1-mmc1970flag412 ///
				 ageflag2-ageflag5 female edflag2-edflag18 ///
				 indlinknflag2-indlinknflag20 ///
				 if formemp==1 [pw=xweighti], robust noc
	est save earnings_premia_formemp_`yr', replace

	* Extract MMC1970 coefficients and standard errors from regression
	preserve
	do mmc1970_coef_extract
	rename var earn_formemp_`yr'
	rename se earnse_formemp_`yr'
	save earn_formemp_`yr', replace 
	erase earnings_premia_formemp_`yr'.ster
	restore
}


*********************************************
* Calculate changes log earnings premia and associated standard errors

* merge all earnings premium files
use earn_all_1970, clear
foreach yr in 1980 1991 2000 2010 {
	merge 1:1 mmc1970 using earn_all_`yr'
	drop _merge // all perfect match
}
foreach yr in 1980 1991 {
	merge 1:1 mmc1970 using earn_prev_formemp_`yr'
	drop _merge // all perfect match
}
foreach yr in 1991 2000 2010 {
	merge 1:1 mmc1970 using earn_formemp_`yr'
	drop _merge // all perfect match
}

* calculate changes and associated standard errors

* 1970-1980
gen dln_earn_all_70_80 = earn_all_1980 - earn_all_1970
gen dln_earnse_all_70_80 = sqrt(earnse_all_1980^2 + earnse_all_1970^2)

* 1980-1991
gen dln_earn_all_80_91 = earn_all_1991 - earn_all_1980
gen dln_earnse_all_80_91 = sqrt(earnse_all_1991^2 + earnse_all_1980^2)
gen dln_earn_prev_formemp_80_91 = earn_prev_formemp_1991 - earn_prev_formemp_1980
gen dln_earnse_prev_formemp_80_91 = sqrt(earnse_prev_formemp_1991^2 + earnse_prev_formemp_1980^2)

* 1991-2000
gen dln_earn_all_91_00 = earn_all_2000 - earn_all_1991
gen dln_earnse_all_91_00 = sqrt(earnse_all_2000^2 + earnse_all_1991^2)
gen dln_earn_formemp_91_00 = earn_formemp_2000 - earn_formemp_1991
gen dln_earnse_formemp_91_00 = sqrt(earnse_formemp_2000^2 + earnse_formemp_1991^2)

* 1991-2010
gen dln_earn_all_91_10 = earn_all_2010 - earn_all_1991
gen dln_earnse_all_91_10 = sqrt(earnse_all_2010^2 + earnse_all_1991^2)
gen dln_earn_formemp_91_10 = earn_formemp_2010 - earn_formemp_1991
gen dln_earnse_formemp_91_10 = sqrt(earnse_formemp_2010^2 + earnse_formemp_1991^2)

* output results
sort mmc1970
save ../Data/dln_earnings, replace

* delete intermediate files
erase earn_all_1970.dta
foreach yr in 1980 1991 2000 2010 {
	erase earn_all_`yr'.dta
}
foreach yr in 1980 1991 {
	erase earn_prev_formemp_`yr'.dta
}
foreach yr in 1991 2000 2010 {
	erase earn_formemp_`yr'.dta
}


log close
cd "${root}"
