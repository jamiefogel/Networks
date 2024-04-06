******************************************************************************
* table_4_panel_B.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates Table 4 Panel B, using Census data to measure the effects of
* regional tariff reductions on regional informal earnings premia.
*
* Output: table_4_panel_B.xls - regression output for Table 4 Panel B
******************************************************************************

cd "${root}Codes_Census"
log using table_4_panel_B.txt, text replace
set matsize 10000

*********************************************
* Calculate overall earnings premia in 1970 and 1980 (for pre-trends)

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

* RAIS education bins
gen raised1 = (educ==0)
gen raised2 = inrange(educ,1,3)
gen raised3 = (educ==4)
gen raised4 = inrange(educ,5,7)
gen raised5 = (educ==8)
gen raised6 = inrange(educ,9,10)
gen raised7 = (educ==11)
gen raised8 = inrange(educ,12,14)
gen raised9 = (educ>=15) & educ < .

* all earnings 
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female raised2-raised9 ///
             indlinknflag2-indlinknflag20 ///
			 [pw=xweighti], robust noc
est save earnings_premia_all_1970, replace

* Extract MMC1970 coefficients and standard errors from regression
preserve
est use earnings_premia_all_1970
do mmc1970_coef_extract
rename var earn_all_1970
rename se earnse_all_1970
save earnings_premia_all_1970, replace  
erase earnings_premia_all_1970.ster
restore

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

* RAIS education bins
gen raised1 = (educ==0)
gen raised2 = inrange(educ,1,3)
gen raised3 = (educ==4)
gen raised4 = inrange(educ,5,7)
gen raised5 = (educ==8)
gen raised6 = inrange(educ,9,10)
gen raised7 = (educ==11)
gen raised8 = inrange(educ,12,14)
gen raised9 = (educ>=15) & educ < .

* all earnings 
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female raised2-raised9 ///
             indlinknflag2-indlinknflag20 ///
			 [pw=xweighti], robust noc
est save earnings_premia_all_1980, replace

* Extract MMC1970 coefficients and standard errors from regression
preserve
est use earnings_premia_all_1980
do mmc1970_coef_extract
rename var earn_all_1980
rename se earnse_all_1980
save earnings_premia_all_1980, replace  
erase earnings_premia_all_1980.ster
restore

*********************************************
* Calculate informal earnings premia in 1980-2010

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

* RAIS education bins
gen raised1 = (educ==0)
gen raised2 = inrange(educ,1,3)
gen raised3 = (educ==4)
gen raised4 = inrange(educ,5,7)
gen raised5 = (educ==8)
gen raised6 = inrange(educ,9,10)
gen raised7 = (educ==11)
gen raised8 = inrange(educ,12,14)
gen raised9 = (educ>=15) & educ < .

* informal (previdencia) earnings 
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female raised2-raised9 ///
             indlinknflag2-indlinknflag20 ///
			 if prev_nonformemp==1 [pw=xweighti], robust noc
est save earnings_premia_prev_nonformemp_1980, replace

* Extract MMC1970 coefficients and standard errors from regression
preserve
est use earnings_premia_prev_nonformemp_1980
do mmc1970_coef_extract
rename var earn_prev_nonformemp_1980
rename se earnse_prev_nonformemp_1980
save earnings_premia_prev_nonformemp_1980, replace  
erase earnings_premia_prev_nonformemp_1980.ster
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

* RAIS education bins
gen raised1 = (educ==0)
gen raised2 = inrange(educ,1,3)
gen raised3 = (educ==4)
gen raised4 = inrange(educ,5,7)
gen raised5 = (educ==8)
gen raised6 = inrange(educ,9,10)
gen raised7 = (educ==11)
gen raised8 = inrange(educ,12,14)
gen raised9 = (educ>=15) & educ < .

* informal (previdencia) earnings 
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female raised2-raised9 ///
             indlinknflag2-indlinknflag20 ///
			 if prev_nonformemp==1 [pw=xweighti], robust noc
est save earnings_premia_prev_nonformemp_1991, replace

* informal (carteira) earnings 
reg lnrymain mmc1970flag1-mmc1970flag412 ///
             ageflag2-ageflag5 female raised2-raised9 ///
             indlinknflag2-indlinknflag20 ///
			 if nonformemp==1 [pw=xweighti], robust noc
est save earnings_premia_nonformemp_1991, replace


* Extract MMC1970 coefficients and standard errors from regression
foreach s in prev_nonformemp nonformemp {
	preserve
	est use earnings_premia_`s'_1991
	do mmc1970_coef_extract
	rename var earn_`s'_1991
	rename se earnse_`s'_1991
	save earnings_premia_`s'_1991, replace  
	erase earnings_premia_`s'_1991.ster
	restore
}

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

	* RAIS education bins
	gen raised1 = (educ==0)
	gen raised2 = inrange(educ,1,3)
	gen raised3 = (educ==4)
	gen raised4 = inrange(educ,5,7)
	gen raised5 = (educ==8)
	gen raised6 = inrange(educ,9,10)
	gen raised7 = (educ==11)
	gen raised8 = inrange(educ,12,14)
	gen raised9 = (educ>=15) & educ < .

	* informal (carteira) earnings
	reg lnrymain mmc1970flag1-mmc1970flag412 ///
				ageflag2-ageflag5 female raised2-raised9 ///
				indlinknflag2-indlinknflag20 ///
				if nonformemp==1 [pw=xweighti], robust noc
	est save earnings_premia_nonformemp_`yr', replace
	
	* Extract MMC1970 coefficients and standard errors from regression
	foreach s in nonformemp {
		preserve
		est use earnings_premia_`s'_`yr'
		do mmc1970_coef_extract
		rename var earn_`s'_`yr'
		rename se earnse_`s'_`yr'
		save earnings_premia_`s'_`yr', replace  
		erase earnings_premia_`s'_`yr'.ster
		restore
	}
}

*********************************************
* Calculate changes in log overall earnings premia and associated standard errors

use earnings_premia_all_1970, clear
foreach yr in 1980 {
	merge 1:1 mmc1970 using earnings_premia_all_`yr'
	drop _merge // perfect match
}	

gen dln_earn_all_70_80 = earn_all_1980 - earn_all_1970
gen dln_earnse_all_70_80 = sqrt(earnse_all_1980^2 + earnse_all_1970^2)

sort mmc1970
save dln_earnings_all, replace

* remove intermediate data files
foreach yr in 1970 1980 {
	erase  earnings_premia_all_`yr'.dta
}

*********************************************
* Calculate changes in log informal earnings premia and associated standard errors

* earnings
use earnings_premia_prev_nonformemp_1980
merge 1:1 mmc1970 using earnings_premia_prev_nonformemp_1991
drop _merge // perfect match
merge 1:1 mmc1970 using earnings_premia_nonformemp_1991
drop _merge // perfect match
merge 1:1 mmc1970 using earnings_premia_nonformemp_2000
drop _merge // perfect match
merge 1:1 mmc1970 using earnings_premia_nonformemp_2010
drop _merge // perfect match

gen dln_earn_prev_nonformemp_80_91 = earn_prev_nonformemp_1991 - earn_prev_nonformemp_1980
gen dln_earnse_prev_nonformemp_80_91 = sqrt(earnse_prev_nonformemp_1991^2 + earnse_prev_nonformemp_1980^2)

gen dln_earn_nonformemp_91_00 = earn_nonformemp_2000 - earn_nonformemp_1991
gen dln_earnse_nonformemp_91_00 = sqrt(earnse_nonformemp_2000^2 + earnse_nonformemp_1991^2)

gen dln_earn_nonformemp_91_10 = earn_nonformemp_2010 - earn_nonformemp_1991
gen dln_earnse_nonformemp_91_10 = sqrt(earnse_nonformemp_2010^2 + earnse_nonformemp_1991^2)

sort mmc1970
save dln_earnings_informal, replace

* remove intermediate data files
erase earnings_premia_prev_nonformemp_1980.dta
erase earnings_premia_prev_nonformemp_1991.dta
erase earnings_premia_nonformemp_1991.dta
erase earnings_premia_nonformemp_2000.dta
erase earnings_premia_nonformemp_2010.dta

*********************************************
* Earnings regressions for Table 4 Panel B

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
merge 1:1 mmc1970 using dln_earnings_informal
drop _merge // perfect match
merge 1:1 mmc1970 using dln_earnings_all
drop _merge // perfect match

* restrict sample
drop if mmc1970_drop==1
codebook mmc1970 // 405 observations

* summary statistics
sum dln_earn_nonformemp_91_00
sum dln_earn_nonformemp_91_10

*********************************************
* main regressions

* 2000

reg dln_earn_nonformemp_91_00 rtr_kume_main dln_earn_prev_nonformemp_80_91 stflag2-stflag27 ///
    [aw=dln_earnse_nonformemp_91_00^-2], cluster(c_mesoreg1970)
outreg2 using ../Results/CensusOther/table_4_panel_B.xls, replace ctitle("2000") sortvar(rtr_kume_main dln_earn_prev_nonformemp_80_91 dln_earn_all_70_80)

reg dln_earn_nonformemp_91_00 rtr_kume_main dln_earn_all_70_80 stflag2-stflag27 ///
    [aw=dln_earnse_nonformemp_91_00^-2], cluster(c_mesoreg1970)
outreg2 using ../Results/CensusOther/table_4_panel_B.xls, append ctitle("2000") sortvar(rtr_kume_main dln_earn_prev_nonformemp_80_91 dln_earn_all_70_80)

reg dln_earn_nonformemp_91_00 rtr_kume_main dln_earn_prev_nonformemp_80_91 ///
     dln_earn_all_70_80 stflag2-stflag27 ///
    [aw=dln_earnse_nonformemp_91_00^-2], cluster(c_mesoreg1970)
outreg2 using ../Results/CensusOther/table_4_panel_B.xls, append ctitle("2000") sortvar(rtr_kume_main dln_earn_prev_nonformemp_80_91 dln_earn_all_70_80)

* 2010

reg dln_earn_nonformemp_91_10 rtr_kume_main dln_earn_prev_nonformemp_80_91 stflag2-stflag27 ///
    [aw=dln_earnse_nonformemp_91_10^-2], cluster(c_mesoreg1970)
outreg2 using ../Results/CensusOther/table_4_panel_B.xls, append ctitle("2010") sortvar(rtr_kume_main dln_earn_prev_nonformemp_80_91 dln_earn_all_70_80)

reg dln_earn_nonformemp_91_10 rtr_kume_main dln_earn_all_70_80 stflag2-stflag27 ///
    [aw=dln_earnse_nonformemp_91_10^-2], cluster(c_mesoreg1970)
outreg2 using ../Results/CensusOther/table_4_panel_B.xls, append ctitle("2010") sortvar(rtr_kume_main dln_earn_prev_nonformemp_80_91 dln_earn_all_70_80)

reg dln_earn_nonformemp_91_10 rtr_kume_main dln_earn_prev_nonformemp_80_91 ///
    dln_earn_all_70_80 stflag2-stflag27 ///
    [aw=dln_earnse_nonformemp_91_10^-2], cluster(c_mesoreg1970)
outreg2 using ../Results/CensusOther/table_4_panel_B.xls, append ctitle("2010") sortvar(rtr_kume_main dln_earn_prev_nonformemp_80_91 dln_earn_all_70_80)


log close
cd "${root}"
