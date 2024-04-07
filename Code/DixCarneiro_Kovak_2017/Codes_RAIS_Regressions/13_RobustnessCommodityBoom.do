********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates Tables B9 and B11
********************************************************************************

clear

set more off

* global root "C:/Users/rd123/Dropbox/DixCarneiroKovakRodriguez/ReplicationFiles/"

global data1    "${root}Data/"
global data2    "${root}Data_Other/"
global output   "${root}Results/RobustnessCommodityBoom/"
global earnings "${root}ProcessedData_RAIS/RegionalEarnPremia/"

********************************************************************************
********************************************************************************

* Make sure mmc variables across datasets previously generated are 
* transformed to string + rename variables whenever necessary

use ${data1}rtc_kume
tostring mmc, replace
sort mmc
save ${data1}rtc_kume, replace

use ${data2}mmc_1991_2010_to_c_mesoreg
tostring mmc, replace
rename c_mesoreg mesoreg
sort mmc
save ${data2}rais_mmc_to_mesoreg, replace

use ${data1}adao_shock_mmc
tostring mmc, replace
sort mmc
save ${data1}adao_shock_mmc, replace

use ${data1}rcs
tostring mmc, replace
sort mmc
save ${data1}rcs, replace

use ${data1}sector_shares
tostring mmc, replace
sort mmc
save ${data1}sector_shares, replace

use ${data2}mmc1970_to_c_mesoreg1970
tostring mmc1970, replace
rename mmc1970 mmc
rename c_mesoreg1970 mesoreg
save ${data2}rais_mmc1970_to_mesoreg, replace

use ${data1}rtc_kume_mmc1970
tostring mmc1970, replace
rename mmc1970 mmc
sort mmc
save ${data1}rtc_kume_mmc1970_2, replace

********************************************************************************
********************************************************************************

use ${earnings}mmcEarnPremia_main_1986_2010, clear

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main rec_kume_main) 
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

sort mmc
merge 1:1 mmc using ${data1}adao_shock_mmc
drop _merge

sort mmc
merge 1:1 mmc using ${data1}rcs
drop _merge

sort mmc
merge 1:1 mmc using ${data1}sector_shares
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986	

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

keep if delta_86_90 ~= .

********************************************************************************
* Quartiles of share of workers in agriculture
********************************************************************************

_pctile empl_share_ag_mining if delta_86_90 ~= ., nquantiles(4) 
gen q1 = empl_share_ag_mining < `r(r1)'
gen q2 = empl_share_ag_mining >= `r(r1)' & empl_share_ag_mining < `r(r2)'
gen q3 = empl_share_ag_mining >= `r(r2)' & empl_share_ag_mining < `r(r3)'
gen q4 = empl_share_ag_mining >= `r(r3)' 

********************************************************************************
********************************************************************************

*******************************************************
* Less Agricultural Regions -- p50 -- Table B9, Panel B
*******************************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects
sum empl_share_ag_mining if delta_86_90 ~= ., d
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 if empl_share_ag_mining <= `r(p50)' [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessCommodityBoomB, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	sum empl_share_ag_mining if delta_86_90 ~= ., d
	reg d_ln_w rtr_kume_main delta_86_90 state2-state27 if empl_share_ag_mining <= `r(p50)' [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessCommodityBoomB, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

*******************************************************
* Less Agricultural Regions -- p25 -- Table B9, Panel C
*******************************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects
sum empl_share_ag_mining if delta_86_90 ~= ., d
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 if empl_share_ag_mining <= `r(p25)' [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessCommodityBoomC, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	sum empl_share_ag_mining if delta_86_90 ~= ., d
	reg d_ln_w rtr_kume_main delta_86_90 state2-state27 if empl_share_ag_mining <= `r(p25)' [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessCommodityBoomC, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

********************************************************************************
*  Direct commodity price controls per Adao (2015) -- table B9, Panel E
********************************************************************************

set more off

capture drop d_ln_w 
capture drop weights
capture drop adao_shock
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
gen adao_shock = adao_shock_1991_1992
* regression with state fixed effects
reg d_ln_w rtr_kume_main q2-q4 adao_shock delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main q2-q4 adao_shock delta_86_90 using ${output}RobustnessCommodityBoomE, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w 
	capture drop weights
	capture drop adao_shock
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	gen adao_shock = adao_shock_1991_`yr'
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main q2-q4 adao_shock delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main q2-q4 adao_shock delta_86_90 using ${output}RobustnessCommodityBoomE, excel bdec(4) ctitle(`yr') append
}

********************************************************************************
********************************************************************************

*************************************************************************************************
*  Direct commodity price controls using detailed commodity price data (IMF) -- Table B9, Panel F
*************************************************************************************************

set more off

capture drop d_ln_w 
capture drop weights
capture drop com_shock
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
gen com_shock = rcs_all1992
* regression with state fixed effects
reg d_ln_w rtr_kume_main com_shock delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main com_shock delta_86_90 using ${output}RobustnessCommodityBoomF, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w 
	capture drop weights
	capture drop com_shock
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	gen com_shock = rcs_all`yr'
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main com_shock delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main com_shock delta_86_90 using ${output}RobustnessCommodityBoomF, excel bdec(4) ctitle(`yr') append
}

********************************************************************************
********************************************************************************






********************************************************************************
********************************************************************************

clear

use ${earnings}mmcEarnPremia_manuf_1986_2010, clear

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main) 
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986	

forvalues year = 1986(1)2010{

	drop if coeff_rem_dez`year' == .

}

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"
drop if mmc == "13011"

keep if delta_86_90 ~= .

*********************************************
* Manufacturing Earnings -- Table B9, Panel D
*********************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessCommodityBoomD, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessCommodityBoomD, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************


********************************************************************************
* Costa et al shock controls -- Table B11
********************************************************************************

/*
use ${data}_CGP2015_BrazilChina_IS_XD_Censo_2000
rename cod_microregion mmc
tostring mmc, replace
sort mmc
save ${data}_CGP2015_BrazilChina_IS_XD_Censo_2000, replace
*/

use ${earnings}mmcEarnPremia_mmc1970_1986_2010, clear

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}rtc_kume_mmc1970_2
drop _merge

sort mmc
merge 1:1 mmc using  ${data2}rais_mmc1970_to_mesoreg
drop _merge

sort mmc
merge 1:1 mmc using ${data2}_CGP2015_BrazilChina_IS_XD_Censo_2000
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

* pre-trends from RAIS
gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986

keep if delta_86_90 ~= .

drop if mmc == "26019" // Fernando de Noronha
drop if mmc == "13901" // Manaus

********************************************************************************
********************************************************************************

set more off

capture drop d_ln_w 
capture drop weights
gen d_ln_w = coeff_rem_dez2010 - coeff_rem_dez2000
gen weights = 1/sqrt(SE_rem_dez2010^2+ SE_rem_dez2000^2)

reg d_ln_w rtr_kume_main delta_86_90 state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${output}CommodityBoom_Costaetal, excel bdec(4) ctitle(OLS) replace

ivreg2 d_ln_w rtr_kume_main (IS_mt = ivIS_mt) delta_86_90 state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main IS_mt delta_86_90 using ${output}CommodityBoom_Costaetal, excel bdec(4) adds(K-P rk LM statistic, `e(idstat)', p-value, `e(idp)', K-P rk Wald F statistic, `e(rkf)') append

ivreg2 d_ln_w rtr_kume_main (XD_mt = ivXD_mt) delta_86_90 state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main XD_mt delta_86_90 using ${output}CommodityBoom_Costaetal, excel bdec(4) adds(K-P rk LM statistic, `e(idstat)', p-value, `e(idp)', K-P rk Wald F statistic, `e(rkf)') append

ivreg2 d_ln_w rtr_kume_main (IS_mt XD_mt = ivIS_mt ivXD_mt) delta_86_90 state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main IS_mt XD_mt delta_86_90 using ${output}CommodityBoom_Costaetal, excel bdec(4) adds(K-P rk LM statistic, `e(idstat)', p-value, `e(idp)', K-P rk Wald F statistic, `e(rkf)') append

********************************************************************************
********************************************************************************

erase ${data2}rais_mmc_to_mesoreg.dta
erase ${data2}rais_mmc1970_to_mesoreg.dta
erase ${data1}rtc_kume_mmc1970_2.dta
