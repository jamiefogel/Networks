************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates results reported in Tables B6 and B8
************************************************

clear

set more off

* global root         "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\ReplicationFiles\"

global data1        "${root}Data\"
global data2        "${root}Data_Other\"
global output       "${root}Results\RobustnessEarn\"
global earnings     "${root}ProcessedData_RAIS\RegionalEarnPremia\"
global SOF          "${root}ProcessedData_RAIS\StateOwnedFirms\"

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

use ${data1}mmc_rer
tostring mmc, replace
sort mmc
save ${data1}mmc_rer, replace

use ${data1}rtc_trains
tostring mmc, replace
keep mmc rtc_trains_t_theta_1995_*
sort mmc
save ${data1}rtc_trains_post, replace

use ${data1}frtc_kume
tostring mmc, replace
sort mmc
save ${data1}frtc_kume, replace

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

use ${data1}dln_earnings
tostring mmc1970, replace
rename mmc1970 mmc
sort mmc
save ${data1}dln_earnings_2, replace

********************************************************************************
********************************************************************************

use ${earnings}mmcEarnPremia_mmc1970_1986_2010

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}rtc_kume_mmc1970_2
drop _merge

sort mmc
merge 1:1 mmc using  ${data2}rais_mmc1970_to_mesoreg
drop _merge

sort mmc
merge 1:1 mmc using ${data1}dln_earnings_2
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

* pre-trends from RAIS
gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986
* pretrends from Census
gen delta_70_80 = earn_all_1980 - earn_all_1970 
gen delta_80_91 = earn_prev_formemp_1991 - earn_prev_formemp_1980

keep if delta_86_90 ~= .

drop if mmc == "26019" // Fernando de Noronha
drop if mmc == "13901" // Manaus

****************************************
* Longer Pre-Trends -- Table B6, Panel B
****************************************

* Specification 1: OLS with delta_70_80 and delta_80_91

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_70_80 delta_80_91 delta_86_90 state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_70_80 delta_80_91 delta_86_90 using ${output}RobustnessEarn_LongPT1, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main delta_70_80 delta_80_91 delta_86_90 state2-state27  [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_70_80 delta_80_91 delta_86_90 using ${output}RobustnessEarn_LongPT1, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

****************************************
* Longer Pre-Trends -- Table B6, Panel C
****************************************

* Specification 2: OLS with delta_70_80 and delta_80_91, base year 1992

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1993 - coeff_rem_dez1992
gen weights = 1/sqrt(SE_rem_dez1993^2+ SE_rem_dez1992^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_70_80 delta_80_91 delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_70_80 delta_80_91 delta_86_90 using ${output}RobustnessEarn_LongPT2, excel bdec(4) ctitle(1993) replace


forvalues yr = 1994(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1992
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1992^2)
	reg d_ln_w rtr_kume_main delta_70_80 delta_80_91 delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_70_80 delta_80_91 delta_86_90 using ${output}RobustnessEarn_LongPT2, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

********************************************************************************
********************************************************************************

use ${earnings}mmcEarnPremia_main_1986_2010, clear

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}mmc_rer
drop _merge

sort mmc
merge 1:1 mmc using ${data1}rtc_trains_post
drop _merge

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main rtc_kume_nt_theta_1990_1995 rec_kume_main) 
drop _merge

sort mmc
merge 1:1 mmc using ${data1}frtc_kume, keepusing(frtc_kume_main) 
drop _merge

sort mmc
merge 1:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

sort mmc
merge 1:1 mmc using ${SOF}share_SOF
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

* RTR with effective rates of protection
replace rec_kume_main = - rec_kume_main

* RTR with zero NT price changes
gen rtr_kume_NT = -rtc_kume_nt_theta_1990_1995

* RTR with formal employment weights
gen frtr_kume_main = -frtc_kume_main

gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986	

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

keep if delta_86_90 ~= .

******************************************************************
* RTR with formal employment industry weights -- Table B6, Panel D
******************************************************************

set more off

capture drop d_ln_w 
capture drop weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects 
reg d_ln_w frtr_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 frtr_kume_main delta_86_90 using ${output}RobustnessEarn_formalRTR, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w 
	capture drop weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w frtr_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 frtr_kume_main delta_86_90 using ${output}RobustnessEarn_formalRTR, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

*************************************************************
* RTR with Effective Rates of Protection -- Table B6, Panel E
*************************************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects 
reg d_ln_w rec_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rec_kume_main delta_86_90 using ${output}RobustnessEarn_ERP, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rec_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rec_kume_main delta_86_90 using ${output}RobustnessEarn_ERP, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

*****************************************************
* RTR with zero NT price changes -- Table B6, Panel F
*****************************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects 
reg d_ln_w rtr_kume_NT delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main_NT delta_86_90 using ${output}RobustnessEarn_NTRTR, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rtr_kume_NT delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main_NT delta_86_90 using ${output}RobustnessEarn_NTRTR, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

*********************************
* Unweighted -- Table B6, Panel I
*********************************

set more off

capture drop d_ln_w 
capture drop weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
* regression with state fixed effects 
reg d_ln_w rtr_kume_main delta_86_90 state2-state27, cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${output}RobustnessEarn_Unweighted, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w 
	capture drop weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main delta_86_90 state2-state27, cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90 using ${output}RobustnessEarn_Unweighted, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

*********************************************************
* Weighted By 1991 Formal Employment -- Table B6, Panel J
*********************************************************

set more off

capture drop d_ln_w 
capture drop weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = obs_dez1991
* regression with state fixed effects 
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 [aw=weights], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${output}RobustnessEarn_WeightEmp, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w 
	capture drop weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = obs_dez1991
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main delta_86_90 state2-state27 [aw=weights], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90 using ${output}RobustnessEarn_WeightEmp, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************


***********************************************************************************
* Controlling For Post Liberalization Tariff Changes -- Table B8, Panel B
***********************************************************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects 
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${output}RobustnessEarn_RTCPOST, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)1995{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90 using ${output}RobustnessEarn_RTCPOST, excel bdec(4) ctitle(`yr') append
}	


forvalues yr = 1996(1)2010{
	capture drop d_ln_w weights
	capture drop rtc_post
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	gen rtc_post = rtc_trains_t_theta_1995_`yr'
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main rtc_post delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main rtc_post delta_86_90 using ${output}RobustnessEarn_RTCPOST, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

***********************************************************************************
* Controlling for "Regional RER" -- Table B8, Panel C
***********************************************************************************

set more off

capture drop d_ln_w weights
cap drop rer_imports
cap drop rer_exports
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
gen rer_imports = rer_import_1992 
gen rer_exports = rer_export_1992
* regression with state fixed effects 
reg d_ln_w rtr_kume_main rer_imports rer_exports delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main rer_imports rer_exports delta_86_90 using ${output}RobustnessEarn_RER, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	cap drop rer_imports
	cap drop rer_exports
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen rer_imports = rer_import_`yr' 
	gen rer_exports = rer_export_`yr'
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main rer_imports rer_exports delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main rer_imports rer_exports delta_86_90 using ${output}RobustnessEarn_RER, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

********************************************************************************
* PRIVATIZATION 1 -- Table B8, Panel D
********************************************************************************

set more off

_pctile share_SOF1995 if delta_86_90 ~= ., nquantiles(4) 
gen share_SOF_q1 = share_SOF1995 < `r(r1)'
gen share_SOF_q2 = share_SOF1995 >= `r(r1)' & share_SOF1995 < `r(r2)'
gen share_SOF_q3 = share_SOF1995 >= `r(r2)' & share_SOF1995 < `r(r3)'
gen share_SOF_q4 = share_SOF1995 >= `r(r3)'

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1995 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1995^2+ SE_rem_dez1991^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 share_SOF_q2-share_SOF_q4 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 share_SOF_q2-share_SOF_q4 using ${output}RobustnessEarn_Privatization1, excel bdec(4) ctitle(1995) replace

forvalues yr = 1996(1)2010{
	
capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 share_SOF_q2-share_SOF_q4 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 share_SOF_q2-share_SOF_q4 using ${output}RobustnessEarn_Privatization1, excel bdec(4) ctitle(`yr') append

}

********************************************************************************
* PRIVATIZATION 2 -- Table B8, Panel E
********************************************************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1995 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1995^2+ SE_rem_dez1991^2)
* regression without state fixed effects
reg d_ln_w rtr_kume_main delta_86_90 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${output}RobustnessEarn_Privatization2, excel bdec(4) ctitle(1995) replace
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 using ${output}RobustnessEarn_Privatization2, excel bdec(4) ctitle(1995) replace

forvalues yr = 1996(1)2010{

cap drop control
gen control = share_SOF`yr' - share_SOF1995
capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_86_90 state2-state27 control [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90 control using ${output}RobustnessEarn_Privatization2, excel bdec(4) ctitle(`yr') append

}

********************************************************************************
********************************************************************************



********************************************************************************
********************************************************************************

use ${earnings}mmcEarnPremia_noindfe_1986_2010, clear

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

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

keep if delta_86_90 ~= .

**************************************************************************
* No Industry Fixed Effect Controls in the first step -- Table B6, Panel G
**************************************************************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_86_90  state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessEarn_NoIndFE, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main delta_86_90  state2-state27  [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessEarn_NoIndFE, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

********************************************************************************
********************************************************************************

use ${earnings}mmcEarnPremia_rawavg_1986_2010, clear

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

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

keep if delta_86_90 ~= .

***********************************
* Raw Averages -- Table B6, Panel H
***********************************

set more off

capture drop d_ln_w weights
gen d_ln_w = coeff_rem_dez1992 - coeff_rem_dez1991
gen weights = 1/sqrt(SE_rem_dez1992^2+ SE_rem_dez1991^2)
* regression with state fixed effects
reg d_ln_w rtr_kume_main delta_86_90  state2-state27  [aw=weights^2], cluster(mesoreg)
outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessEarn_Raw, excel bdec(4) ctitle(1992) replace


forvalues yr = 1993(1)2010{
	capture drop d_ln_w weights
	gen d_ln_w = coeff_rem_dez`yr' - coeff_rem_dez1991
	gen weights = 1/sqrt(SE_rem_dez`yr'^2+ SE_rem_dez1991^2)
	* regression with state fixed effects
	reg d_ln_w rtr_kume_main delta_86_90  state2-state27  [aw=weights^2], cluster(mesoreg)
	outreg2 rtr_kume_main delta_86_90  using ${output}RobustnessEarn_Raw, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

erase ${data1}dln_earnings_2.dta
erase ${data2}rais_mmc1970_to_mesoreg.dta
erase ${data1}rtc_kume_mmc1970_2.dta
erase ${data2}rais_mmc_to_mesoreg.dta
