********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates Table 6 and Figure 5
********************************************************************************

*********
* TABLE 6 
*********

clear

set more off

* global root "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\ReplicationFiles\"

global data1 "${root}Data\"
global data2 "${root}Data_Other\"
global output "${root}Results\SlowRespImportsExports\"
global earnings "${root}ProcessedData_RAIS\RegionalEarnPremia\"

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

use ${data1}adh_shocks
tostring mmc, replace
sort mmc
save ${data1}adh_shocks, replace

use ${data1}colombia_worldmb_adh_shocks
keep mmc adh_t_imports_* adh_t_exports_* 
forvalues year = 1992(1)2010{
	rename adh_t_imports_`year' adh_colombia_imp_`year'
	rename adh_t_exports_`year' adh_colombia_exp_`year'
}
tostring mmc, replace
sort mmc
save ${data1}colombia_worldmb_adh_shocks_aux, replace

use ${data1}la_worldmb_adh_shocks
keep mmc adh_t_imports_* adh_t_exports_*
forvalues year = 1995(1)2010{
	rename adh_t_imports_`year' adh_latam_imp_`year'
	rename adh_t_exports_`year' adh_latam_exp_`year'
}
tostring mmc, replace
sort mmc
save ${data1}la_worldmb_adh_shocks_aux, replace

********************************************************************************
* Colombia
********************************************************************************

use ${earnings}mmcEarnPremia_main_1986_2010, clear

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}adh_shocks
drop _merge

sort mmc
merge 1:1 mmc using ${data1}colombia_worldmb_adh_shocks_aux
drop _merge

sort mmc
merge 1:1 mmc using ${data1}adao_shock_mmc
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986	

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

keep if delta_86_90 ~= .

********************************************************************************
********************************************************************************

set matsize 10000

keep mmc adh_t_exports_* adh_t_imports_* coeff_rem_dez* SE_rem_dez* ///
	 adh_colombia_imp_* adh_colombia_exp_* ///
	 adao_shock_1991_* 
order mmc coeff_rem_dez* SE_rem_dez* adh_t_exports_* adh_t_imports_* ///
      adh_colombia_imp_* adh_colombia_exp_* ///
	  adao_shock_1991_* 

set more off
	  
reshape long coeff_rem_dez SE_rem_dez adh_t_imports_ adh_t_exports_ ///
        adh_colombia_imp_ adh_colombia_exp_ ///
		adao_shock_1991_ , i(mmc) j(year)

rename adh_t_imports_ adh_t_imports
rename adh_t_exports_ adh_t_exports
rename adh_colombia_imp_ adh_colombia_imp
rename adh_colombia_exp_ adh_colombia_exp
rename adao_shock_1991_ adao_shock

sort mmc
merge m:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main) 
drop _merge

sort mmc
merge m:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

sort mmc year

by mmc: egen coeff_rem_dez1991 = total(coeff_rem_dez*(year == 1991))
by mmc: egen SE_rem_dez1991 = total(SE_rem_dez*(year == 1991))

by mmc: egen coeff_rem_dez1986 = total(coeff_rem_dez*(year == 1986))
by mmc: egen coeff_rem_dez1990 = total(coeff_rem_dez*(year == 1990))

gen pre_trend = coeff_rem_dez1990 - coeff_rem_dez1986

gen delta_ln_w = coeff_rem_dez - coeff_rem_dez1991 
gen weight = 1 / sqrt(SE_rem_dez^2 + SE_rem_dez1991^2)

forvalues year = 1992(1)2010{
	gen rtr_`year' = rtr_kume_main*(year == `year')
}

replace adh_t_imports = adh_t_imports / 1000000
replace adh_t_exports = adh_t_exports / 1000000

forvalues year = 1992(1)2010{
	gen adh_colombia_imp_`year' = adh_colombia_imp*(year == `year')
}

forvalues year = 1992(1)2010{
	gen adh_colombia_exp_`year' = adh_colombia_exp*(year == `year')
}

forvalues year = 1992(1)2010{
	gen adao_shock_`year' = adao_shock*(year == `year')
}

forvalues year = 1992(1)2010{
	gen pre_trend_`year' = pre_trend*(year == `year')
}

forvalues year = 1992(1)2010{
	gen time_`year' = (year == `year')
}


forvalues year = 1992(1)2010{
	forvalues i = 1(1)27{
		gen state`i'_`year' = state`i'*(year == `year')
	}
}

keep if year >= 1992 & year <= 2010


set more off

****************************************
* Results Reported in TABLE 6 -- Panel A
****************************************

reg delta_ln_w rtr_1992-rtr_2010 state1_1992-state27_2010 pre_trend_1992-pre_trend_2010 [aw = weight^2], cluster(mesoreg)
outreg2 rtr_1992-rtr_2010 using ${output}ColombiaIV, excel bdec(4) sideway ctitle(OLS) replace

set more off

****************************************
* Results Reported in TABLE 6 -- Panel B
****************************************

reg delta_ln_w rtr_1992-rtr_2010 state1_1992-state27_2010 pre_trend_1992-pre_trend_2010 adh_t_imports adh_t_exports [aw = weight^2], cluster(mesoreg)
outreg2 rtr_1992-rtr_2010 adh_t_imports adh_t_exports using ${output}ColombiaIV, excel bdec(4) sideway ctitle(ADH OLS) append

set more off

ivreg2 delta_ln_w rtr_1992-rtr_2010 state1_1992-state27_2010 pre_trend_1992-pre_trend_2010 (adh_t_imports adh_t_exports = adh_colombia_imp adh_colombia_exp adao_shock) [aw = weight^2], cluster(mesoreg)
outreg2 adh_t_imports adh_t_exports rtr_1992-rtr_2010 using ${output}ColombiaIV, ///
excel bdec(4) sideway ctitle(ADH IV1) adds(Kleibergen-Paap rk LM statistic, `e(idstat)', p-value, `e(idp)', Kleibergen-Paap rk Wald F statistic, `e(rkf)') append

set more off

****************************************
* Results Reported in TABLE 6 -- Panel D
****************************************

ivreg2 delta_ln_w rtr_1992-rtr_2010 state1_1992-state27_2010 pre_trend_1992-pre_trend_2010 (adh_t_imports adh_t_exports = adh_colombia_imp_1992-adh_colombia_imp_2010 adh_colombia_exp_1992-adh_colombia_exp_2010 adao_shock_1992-adao_shock_2010) [aw = weight^2], cluster(mesoreg)
outreg2 adh_t_imports adh_t_exports rtr_1992-rtr_2010 using ${output}ColombiaIV, ///
excel bdec(4) sideway ctitle(ADH IV2) adds(Kleibergen-Paap rk LM statistic, `e(idstat)', p-value, `e(idp)', Kleibergen-Paap rk Wald F statistic, `e(rkf)') append






********************************************************************************
* Latin America
********************************************************************************

use ${earnings}mmcEarnPremia_main_1986_2010, clear

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}adh_shocks
drop _merge

sort mmc
merge 1:1 mmc using ${data1}adao_shock_mmc
drop _merge

sort mmc
merge 1:1 mmc using ${data1}la_worldmb_adh_shocks_aux
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

************
* Pre-Trends
************

gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986	
gen delta2_86_90 = log(obs_dez1990) - log(obs_dez1986)

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

keep if delta_86_90 ~= .

********************************************************************************
********************************************************************************

set matsize 10000

keep mmc adh_t_exports_* adh_t_imports_* coeff_rem_dez* SE_rem_dez* ///
	 adh_latam_imp_* adh_latam_exp_* ///
	 adao_shock_1991_* 
order mmc coeff_rem_dez* SE_rem_dez* adh_t_exports_* adh_t_imports_* ///
      adh_latam_imp_* adh_latam_exp_* ///
	  adao_shock_1991_* 

set more off
	  
reshape long coeff_rem_dez SE_rem_dez adh_t_imports_ adh_t_exports_ ///
        adh_latam_imp_ adh_latam_exp_ ///
		adao_shock_1991_ , i(mmc) j(year)

rename adh_t_imports_ adh_t_imports
rename adh_t_exports_ adh_t_exports
rename adh_latam_imp_ adh_latam_imp
rename adh_latam_exp_ adh_latam_exp
rename adao_shock_1991_ adao_shock


sort mmc
merge m:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main) 
drop _merge

sort mmc
merge m:1 mmc using ${data2}rais_mmc_to_mesoreg
drop _merge

gen state = substr(mmc,1,2)
qui tab state, gen(state)

gen rtr_kume_main = -rtc_kume_main

sort mmc year

by mmc: egen coeff_rem_dez1991 = total(coeff_rem_dez*(year == 1991))
by mmc: egen SE_rem_dez1991 = total(SE_rem_dez*(year == 1991))

by mmc: egen coeff_rem_dez1986 = total(coeff_rem_dez*(year == 1986))
by mmc: egen coeff_rem_dez1990 = total(coeff_rem_dez*(year == 1990))

gen pre_trend = coeff_rem_dez1990 - coeff_rem_dez1986

gen delta_ln_w = coeff_rem_dez - coeff_rem_dez1991 
gen weight = 1 / sqrt(SE_rem_dez^2 + SE_rem_dez1991^2)

forvalues year = 1995(1)2010{
	gen rtr_`year' = rtr_kume_main*(year == `year')
}

replace adh_t_imports = adh_t_imports / 1000000
replace adh_t_exports = adh_t_exports / 1000000

forvalues year = 1995(1)2010{
	gen adh_latam_imp_`year' = adh_latam_imp*(year == `year')
}

forvalues year = 1995(1)2010{
	gen adh_latam_exp_`year' = adh_latam_exp*(year == `year')
}

forvalues year = 1995(1)2010{
	gen adao_shock_`year' = adao_shock*(year == `year')
}

forvalues year = 1995(1)2010{
	gen pre_trend_`year' = pre_trend*(year == `year')
}

forvalues year = 1995(1)2010{
	gen time_`year' = (year == `year')
}


forvalues year = 1995(1)2010{
	forvalues i = 1(1)27{
		gen state`i'_`year' = state`i'*(year == `year')
	}
}

keep if year >= 1995 & year <= 2010


set more off

reg delta_ln_w rtr_1995-rtr_2010 state1_1995-state27_2010 pre_trend_1995-pre_trend_2010 [aw = weight^2], cluster(mesoreg)
outreg2 rtr_1995-rtr_2010 using ${output}latamIV, excel bdec(4) sideway ctitle(OLS) replace

set more off

reg delta_ln_w rtr_1995-rtr_2010 state1_1995-state27_2010 pre_trend_1995-pre_trend_2010 adh_t_imports adh_t_exports [aw = weight^2], cluster(mesoreg)
outreg2 rtr_1995-rtr_2010 adh_t_imports adh_t_exports using ${output}latamIV, excel bdec(4) sideway ctitle(ADH OLS) append

set more off

ivreg2 delta_ln_w rtr_1995-rtr_2010 state1_1995-state27_2010 pre_trend_1995-pre_trend_2010 (adh_t_imports adh_t_exports = adh_latam_imp adh_latam_exp adao_shock) [aw = weight^2], cluster(mesoreg)
outreg2 adh_t_imports adh_t_exports rtr_1995-rtr_2010 using ${output}latamIV, ///
excel bdec(4) sideway ctitle(ADH IV1) adds(Kleibergen-Paap rk LM statistic, `e(idstat)', p-value, `e(idp)', Kleibergen-Paap rk Wald F statistic, `e(rkf)') append

set more off

****************************************
* Results Reported in TABLE 6 -- Panel C
****************************************

ivreg2 delta_ln_w rtr_1995-rtr_2010 state1_1995-state27_2010 pre_trend_1995-pre_trend_2010 (adh_t_imports adh_t_exports = adh_latam_imp_1995-adh_latam_imp_2010 adh_latam_exp_1995-adh_latam_exp_2010 adao_shock_1995-adao_shock_2010) [aw = weight^2], cluster(mesoreg)
outreg2 adh_t_imports adh_t_exports rtr_1995-rtr_2010 using ${output}latamIV, ///
excel bdec(4) sideway ctitle(ADH IV2) adds(Kleibergen-Paap rk LM statistic, `e(idstat)', p-value, `e(idp)', Kleibergen-Paap rk Wald F statistic, `e(rkf)') append


********************************************************************************
********************************************************************************

**********
* Figure 5
**********

clear

set more off

global data1 "${root}Data\"
global data2 "${root}Data_Other\"
global output "${root}Results\SlowRespImportsExports\"
global earnings "${root}ProcessedData_RAIS\RegionalEarnPremia\"

********************************************************************************
********************************************************************************

use ${earnings}mmcEarnPremia_main_1986_2010, clear

reshape wide coeff_rem_dez SE_rem_dez obs_dez, i(mmc) j(year)

sort mmc
merge 1:1 mmc using ${data1}rtc_kume, keepusing(rtc_kume_main) 
drop _merge

sort mmc
merge 1:1 mmc using ${data1}adh_shocks
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

********************************************************************************
********************************************************************************

set more off

cap drop adh_imports
gen adh_imports = adh_t_imports_1991 / 100000
sum adh_imports, d
reg adh_imports rtr_kume_main state2-state27, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}ADH_IMPORTS_RTR, excel bdec(4) ctitle(1991) replace

forvalues yr = 1992(1)2010{
	cap drop adh_imports
	gen adh_imports = adh_t_imports_`yr' / 100000
	sum adh_imports, d
	reg adh_imports rtr_kume_main state2-state27, robust
	outreg2 rtr_kume_main using ${output}ADH_IMPORTS_RTR, excel bdec(4) ctitle(`yr') append
}	

set more off

cap drop adh_exports
gen adh_exports = adh_t_exports_1991 / 100000
sum adh_exports, d
reg adh_exports rtr_kume_main state2-state27, cluster(mesoreg)
outreg2 rtr_kume_main using ${output}ADH_EXPORTS_RTR, excel bdec(4) ctitle(1991) replace

forvalues yr = 1992(1)2010{
	cap drop adh_exports
	gen adh_exports = adh_t_exports_`yr' / 100000
	sum adh_exports, d
	reg adh_exports rtr_kume_main state2-state27, cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}ADH_EXPORTS_RTR, excel bdec(4) ctitle(`yr') append
}	

set more off

cap drop adh_net_exports
gen adh_net_exports = (adh_t_exports_1991 - adh_t_imports_1991) / 100000
sum adh_net_exports, d
reg adh_net_exports rtr_kume_main state2-state27, robust
outreg2 rtr_kume_main using ${output}ADH_NET_EXPORTS_RTR, excel bdec(4) ctitle(1991) replace

forvalues yr = 1992(1)2010{
	cap drop adh_net_exports
	gen adh_net_exports = (adh_t_exports_`yr' - adh_t_imports_`yr') / 100000
	sum adh_net_exports, d
	reg adh_net_exports rtr_kume_main state2-state27, cluster(mesoreg)
	outreg2 rtr_kume_main using ${output}ADH_NET_EXPORTS_RTR, excel bdec(4) ctitle(`yr') append
}	

********************************************************************************
********************************************************************************

erase ${data1}colombia_worldmb_adh_shocks_aux.dta
erase ${data1}la_worldmb_adh_shocks_aux.dta
erase ${data2}rais_mmc_to_mesoreg.dta
