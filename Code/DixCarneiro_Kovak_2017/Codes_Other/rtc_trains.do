******************************************************************************
* rtc_trains.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates tariff changes using UNCTAD TRAINS tariff data.
*
* Output: /Data/rtc_trains.dta
******************************************************************************

cd "${root}Codes_Other"
log using rtc_trains.txt, text replace

**************************
* Calculate tariffs at the Industry classification level

forvalues yr = 1989/2011 {

  disp("year: `yr'")

  use ../Data_Other/trains/trains_brazil_`yr', clear

  if (`yr' >= 1989 & `yr' <= 1995) {
    sort hs1992
    merge 1:1 hs1992 using ../Data_Other/hs1992_to_industry
    list hs1992 if _merge == 1 // should be none
  }
  if (`yr' >= 1996 & `yr' <= 2001) {
    sort hs1996
	merge 1:1 hs1996 using ../Data_Other/hs1996_to_industry
	list hs1996 if _merge == 1 // should be none - 1999 has one invalid code: 281819 - confirmed invalid in comtrade documentation of hs1996 (h1)
  }
  if (`yr' >= 2002 & `yr' <= 2006) {
    sort hs2002
	merge 1:1 hs2002 using ../Data_Other/comtrade_h2_to_h1
	list hs2002 if _merge ==1 // should be none
	keep if _merge == 3
	drop _merge
	sort hs1996
	merge m:1 hs1996 using ../Data_Other/hs1996_to_industry
	list hs1996 if _merge == 1 // should be none
  }
  if (`yr' >= 2007 & `yr' <= 2011) {
    sort hs2007
	merge 1:1 hs2007 using ../Data_Other/comtrade_h3_to_h1
	list hs2007 if _merge ==1 // should be none
	keep if _merge == 3
	drop _merge
	sort hs1996
	merge m:1 hs1996 using ../Data_Other/hs1996_to_industry
	list hs1996 if _merge == 1 // should be none  
  }
   
  keep if _merge == 3
  collapse (mean) tariff, by(industry)
  keep if industry < .
  capture drop year
  gen year = `yr'
  keep industry tariff year
  order industry year tariff 
  sort industry
  save trains_industry_`yr', replace
}

use trains_industry_1989, clear
forvalues yr = 1990/2011 {
  append using trains_industry_`yr'
}
forvalues yr = 1989/2011 {
  erase trains_industry_`yr'.dta
}

**************************
* Calculate tariff changes

sort industry year
reshape wide tariff, i(industry) j(year)

forvalues yr = 1991/2011 {
  gen dlnonetariff_1990_`yr' = ln(1+(tariff`yr'/100)) - ln(1+(tariff1990/100)) 
  gen dlnonetariff_1995_`yr' = ln(1+(tariff`yr'/100)) - ln(1+(tariff1995/100))
}

sum dlnonetariff_1990_1995
list industry dlnonetariff_1990_1995, clean

keep industry d*
save tariff_chg_trains, replace

**************************
* Industry-level weights for regional tariff changes

use ../Data_Census/code_sample, clear
keep if year == 1991

* generate lambda: indsutrial distribution of labor in each region
keep if industry < .
keep industryflag* mmc xweighti
forvalues ind = 1/44 {
  rename industryflag`ind' lambda`ind'
}
rename industryflag45 lambda99
collapse (mean) lambda* [pw=xweighti], by(mmc)

reshape long lambda, i(mmc) j(industry)

* bring in thetas
sort industry
merge m:1 industry using ../Data_Other/theta_industry
drop _merge

* calculate versions of beta

* including nontradables, without theta adjustment
gen beta_nt_notheta = lambda
bysort mmc: egen test = sum(beta_nt_notheta) // confirm proper weights
sum test // all = 1
drop test

* including nontradables, without theta adjustment
gen beta_nt_theta_temp = lambda / theta
bysort mmc: egen total = sum(beta_nt_theta_temp)
gen beta_nt_theta = beta_nt_theta_temp / total
by mmc: egen test = sum(beta_nt_theta)
sum test // all = 1
drop test total beta_nt_theta_temp

* omitting nontradables, without theta adjustment
gen beta_t_notheta_temp = lambda if industry != 99
bysort mmc: egen total = sum(beta_t_notheta_temp) if industry != 99
gen beta_t_notheta = beta_t_notheta_temp / total
by mmc: egen test = sum(beta_t_notheta)
sum test // all = 1
drop test total beta_t_notheta_temp

* omitting nontradables, with theta adjustment
gen beta_t_theta_temp = lambda / theta if industry != 99
bysort mmc: egen total = sum(beta_t_theta_temp) if industry != 99
gen beta_t_theta = beta_t_theta_temp / total
by mmc: egen test = sum(beta_t_theta)
sum test // all - 1
drop test total beta_t_theta_temp

keep mmc industry beta*
sort mmc industry
save ../Data/beta_industry, replace

**************************
* regional tariff changes

* merge tariff changes onto beta weights

merge m:1 industry using tariff_chg_trains
tab industry if _merge < 3 // all nontradable
drop _merge
forvalues yr = 1991/2011 {
  replace dlnonetariff_1990_`yr' = 0 if industry == 99
  list industry dlnonetariff_1990_`yr' if dlnonetariff_1990_`yr' >= . // should be none
}

* create regional weighted averages

* set up sum elements
forvalues yr = 1991/2011 {
  foreach v in nt_notheta nt_theta t_notheta t_theta {
    gen element_1990_`v'_`yr' = beta_`v' * dlnonetariff_1990_`yr'
	gen element_1995_`v'_`yr' = beta_`v' * dlnonetariff_1995_`yr'
  }
}
* sum to create weighted averages
collapse (sum) element*, by(mmc)
* rename collapsed weighted averages
forvalues yr = 1991/2011 {
  foreach v in nt_notheta nt_theta t_notheta t_theta {
    rename element_1990_`v'_`yr' rtc_trains_`v'_1990_`yr'
    rename element_1995_`v'_`yr' rtc_trains_`v'_1995_`yr'
  }
}

* rename to create rtc_main
rename rtc_trains_t_theta_1990_1995 rtc_trains_main

sum rtc*1995 rtc_trains_main
sort mmc
save ../Data/rtc_trains, replace

erase tariff_chg_trains.dta

log close
cd "${root}"
