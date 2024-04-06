******************************************************************************
* frtc_kume.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates regional tariff changes using formal employment weights
*
* Output: /Data/frtc_kume.dta
******************************************************************************

cd "${root}Codes_Other"
log using frtc_kume.txt, text replace

***************************
* calculate formal beta weights

* load census data for 1991
use ../Data_Census/code_sample if year==1991, clear
keep if formemp==1

************
* generate lambda: indsutrial distribution of labor in each region
keep if indmatch < .
keep indmatchflag* mmc xweighti
rename indmatchflag1 lambda1
rename indmatchflag2 lambda2
rename indmatchflag3 lambda3
rename indmatchflag4 lambda4
rename indmatchflag5 lambda5
rename indmatchflag6 lambda8
rename indmatchflag7 lambda10
rename indmatchflag8 lambda12
rename indmatchflag9 lambda14
rename indmatchflag10 lambda15
rename indmatchflag11 lambda16
rename indmatchflag12 lambda17
rename indmatchflag13 lambda18
rename indmatchflag14 lambda20
rename indmatchflag15 lambda21
rename indmatchflag16 lambda22
rename indmatchflag17 lambda23
rename indmatchflag18 lambda24
rename indmatchflag19 lambda25
rename indmatchflag20 lambda32
rename indmatchflag21 lambda99
collapse (mean) lambda* [pw=xweighti], by(mmc)

reshape long lambda, i(mmc) j(indmatch)
save lambda_formal, replace

* bring in thetas
sort indmatch
merge m:1 indmatch using ../Data_Other/theta_indmatch
drop _merge

************
* calculate versions of beta

* including nontradables, without theta adjustment
gen beta_nt_notheta = lambda
bysort mmc: egen test = sum(beta_nt_notheta) // confirm proper weights
sum test // all = 1
drop test

* including nontradables, with theta adjustment
gen beta_nt_theta_temp = lambda / theta
bysort mmc: egen total = sum(beta_nt_theta_temp)
gen beta_nt_theta = beta_nt_theta_temp / total
by mmc: egen test = sum(beta_nt_theta)
sum test // all = 1
drop test total beta_nt_theta_temp

* omitting nontradables, without theta adjustment
gen beta_t_notheta_temp = lambda if indmatch != 99
bysort mmc: egen total = sum(beta_t_notheta_temp) if indmatch != 99
gen beta_t_notheta = beta_t_notheta_temp / total
by mmc: egen test = sum(beta_t_notheta)
sum test // all = 1
drop test total beta_t_notheta_temp

* omitting nontradables, with theta adjustment
gen beta_t_theta_temp = lambda / theta if indmatch != 99
bysort mmc: egen total = sum(beta_t_theta_temp) if indmatch != 99
gen beta_t_theta = beta_t_theta_temp / total
by mmc: egen test = sum(beta_t_theta)
sum test // all - 1
drop test total beta_t_theta_temp

keep mmc indmatch beta*
sort mmc indmatch

erase lambda_formal.dta

***************************
* merge tariff changes onto beta weights

sort indmatch
merge m:1 indmatch using ../Data/tariff_chg_kume
tab indmatch if _merge < 3 // all nontradable
drop _merge
forvalues yr = 1991/1998 {
  replace dlnonetariff_1990_`yr' = 0 if indmatch == 99
  replace dlnoneerp_1990_`yr' = 0 if indmatch == 99
  list indmatch dlnonetariff_1990_`yr' if dlnonetariff_1990_`yr' >= . // should be none
  list indmatch dlnoneerp_1990_`yr' if dlnoneerp_1990_`yr' >= . // should be none
}

***************************
* create regional weighted averages

* set up sum elements
forvalues yr = 1991/1998 {
  foreach v in nt_notheta nt_theta t_notheta t_theta {
    foreach m in tariff erp {
      gen el_`m'_1990_`v'_`yr' = beta_`v' * dlnone`m'_1990_`yr'
	  gen el_`m'_1995_`v'_`yr' = beta_`v' * dlnone`m'_1995_`yr'
	}
  }
}
* sum to create weighted averages
collapse (sum) el*, by(mmc)
* rename collapsed weighted averages
forvalues yr = 1991/1998 {
  foreach v in nt_notheta nt_theta t_notheta t_theta {
    foreach m in tariff erp {
	  if ("`m'"=="tariff") {
        rename el_`m'_1990_`v'_`yr' frtc_kume_`v'_1990_`yr'
        rename el_`m'_1995_`v'_`yr' frtc_kume_`v'_1995_`yr'
	  }
	  if ("`m'"=="erp") {
        rename el_`m'_1990_`v'_`yr' frec_kume_`v'_1990_`yr'
        rename el_`m'_1995_`v'_`yr' frec_kume_`v'_1995_`yr'
	  }
	}
  }
}

* rename to create rtc_kume_main
rename frtc_kume_t_theta_1990_1995 frtc_kume_main
rename frec_kume_t_theta_1990_1995 frec_kume_main

sum frtc_kume_main frec_kume_main
sort mmc
save ../Data/frtc_kume, replace


log close
cd "${root}"
