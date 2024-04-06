******************************************************************************
* table_1_census.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates descriptive statistics using Census data for the relevant rows 
* of Table 1.
*
* Output: table_1_census.txt - Stata output for Census rows of Table 1
******************************************************************************

cd "${root}Codes_Census"
log using ../Results/CensusOther/table_1_census.txt, text replace

*****************************************
* Change in log informal earnings premium

use dln_earnings_informal, clear
merge 1:1 mmc using ../Data_Other/mmc1970_drop
drop _merge
drop if mmc1970_drop==1
codebook mmc1970 // 405 observations

* summary statistics
sum dln_earn_nonformemp_91_00
sum dln_earn_nonformemp_91_10

*****************************************
* Change in log informal employment

use dln_employment_mmc1970, clear
merge 1:1 mmc using ../Data_Other/mmc1970_drop
drop _merge
drop if mmc1970_drop==1
codebook mmc1970 // 405 observations

* summary statistics
sum dln_emp_nonformemp_91_00
sum dln_emp_nonformemp_91_10

*****************************************
* Change in log working-age population

use dln_population_mmc1970, clear
merge 1:1 mmc using ../Data_Other/mmc1970_drop
drop _merge
drop if mmc1970_drop==1
codebook mmc1970 // 405 observations

* summary statistics
sum dln_pop_91_00
sum dln_pop_91_10

*****************************************
* Share Agriculture/Mining

use ../Data/lambda, clear

* recode indmatch to sector
gen sector = 1 if indmatch == 1 // agriculture
replace sector = 2 if inlist(indmatch,2) // mining
replace sector = 3 if inlist(indmatch,3) // fuels
replace sector = 4 if inrange(indmatch,4,32) // manufacturing
replace sector = 5 if indmatch == 99 // nontraded

* aggregate labor shares to sector level
collapse (sum) lambda, by(mmc sector)
* test employment shares still sum to 1
bysort mmc: egen test = sum(lambda) 
sum test // all = 1
drop test

* reshape and prep for use as regression control
reshape wide lambda, i(mmc) j(sector)
rename lambda1 empl_share_ag
rename lambda2 empl_share_mining
rename lambda3 empl_share_fuels
rename lambda4 empl_share_manuf
rename lambda5 empl_share_nt
gen test = empl_share_ag + empl_share_mining + empl_share_fuels + empl_share_manuf + empl_share_nt
sum test // all = 1
drop test
gen empl_share_ag_mining = empl_share_ag + empl_share_mining
gen empl_share_ag_mining_fuels = empl_share_ag + empl_share_mining + empl_share_fuels
gen empl_share_mining_fuels = empl_share_mining + empl_share_fuels
sort mmc
save ../Data/sector_shares, replace

* summary statistics
sum empl_share_ag_mining


*****************************************
* Share Manufacturing

sum empl_share_manuf


log close
cd "${root}"
