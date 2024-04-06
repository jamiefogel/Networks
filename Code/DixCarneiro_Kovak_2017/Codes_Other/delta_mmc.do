******************************************************************************
* delta_mmc.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates delta in equation (9).
*
* Output: /Data/delta_mmc.dta
******************************************************************************

cd "${root}Codes_Other"
log using delta_mmc.txt, text replace

use ../Data/lambda, clear
sort indmatch
merge m:1 indmatch using ../Data_Other/theta_indmatch
drop _merge // perfect match

tab indmatch // still have nontradable
gen sumelement = lambda / theta
collapse (sum) denominator=sumelement, by(mmc)
gen delta = 1/denominator
keep mmc delta
sum delta
save ../Data/delta_mmc, replace


log close
cd "${root}"
