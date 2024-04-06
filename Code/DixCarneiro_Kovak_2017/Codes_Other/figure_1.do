******************************************************************************
* figure_1.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates Figure 1, showing the industry-level tariff changes based on 
* tariff data from Kume et al. (2003)
*
* Output: figure_1.pdf
******************************************************************************

cd "${root}Codes_Other"
log using figure_1.txt, text replace

******************
* Generate tariff changes from IndMatch level tariff data from Kume et al.

use ../Data_Other/kume_indmatch, clear
keep tariff erp indmatch year
reshape wide tariff erp, i(indmatch) j(year)

forvalues yr = 1991/1998 {
  gen dlnonetariff_1990_`yr' = ln(1+(tariff`yr'/100)) - ln(1+(tariff1990/100)) 
  gen dlnonetariff_1995_`yr' = ln(1+(tariff`yr'/100)) - ln(1+(tariff1995/100))
  gen dlnoneerp_1990_`yr' = ln(1+(erp`yr'/100)) - ln(1+(erp1990/100)) 
  gen dlnoneerp_1995_`yr' = ln(1+(erp`yr'/100)) - ln(1+(erp1995/100))
}
  
  
sum dlnonetariff_1990_1995
sum dlnoneerp_1990_1995
list indmatch dlnonetariff_1990_1995, clean

gen lnonetariff_1990 = ln(1+(tariff1990/100))
corr dlnonetariff_1990_1995 lnonetariff_1990

keep indmatch d*
save ../Data/tariff_chg_kume, replace

******************
* Prepare Figure 1

* start by calculating initial employment by Industry to sort industries
* on figure
use ../Data_Census/code_sample, clear
keep if year == 1991
keep if indmatch < .
collapse (sum) emp=xweighti, by(indmatch)

* merge in tariff changes
sort indmatch
merge 1:1 indmatch using ../Data/tariff_chg_kume
list indmatch _merge if _merge < 3
keep if _merge == 3 // drops nontraded (no tariff change available)
drop _merge

* merge in industry names
merge 1:1 indmatch using ../Data_Other/indmatchnames
list indmatch _merge if _merge < 3
keep if _merge == 3 // drops nontraded (no tariff change available)
drop _merge

* make figure
keep indmatch emp dlnonetariff_1990_1995 indname
gen negemp = -emp
graph bar dlnonetariff_1990_1995, ///
      over(indname, label(angle(vertical) labsize(small)) sort(negemp)) ///
	  ytitle("Change in ln(1+tariff), 1990-95") ///
	  yscale(range(-0.30 (0.05) 0.05)) ///
	  ylabel(,angle(horizontal) format(%3.2f) labsize(small)) ///
	  bar(1,color(gs8)) ///
	  graphregion(color(white))
graph export ../Results/CensusOther/figure_1.pdf, replace


log close
cd "${root}"
