******************************************************************************
* tariff_chg_kume_subsibge.do
* Dix-Carneiro and Kovak AER replication files
*
* Calculates tariff changes by SUBSIBGE industry classification used in RAIS
* from tariff data based on Kume et al. (2003)
*
* Output: /Data/tariff_chg_kume_subsibge.dta
******************************************************************************

cd "${root}Codes_Other"
log using tariff_chg_kume_subsibge.txt, text replace

use ../Data_Other/kume_subsibge, clear
keep tariff erp subsibge year
reshape wide tariff erp, i(subsibge) j(year)

forvalues yr = 1991/1998 {
  gen dlnonetariff_1990_`yr' = ln(1+(tariff`yr'/100)) - ln(1+(tariff1990/100)) 
  gen dlnonetariff_1995_`yr' = ln(1+(tariff`yr'/100)) - ln(1+(tariff1995/100))
  gen dlnoneerp_1990_`yr' = ln(1+(erp`yr'/100)) - ln(1+(erp1990/100)) 
  gen dlnoneerp_1995_`yr' = ln(1+(erp`yr'/100)) - ln(1+(erp1995/100))
}

sum dlnonetariff_1990_1995
sum dlnoneerp_1990_1995
list subsibge dlnonetariff_1990_1995, clean

keep subsibge d*
save ../Data/tariff_chg_kume_subsibge, replace

log close
cd "${root}"
