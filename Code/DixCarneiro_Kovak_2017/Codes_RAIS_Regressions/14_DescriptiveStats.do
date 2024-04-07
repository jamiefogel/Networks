********************************************************************************
* Dix-Carneiro and Kovak AER replication files
* Generates part of Table 1, with Descriptive Statistics
* Remaining rows (indexed by an "a" superscript) are generated using Census data
********************************************************************************

clear

set more off

* global root "C:/Users/rd123/Dropbox/DixCarneiroKovakRodriguez/ReplicationFiles/"

global data1 "${root}Data/"
global data2 "${root}Data_Other/"
global output "${root}Results/DescriptiveStatistics/"
global earnings "${root}ProcessedData_RAIS/RegionalEarnPremia/"
global employment "${root}ProcessedData_RAIS/RegionalEmployment/"
global plants "${root}ProcessedData_RAIS/Plants/"
global jobcreat "${root}ProcessedData_RAIS/JobDestruction_JobCreation/"

********************************************************************************
********************************************************************************

matrix desc_stats = J(45,5,0)

*************************
* EARNINGS PREMIUM GROWTH
*************************

use ${earnings}mmcEarnPremia_main_1986_2010, clear

sort year
merge m:1 year using ${data2}Minimum_Wage, keepusing(year inpc10 min_wage)
keep if _merge == 3
drop _merge

rename inpc10 inpc10_

reshape wide coeff_rem_dez SE_rem_dez obs_dez min_wage inpc10_, i(mmc) j(year)

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

gen delta_86_90 = coeff_rem_dez1990 - coeff_rem_dez1986

keep if delta_86_90 ~= .

foreach year in 1995 2000 2005 2010{
	gen delta_earn_1991_`year' = coeff_rem_dez`year' - coeff_rem_dez1991 + log(min_wage`year') - ///
	log(min_wage1991) - log(inpc10_`year') + log(inpc10_1991)
}

sum delta_earn_1991_1995, d
matrix desc_stats[1,1] = `r(mean)'
matrix desc_stats[1,2] = sqrt(`r(Var)')
matrix desc_stats[1,3] = `r(p25)'
matrix desc_stats[1,4] = `r(p50)'
matrix desc_stats[1,5] = `r(p75)'

sum delta_earn_1991_2000, d
matrix desc_stats[2,1] = `r(mean)'
matrix desc_stats[2,2] = sqrt(`r(Var)')
matrix desc_stats[2,3] = `r(p25)'
matrix desc_stats[2,4] = `r(p50)'
matrix desc_stats[2,5] = `r(p75)'

sum delta_earn_1991_2005, d
matrix desc_stats[3,1] = `r(mean)'
matrix desc_stats[3,2] = sqrt(`r(Var)')
matrix desc_stats[3,3] = `r(p25)'
matrix desc_stats[3,4] = `r(p50)'
matrix desc_stats[3,5] = `r(p75)'

sum delta_earn_1991_2010, d
matrix desc_stats[4,1] = `r(mean)'
matrix desc_stats[4,2] = sqrt(`r(Var)')
matrix desc_stats[4,3] = `r(p25)'
matrix desc_stats[4,4] = `r(p50)'
matrix desc_stats[4,5] = `r(p75)'

********************************************************************************
********************************************************************************

**************************
* FORMAL EMPLOYMENT GROWTH
**************************

use ${employment}mmcEmployment_main_1986_2010, clear

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

gen delta_emp_86_90 = log(emp_dez1990) -  log(emp_dez1986)

keep if delta_emp_86_90 ~= .

foreach year in 1995 2000 2005 2010{
	gen delta_emp_1991_`year' = log(emp_dez`year') -  log(emp_dez1991)
}

sum delta_emp_1991_1995, d
matrix desc_stats[5,1] = `r(mean)'
matrix desc_stats[5,2] = sqrt(`r(Var)')
matrix desc_stats[5,3] = `r(p25)'
matrix desc_stats[5,4] = `r(p50)'
matrix desc_stats[5,5] = `r(p75)'

sum delta_emp_1991_2000, d
matrix desc_stats[6,1] = `r(mean)'
matrix desc_stats[6,2] = sqrt(`r(Var)')
matrix desc_stats[6,3] = `r(p25)'
matrix desc_stats[6,4] = `r(p50)'
matrix desc_stats[6,5] = `r(p75)'

sum delta_emp_1991_2005, d
matrix desc_stats[7,1] = `r(mean)'
matrix desc_stats[7,2] = sqrt(`r(Var)')
matrix desc_stats[7,3] = `r(p25)'
matrix desc_stats[7,4] = `r(p50)'
matrix desc_stats[7,5] = `r(p75)'

sum delta_emp_1991_2010, d
matrix desc_stats[8,1] = `r(mean)'
matrix desc_stats[8,2] = sqrt(`r(Var)')
matrix desc_stats[8,3] = `r(p25)'
matrix desc_stats[8,4] = `r(p50)'
matrix desc_stats[8,5] = `r(p75)'

********************************************************************************
********************************************************************************

****************************
* Number of Plants -- Growth
****************************

use ${plants}NumberPlants, clear

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

keep if coeff_rem_dez1990 ~= . & coeff_rem_dez1986 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

foreach year in 1995 2000 2005 2010{
	gen delta_nplants_1991_`year' = log(nplants`year'/nplants1991)
}

sum delta_nplants_1991_1995, d
matrix desc_stats[9,1] = `r(mean)'
matrix desc_stats[9,2] = sqrt(`r(Var)')
matrix desc_stats[9,3] = `r(p25)'
matrix desc_stats[9,4] = `r(p50)'
matrix desc_stats[9,5] = `r(p75)'

sum delta_nplants_1991_2000, d
matrix desc_stats[10,1] = `r(mean)'
matrix desc_stats[10,2] = sqrt(`r(Var)')
matrix desc_stats[10,3] = `r(p25)'
matrix desc_stats[10,4] = `r(p50)'
matrix desc_stats[10,5] = `r(p75)'

sum delta_nplants_1991_2005, d
matrix desc_stats[11,1] = `r(mean)'
matrix desc_stats[11,2] = sqrt(`r(Var)')
matrix desc_stats[11,3] = `r(p25)'
matrix desc_stats[11,4] = `r(p50)'
matrix desc_stats[11,5] = `r(p75)'

sum delta_nplants_1991_2010, d
matrix desc_stats[12,1] = `r(mean)'
matrix desc_stats[12,2] = sqrt(`r(Var)')
matrix desc_stats[12,3] = `r(p25)'
matrix desc_stats[12,4] = `r(p50)'
matrix desc_stats[12,5] = `r(p75)'

********************************************************************************
********************************************************************************

**************************
* Avg Plant Size -- Growth
**************************

use ${plants}PlantSize, clear

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

keep if coeff_rem_dez1990 ~= . & coeff_rem_dez1986 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

foreach year in 1995 2000 2005 2010{
	gen delta_size_1991_`year' = log(avg_plant_size`year'/avg_plant_size1991)
}

sum delta_size_1991_1995, d
matrix desc_stats[13,1] = `r(mean)'
matrix desc_stats[13,2] = sqrt(`r(Var)')
matrix desc_stats[13,3] = `r(p25)'
matrix desc_stats[13,4] = `r(p50)'
matrix desc_stats[13,5] = `r(p75)'

sum delta_size_1991_2000, d
matrix desc_stats[14,1] = `r(mean)'
matrix desc_stats[14,2] = sqrt(`r(Var)')
matrix desc_stats[14,3] = `r(p25)'
matrix desc_stats[14,4] = `r(p50)'
matrix desc_stats[14,5] = `r(p75)'

sum delta_size_1991_2005, d
matrix desc_stats[15,1] = `r(mean)'
matrix desc_stats[15,2] = sqrt(`r(Var)')
matrix desc_stats[15,3] = `r(p25)'
matrix desc_stats[15,4] = `r(p50)'
matrix desc_stats[15,5] = `r(p75)'

sum delta_size_1991_2010, d
matrix desc_stats[16,1] = `r(mean)'
matrix desc_stats[16,2] = sqrt(`r(Var)')
matrix desc_stats[16,3] = `r(p25)'
matrix desc_stats[16,4] = `r(p50)'
matrix desc_stats[16,5] = `r(p75)'

********************************************************************************
********************************************************************************

**************************
* Job Creation/Destruction
**************************

use ${jobcreat}JobCreationDestruction_Base1991, clear

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

keep if coeff_rem_dez1990 ~= . & coeff_rem_dez1986 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

foreach year in 1995 2000 2005 2010{
	gen dest_1991_`year' = log(NEG`year')
}

foreach year in 1995 2000 2005 2010{
	gen creat_1991_`year' = log(POS`year')
}

sum dest_1991_1995, d
matrix desc_stats[17,1] = `r(mean)'
matrix desc_stats[17,2] = sqrt(`r(Var)')
matrix desc_stats[17,3] = `r(p25)'
matrix desc_stats[17,4] = `r(p50)'
matrix desc_stats[17,5] = `r(p75)'

sum dest_1991_2000, d
matrix desc_stats[18,1] = `r(mean)'
matrix desc_stats[18,2] = sqrt(`r(Var)')
matrix desc_stats[18,3] = `r(p25)'
matrix desc_stats[18,4] = `r(p50)'
matrix desc_stats[18,5] = `r(p75)'

sum dest_1991_2005, d
matrix desc_stats[19,1] = `r(mean)'
matrix desc_stats[19,2] = sqrt(`r(Var)')
matrix desc_stats[19,3] = `r(p25)'
matrix desc_stats[19,4] = `r(p50)'
matrix desc_stats[19,5] = `r(p75)'

sum dest_1991_2010, d
matrix desc_stats[20,1] = `r(mean)'
matrix desc_stats[20,2] = sqrt(`r(Var)')
matrix desc_stats[20,3] = `r(p25)'
matrix desc_stats[20,4] = `r(p50)'
matrix desc_stats[20,5] = `r(p75)'

***

sum creat_1991_1995, d
matrix desc_stats[21,1] = `r(mean)'
matrix desc_stats[21,2] = sqrt(`r(Var)')
matrix desc_stats[21,3] = `r(p25)'
matrix desc_stats[21,4] = `r(p50)'
matrix desc_stats[21,5] = `r(p75)'

sum creat_1991_2000, d
matrix desc_stats[22,1] = `r(mean)'
matrix desc_stats[22,2] = sqrt(`r(Var)')
matrix desc_stats[22,3] = `r(p25)'
matrix desc_stats[22,4] = `r(p50)'
matrix desc_stats[22,5] = `r(p75)'

sum creat_1991_2005, d
matrix desc_stats[23,1] = `r(mean)'
matrix desc_stats[23,2] = sqrt(`r(Var)')
matrix desc_stats[23,3] = `r(p25)'
matrix desc_stats[23,4] = `r(p50)'
matrix desc_stats[23,5] = `r(p75)'

sum creat_1991_2010, d
matrix desc_stats[24,1] = `r(mean)'
matrix desc_stats[24,2] = sqrt(`r(Var)')
matrix desc_stats[24,3] = `r(p25)'
matrix desc_stats[24,4] = `r(p50)'
matrix desc_stats[24,5] = `r(p75)'

********************************************************************************
********************************************************************************

******************
* Plant Entry Exit
******************

use ${jobcreat}PlantEntryExit_Base1991, clear

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

keep if coeff_rem_dez1990 ~= . & coeff_rem_dez1986 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

foreach year in 1995 2000 2005 2010{
	gen exit_1991_`year' = log(NEG`year')
}

foreach year in 1995 2000 2005 2010{
	gen entry_1991_`year' = log(POS`year')
}

sum exit_1991_1995, d
matrix desc_stats[25,1] = `r(mean)'
matrix desc_stats[25,2] = sqrt(`r(Var)')
matrix desc_stats[25,3] = `r(p25)'
matrix desc_stats[25,4] = `r(p50)'
matrix desc_stats[25,5] = `r(p75)'

sum exit_1991_2000, d
matrix desc_stats[26,1] = `r(mean)'
matrix desc_stats[26,2] = sqrt(`r(Var)')
matrix desc_stats[26,3] = `r(p25)'
matrix desc_stats[26,4] = `r(p50)'
matrix desc_stats[26,5] = `r(p75)'

sum exit_1991_2005, d
matrix desc_stats[27,1] = `r(mean)'
matrix desc_stats[27,2] = sqrt(`r(Var)')
matrix desc_stats[27,3] = `r(p25)'
matrix desc_stats[27,4] = `r(p50)'
matrix desc_stats[27,5] = `r(p75)'

sum exit_1991_2010, d
matrix desc_stats[28,1] = `r(mean)'
matrix desc_stats[28,2] = sqrt(`r(Var)')
matrix desc_stats[28,3] = `r(p25)'
matrix desc_stats[28,4] = `r(p50)'
matrix desc_stats[28,5] = `r(p75)'

***

sum entry_1991_1995, d
matrix desc_stats[29,1] = `r(mean)'
matrix desc_stats[29,2] = sqrt(`r(Var)')
matrix desc_stats[29,3] = `r(p25)'
matrix desc_stats[29,4] = `r(p50)'
matrix desc_stats[29,5] = `r(p75)'

sum entry_1991_2000, d
matrix desc_stats[30,1] = `r(mean)'
matrix desc_stats[30,2] = sqrt(`r(Var)')
matrix desc_stats[30,3] = `r(p25)'
matrix desc_stats[30,4] = `r(p50)'
matrix desc_stats[30,5] = `r(p75)'

sum entry_1991_2005, d
matrix desc_stats[31,1] = `r(mean)'
matrix desc_stats[31,2] = sqrt(`r(Var)')
matrix desc_stats[31,3] = `r(p25)'
matrix desc_stats[31,4] = `r(p50)'
matrix desc_stats[31,5] = `r(p75)'

sum entry_1991_2010, d
matrix desc_stats[32,1] = `r(mean)'
matrix desc_stats[32,2] = sqrt(`r(Var)')
matrix desc_stats[32,3] = `r(p25)'
matrix desc_stats[32,4] = `r(p50)'
matrix desc_stats[32,5] = `r(p75)'

********************************************************************************
********************************************************************************

****************************
* Regional Tariff Reductions
****************************

use mmc rtc_kume_main using ${data1}rtc_kume, clear

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

keep if coeff_rem_dez1990 ~= . & coeff_rem_dez1986 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

gen rtr_kume_main = - rtc_kume_main

sum rtr_kume_main, d
matrix desc_stats[33,1] = `r(mean)'
matrix desc_stats[33,2] = sqrt(`r(Var)')
matrix desc_stats[33,3] = `r(p25)'
matrix desc_stats[33,4] = `r(p50)'
matrix desc_stats[33,5] = `r(p75)'

********************************************************************************
********************************************************************************

*******************
* Formal Employment
*******************

use ${employment}mmcEmployment_main_1986_2010, clear

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

keep if coeff_rem_dez1990 ~= . & coeff_rem_dez1986 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

sum emp_dez1991, d
matrix desc_stats[34,1] = `r(mean)'
matrix desc_stats[34,2] = sqrt(`r(Var)')
matrix desc_stats[34,3] = `r(p25)'
matrix desc_stats[34,4] = `r(p50)'
matrix desc_stats[34,5] = `r(p75)'

sum emp_dez1995, d
matrix desc_stats[35,1] = `r(mean)'
matrix desc_stats[35,2] = sqrt(`r(Var)')
matrix desc_stats[35,3] = `r(p25)'
matrix desc_stats[35,4] = `r(p50)'
matrix desc_stats[35,5] = `r(p75)'

sum emp_dez2000, d
matrix desc_stats[36,1] = `r(mean)'
matrix desc_stats[36,2] = sqrt(`r(Var)')
matrix desc_stats[36,3] = `r(p25)'
matrix desc_stats[36,4] = `r(p50)'
matrix desc_stats[36,5] = `r(p75)'

sum emp_dez2005, d
matrix desc_stats[37,1] = `r(mean)'
matrix desc_stats[37,2] = sqrt(`r(Var)')
matrix desc_stats[37,3] = `r(p25)'
matrix desc_stats[37,4] = `r(p50)'
matrix desc_stats[37,5] = `r(p75)'

sum emp_dez2010, d
matrix desc_stats[38,1] = `r(mean)'
matrix desc_stats[38,2] = sqrt(`r(Var)')
matrix desc_stats[38,3] = `r(p25)'
matrix desc_stats[38,4] = `r(p50)'
matrix desc_stats[38,5] = `r(p75)'

********************************************************************************
********************************************************************************

*******************************
* 1991 Share Agriculture/Mining
*******************************

** DATA FROM BRIAN **

use ${data1}sector_shares, clear

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

keep if coeff_rem_dez1990 ~= . & coeff_rem_dez1986 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

sum empl_share_ag_mining , d
matrix desc_stats[39,1] = `r(mean)'
matrix desc_stats[39,2] = sqrt(`r(Var)')
matrix desc_stats[39,3] = `r(p25)'
matrix desc_stats[39,4] = `r(p50)'
matrix desc_stats[39,5] = `r(p75)'

sum empl_share_manuf, d
matrix desc_stats[40,1] = `r(mean)'
matrix desc_stats[40,2] = sqrt(`r(Var)')
matrix desc_stats[40,3] = `r(p25)'
matrix desc_stats[40,4] = `r(p50)'
matrix desc_stats[40,5] = `r(p75)'

********************************************************************************
********************************************************************************

*******************
* Earnings Averages
*******************

use ${earnings}mmcAvgEarnings_1991_2010, clear

sort year
merge m:1 year using ${data2}Minimum_Wage, keepusing(year inpc10 min_wage)
keep if _merge == 3
drop _merge

rename inpc10 inpc10_

reshape wide rem_dez min_wage inpc10_, i(mmc) j(year)

sort mmc
merge m:1 mmc using ${earnings}mmcEarnPremia_main_1986_2010_wide
drop _merge

keep if coeff_rem_dez1990 ~= . & coeff_rem_dez1986 ~= .

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" | trim(mmc) == "13007"

foreach year in 1991 1995 2000 2005 2010{
	replace rem_dez`year' = rem_dez`year'*min_wage`year'*(inpc10_2010/inpc10_`year')
}

sum rem_dez1991, d
matrix desc_stats[41,1] = `r(mean)'
matrix desc_stats[41,2] = sqrt(`r(Var)')
matrix desc_stats[41,3] = `r(p25)'
matrix desc_stats[41,4] = `r(p50)'
matrix desc_stats[41,5] = `r(p75)'

sum rem_dez1995, d
matrix desc_stats[42,1] = `r(mean)'
matrix desc_stats[42,2] = sqrt(`r(Var)')
matrix desc_stats[42,3] = `r(p25)'
matrix desc_stats[42,4] = `r(p50)'
matrix desc_stats[42,5] = `r(p75)'

sum rem_dez2000, d
matrix desc_stats[43,1] = `r(mean)'
matrix desc_stats[43,2] = sqrt(`r(Var)')
matrix desc_stats[43,3] = `r(p25)'
matrix desc_stats[43,4] = `r(p50)'
matrix desc_stats[43,5] = `r(p75)'

sum rem_dez2005, d
matrix desc_stats[44,1] = `r(mean)'
matrix desc_stats[44,2] = sqrt(`r(Var)')
matrix desc_stats[44,3] = `r(p25)'
matrix desc_stats[44,4] = `r(p50)'
matrix desc_stats[44,5] = `r(p75)'

sum rem_dez2010, d
matrix desc_stats[45,1] = `r(mean)'
matrix desc_stats[45,2] = sqrt(`r(Var)')
matrix desc_stats[45,3] = `r(p25)'
matrix desc_stats[45,4] = `r(p50)'
matrix desc_stats[45,5] = `r(p75)'

********************************************************************************
********************************************************************************

clear 

svmat desc_stats

outsheet _all using ${output}desc_stats.csv, comma replace
