
* Estimates regional premia controlling fo individual fixed effects
* Input data: ${data2}new_panel.dta, the 3% panel of workers constructed running
* 0a_Sampling1986_2010.do and
* 0b_Panel_1986_2010.do

clear all 
set more off
capture log close
global root "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017\"

global data1 "${root}Data_Other\"
global data2 "${root}ProcessedData_RAIS\Panel_1986_2010\"
global output "${root}ProcessedData_RAIS\RegionalEarnPremia_WorkerFE\"

log using ${output}RegionalEarningsPremiaFixedEffects.log, replace

use ${data1}mmc_1991_2010_to_c_mesoreg
tostring mmc, replace
rename c_mesoreg mesoreg
sort mmc
save ${data1}rais_mmc_to_mesoreg, replace

use year pis age educ_25 gender subs_ibge cnpj codemun real_rem_dez using ${data2}new_panel, clear

duplicates drop

*Drop all observations with a NONFORMAL sector	
drop if subs_ibge == "NONFORMAL"

*Keep indivuals with positive and non-missing real_rem_dez observation
keep if real_rem_dez > 0 & real_rem_dez ~= .

*Keep the highest paying job for each individual
gsort + pis + year - real_rem_dez
by pis year: gen obs = _n
keep if obs == 1
drop obs

display _N

preserve
	keep pis year
	duplicates drop
	display _N
restore	

gen log_rem_dez = log(real_rem_dez)

* Drop individuals that start in RAIS with age less than 16
sort pis
by pis: egen min_age = min(age)
drop if min_age < 16
* Only keep observations of working age individuals
keep if age >= 18 & age <= 64
drop if subs_ibge == "5719"
drop if subs_ibge == "9999"

preserve
	keep pis year
	duplicates drop
	sort pis year
	by pis: gen obs = _n
	by pis: egen max_obs = max(obs)
	keep if obs == 1
	drop obs
	tab max_obs
	sum max_obs, d
restore

*Merge with mmc converters
sort codemun
merge m:1 codemun using ${data1}rais_codemun_to_mmc_1991_2010
keep if _merge == 3
drop _merge

*Obtain mesoregion information
sort mmc
merge m:1 mmc using ${data1}rais_mmc_to_mesoreg
keep if _merge == 3
drop _merge

drop if trim(mmc) == "." | trim(mmc) == ""

egen indiv = group(pis)

*Generate mmc-year and industry-year categorical variables
egen mmc_year = group(mmc year)
qui sum mmc_year, meanonly
local max_mmc_year = r(max)
display `max_mmc_year' // number of groups

sort year subs_ibge 
egen subs_ibge_year = group(year subs_ibge)
qui sum subs_ibge_year, meanonly
local max_subs_ibge_year = r(max)
display `max_subs_ibge_year' // number of groups
qui tab subs_ibge_year, gen(subs_ibge_year)

gen     age_aux = 1 if age >= 18 & age <= 24
replace age_aux = 2 if age >= 25 & age <= 29
replace age_aux = 3 if age >= 30 & age <= 39
replace age_aux = 4 if age >= 40 & age <= 49
replace age_aux = 5 if age >= 50 & age <= 64

tab age_aux, gen(cat_age)

gen age_1 = (age-25)
gen age_2 = (age-25)^2

duplicates drop
display _N

set matsize 11000

set more off

felsdvreg log_rem_dez cat_age2-cat_age5 subs_ibge_year2-subs_ibge_year24 /// 1986
                                        subs_ibge_year26-subs_ibge_year48 /// 1987
										subs_ibge_year50-subs_ibge_year72 /// 1988
										subs_ibge_year74-subs_ibge_year96 /// 1989
										subs_ibge_year98-subs_ibge_year120 /// 1990
										subs_ibge_year122-subs_ibge_year144 /// 1991
										subs_ibge_year146-subs_ibge_year168 /// 1992
										subs_ibge_year170-subs_ibge_year192 /// 1993
										subs_ibge_year194-subs_ibge_year216 /// 1994
										subs_ibge_year218-subs_ibge_year240 /// 1995
										subs_ibge_year242-subs_ibge_year264 /// 1996
										subs_ibge_year266-subs_ibge_year288 /// 1997
										subs_ibge_year290-subs_ibge_year312 /// 1998
										subs_ibge_year314-subs_ibge_year336 /// 1999
										subs_ibge_year338-subs_ibge_year360 /// 2000
										subs_ibge_year362-subs_ibge_year384 /// 2001
										subs_ibge_year386-subs_ibge_year408 /// 2002
										subs_ibge_year410-subs_ibge_year432 /// 2003
										subs_ibge_year434-subs_ibge_year456 /// 2004
										subs_ibge_year458-subs_ibge_year480 /// 2005
										subs_ibge_year482-subs_ibge_year504 /// 2006
										subs_ibge_year506-subs_ibge_year528 /// 2007
										subs_ibge_year530-subs_ibge_year552 /// 2008
										subs_ibge_year554-subs_ibge_year576 /// 2009
										subs_ibge_year578-subs_ibge_year600, /// 2010
          ivar(indiv) jvar(mmc_year) feff(feff) feffse(feffse) peff(peff) ///
	      mover(mover) group(group) xb(xb) res(res) mnum(mnum) pobs(pobs) cons

display _N	
	
save ${output}fixed_effects2, replace 

********************************************************************************
********************************************************************************

clear

use ${output}fixed_effects2

drop if feff == .

gen ones = 1
sort mmc year
by mmc year: egen mmc_year_obs = total(ones)

keep mmc year feff feffse mmc_year_obs group
duplicates drop
sort mmc year

keep if group == 1

save ${output}mmcEarnPremia_wrkrFE_1986_2010, replace

erase ${output}fixed_effects2.dta
erase ${data1}rais_mmc_to_mesoreg.dta

log close
