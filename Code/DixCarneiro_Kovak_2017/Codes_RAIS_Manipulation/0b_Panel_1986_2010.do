
* This code reads data ${result}am_rais_final and cleans it to generate a panel
* of workers over time.
* This code cleans the data, correcting reporting mistakes in gender, education
* and age. 
* It generates file ${result}new_panel, which will be used in the codes estimating
* first-stage regressions controlling for individual fixed effects.
* 1g_RegionalEarningsPremiaFixedEffects.do
* and files in folder EarnWrkrFE_VaryingReturns (for estimates of specification
* allowing for varying returns on unobservable heterogenity.

clear

set more off

capture log close

global root "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017\"

global data "F:\RAIS\Data_Brazil\RAIS_Stata2\"
global data2 "${root}Data_Other\"
global result "${root}ProcessedData_RAIS\Panel_1986_2010\"

log using ${result}Panel_1986_2010.log, replace

use ${result}am_rais_final

duplicates drop

* Number of individuals in the raw panel
preserve
keep pis
duplicates drop
display _N
restore

replace subs_ibge = "NONFORMAL" if  cnpj == "" & codemun == "" & subs_ibge == ""
replace clas_cnae = "NONFORMAL" if  subs_ibge == "NONFORMAL"


******************************************************
* Check gender consistency / impute and correct gender
******************************************************

sort pis
by pis: egen min_gender = min(sexo)
by pis: egen max_gender = max(sexo) 

* flags at least one inconsistent gender reported for a given individual
gen flag = 0
replace flag = 1 if min_gender < max_gender & max_gender ~= .
replace flag = . if min_gender == . | max_gender == .

* Fraction of individuals with reported gender with at least one gender inconsistency
preserve
keep pis flag 
duplicates drop
tab flag
restore
drop flag

* Obtain the most frequently reported gender for each individual
* And create time invariant variable gender with the most frequently reported gender within individuals
* If there are multiple modes then drop the individual
* mode() returns a missing value if there are multiple modes
sort pis year rem_media 
by pis: egen mode_gender = mode(sexo)
tab mode_gender, m
* Are multiple gender modes related to education?
tab grau_instr , gen(d_educ)
gen flag = (mode_gender == .)
reg flag d_educ2-d_educ9
drop if mode_gender == .
gen gender = mode_gender
drop flag max_gender min_gender mode_gender d_educ* 

save ${result}interm1, replace


clear

use ${result}interm1

duplicates drop

************************************************
* Check age consistency / impute and correct age
************************************************

* First check age conditional on t >= 1994

* Age in 1994
gen age_1994 = .
replace age_1994 = idade - (year - 1994) if year >= 1994

sort pis
by pis: egen min_age = min(age_1994)
by pis: egen max_age = max(age_1994)

* flags inconsistent age reported for a given individual
* Among individuals with at least one non-missing age reported
gen flag = 0
replace flag = 1 if min_age < max_age & max_age ~= .
replace flag = . if min_age == . | max_age == .

* flags "very" inconsistent age reported for a given individual
* Among individuals with at least one non-missing age reported
gen flag2 = 0
replace flag2 = 1 if max_age - min_age > 5 & max_age ~= .
replace flag2 = . if min_age == . | max_age == .

preserve
keep pis flag flag2
duplicates drop
tab flag
tab flag2
restore

drop flag* min_age max_age


* We now compute the MODE age at 1994
* If there are multiple modes, drop the individual for which the difference in extreme modes is larger than 5 years
* If there are multiple modes, but the difference in extreme modes is equal to 5 years or less, 
* impute/correct age_1994 = (min_mode + max_mode)/2
sort pis
by pis: egen mode_age = mode(age_1994) 
by pis: egen mode_age_min = mode(age_1994), minmode 
by pis: egen mode_age_max = mode(age_1994), maxmode 
gen flag_mode = 0
replace flag_mode = 1 if mode_age_max - mode_age_min > 5 & mode_age_max ~= .
drop if flag_mode == 1
replace mode_age = round((mode_age_min + mode_age_max)/2)
replace age_1994 = mode_age 
drop flag_mode mode_age*

* Now we have imputed / corrected age_1994 for all individuals that appear in the data from 1994 onwards.
* We still need to impute / correct the age of all individuals who do not show up from 1994 onwards -- those
* who show up only between 1986 and 1993

sort pis year

save ${result}interm2, replace


keep pis age_1994
duplicates drop
drop if age_1994 == .
display _N
save ${result}data_age_1994, replace



clear

use ${result}interm2

duplicates drop

keep if age_1994 == . & year <= 1993
keep pis year rem_media idade
drop if idade == .
sort pis year rem_media
by pis: gen obs = _n
egen gp = group(pis)
tsset gp obs
gen lag_idade = L.idade
gen fwd_idade = F.idade
replace idade = lag_idade if lag_idade == fwd_idade & lag_idade ~= .
replace lag_idade = L.idade
replace fwd_idade = F.idade
* Flagging inconsistent age changes, we will delete these individuals
gen flag = 0
replace flag = 1 if (fwd_idade < idade & idade ~= . & fwd_idade ~= .) | (lag_idade > idade & idade ~= . & lag_idade ~= .)
sort pis
by pis: egen flag_incon = total(flag)
replace flag_incon = 1 if flag_incon > 0
drop if flag_incon == 1
drop flag flag_incon

* Impute continuous age to remaining individuals
* If there is a consistent age change: easy
* If there is no age change: compute an average between the lowest and highest possible ages

* Flagging switches
gen switch = 0
replace switch = 1 if idade > lag_idade & lag_idade ~= . & idade ~= .
gen age = .
replace age = 15 if switch == 1 & idade == 2
replace age = 18 if switch == 1 & idade == 3
replace age = 25 if switch == 1 & idade == 4
replace age = 30 if switch == 1 & idade == 5
replace age = 40 if switch == 1 & idade == 6
replace age = 50 if switch == 1 & idade == 7
replace age = 65 if switch == 1 & idade == 8
sort pis
by pis: egen imputation = total(switch)
gen age_1993 = age - (year - 1993)
by pis: egen min_age_1993 = min(age_1993)
by pis: egen max_age_1993 = max(age_1993)
* There are switches that are consistent, in the sense that the age variable increases, 
* but generate inconsitent age patterns, we drop such individuals
gen flag = 0
replace flag = 1 if (max_age_1993 - min_age_1993 > 5) & max_age_1993 ~= .
replace age_1993 = round((max_age_1993+min_age_1993)/2)
drop if flag == 1


* If never switches
sort pis obs
by pis: egen max_obs = max(obs)
gen first_yr = .
replace first_yr = year if obs == 1
gen last_yr = .
replace last_yr = year if obs == max_obs
by pis: egen first_yr_aux = min(first_yr)
by pis: egen last_yr_aux  = max(last_yr)

replace min_age_1993 = .
replace max_age_1993 = .

* For non-switchers, given first and last year of appearance in the data set allows us to infer the
* minimum and maximum possible age in 1993 which is consistent with their observed age category pattern.
* Once we determine the minimum and maximum age in 1993, we impute the individual's age in 1993 as the average between
* the min and max ages
replace min_age_1993 = 10 + 1993 - first_yr_aux if imputation == 0 & idade == 1
replace min_age_1993 = 15 + 1993 - first_yr_aux if imputation == 0 & idade == 2
replace min_age_1993 = 18 + 1993 - first_yr_aux if imputation == 0 & idade == 3
replace min_age_1993 = 25 + 1993 - first_yr_aux if imputation == 0 & idade == 4
replace min_age_1993 = 30 + 1993 - first_yr_aux if imputation == 0 & idade == 5
replace min_age_1993 = 40 + 1993 - first_yr_aux if imputation == 0 & idade == 6
replace min_age_1993 = 50 + 1993 - first_yr_aux if imputation == 0 & idade == 7
replace min_age_1993 = 65 + 1993 - first_yr_aux if imputation == 0 & idade == 8

replace max_age_1993 = 14 + 1993 - last_yr_aux  if imputation == 0 & idade == 1
replace max_age_1993 = 17 + 1993 - last_yr_aux  if imputation == 0 & idade == 2
replace max_age_1993 = 24 + 1993 - last_yr_aux  if imputation == 0 & idade == 3
replace max_age_1993 = 29 + 1993 - last_yr_aux  if imputation == 0 & idade == 4
replace max_age_1993 = 39 + 1993 - last_yr_aux  if imputation == 0 & idade == 5
replace max_age_1993 = 49 + 1993 - last_yr_aux  if imputation == 0 & idade == 6
replace max_age_1993 = 64 + 1993 - last_yr_aux  if imputation == 0 & idade == 7
replace max_age_1993 = 65 + 1993 - first_yr_aux if imputation == 0 & idade == 8

replace age_1993 = round((max_age_1993 + min_age_1993)/2) if imputation == 0 

preserve
keep pis imputation
duplicates drop
tab imputation
restore

keep pis age_1993
sort pis
by pis: egen mode_age_1993_max = mode(age_1993), max
by pis: egen mode_age_1993_min = mode(age_1993), min
replace age_1993 = round((mode_age_1993_max+mode_age_1993_min)/2)
keep pis age_1993
duplicates drop
display _N

sort pis

save ${result}data_age_1993, replace

clear

use ${result}data_age_1993

sort pis
append using ${result}data_age_1994
display _N
replace age_1994 = age_1993 + 1 if age_1994 == .
drop age_1993

sort pis

save ${result}data_1994, replace

clear

use ${result}interm1

sort pis

merge m:1 pis using ${result}data_1994
keep if _merge == 3

* After correcting age in years before 1994, how many actually deviate from
* the age brackets originally reported?
gen age = age_1994 + (year - 1994)
gen cat_age = .
replace cat_age = 1 if age >= 10 & age <= 14
replace cat_age = 2 if age >= 15 & age <= 17
replace cat_age = 3 if age >= 18 & age <= 24
replace cat_age = 4 if age >= 25 & age <= 29
replace cat_age = 5 if age >= 30 & age <= 39
replace cat_age = 6 if age >= 40 & age <= 49
replace cat_age = 7 if age >= 50 & age <= 64
replace cat_age = 8 if age >= 65
gen flag = 0
replace flag = 1 if cat_age ~= idade & idade ~= . & year <= 1993
tab flag if year <= 1993 & idade ~= .

sort pis
by pis: egen max_age = max(age)
* Drop all the individuals whose maximum age during the sample period is equal to 15 or less
drop if max_age <= 15

* Number of individuals after eliminating individuals with inconsistent age and gender
preserve
keep pis
duplicates drop
display _N
restore

drop max_age flag cat_age _merge age_1994

/* 
Age Categories
"1" Child (10-14) - 5
"2" Youth (15-17) - 3
"3" Adolescent (18-24) - 7
"4" Nascent Career (25-29) - 5
"5" Early Career (30-39) - 10
"6" Peak Career (40-49) - 10
"7" Late Career (50-64) - 15
"8" Post Retirement (65-)
*/

erase ${result}interm1.dta
erase ${result}interm2.dta
erase ${result}data_age_1993.dta
erase ${result}data_age_1994.dta

sort pis year rem_media

save ${result}data_gender_age, replace

clear


************************************************************
* Check education consistency / impute and correct education
************************************************************

use ${result}data_gender_age

gen educ = grau_instr
sort pis
by pis: egen max_educ = max(educ)
by pis: egen min_educ = min(educ)
gen flag = 0
replace flag = 1 if max_educ > min_educ & educ ~= .
* Fraction of individuals for whom max_educ > min_educ
tab flag if educ ~= .
drop flag


drop if educ == .
sort pis year rem_media
by pis: gen obs = _n
egen gp = group(pis)
tsset gp obs
gen lag_educ = L.educ
gen fwd_educ = F.educ
replace educ = lag_educ if lag_educ == fwd_educ & lag_educ ~= .
replace lag_educ = L.educ
replace fwd_educ = F.educ

sort pis age year rem_media
by pis: egen max_educ_25 = max(educ) if age >= 25
by pis: egen min_educ_25 = min(educ) if age >= 25
gen flag = 0
replace flag = 1 if max_educ_25 > min_educ_25 & educ ~= . & age >= 25
tab flag if educ ~= . & age >= 25
replace flag = 0
replace flag = 1 if max_educ_25 - min_educ_25 > 1 & educ ~= . & age >= 25
tab flag if educ ~= . & age >= 25

* Impute educ = mode_educ for observations in which the individual is 25 years or older.
sort pis age year rem_media
by pis: egen mode_educ_25 = mode(educ) if age >= 25
by pis: egen min_mode_educ_25 = mode(educ) if age >= 25, min
by pis: egen max_mode_educ_25 = mode(educ) if age >= 25, max
gen educ_25 = educ
replace educ_25 = mode_educ_25 if age >= 25 & mode_educ_25 ~= .
replace educ_25 = round((min_mode_educ_25+max_mode_educ)/2) if age >= 25 & mode_educ_25 == .

* educ_25 is the imputed education using the education information of individuals from age 25 onwards.
* educ_25 is missing for ages less than 25

* Fraction of observations (of individuals aged 25 or older) deviating from educ_25
drop flag
gen flag = 0
replace flag = 1 if educ ~= educ_25 & educ ~= . & educ_25 ~= . & age >= 25
tab flag if educ ~= . & educ_25 ~= . & age >= 25
gen diff = educ - educ_25
tab diff if educ ~= . & educ_25 ~= . & age >= 25

* "Large" Deviations
replace flag = 0
replace flag = 1 if abs(educ - educ_25) > 1 & educ ~= . & educ_25 ~= . & age >= 25
tab flag if educ ~= . & educ_25 ~= . & age >= 25

keep if age >= 25
keep pis educ_25 
duplicates drop

sort pis
save ${result}data_educ, replace
 
clear

use ${result}data_gender_age, replace

sort pis
merge m:1 pis using ${result}data_educ
drop _merge
erase ${result}data_gender_age.dta

sort year

save ${result}new_panel, replace


clear

use ${data2}Minimum_Wage

sort year
keep if year >= 1986 & year <= 2010
merge 1:m year using ${result}new_panel
drop _merge

gen real_rem_dez   = rem_dez*real_min_wage05
gen real_rem_media = rem_media*real_min_wage05

drop rem_dez rem_media min_wage inpc inpc05 real_min_wage05
	
* XX
rename cbo1994 cbo94
	
label var pis         		"Worker ID: PIS"
label var year        		"Year"
label var cnpj        		"14-digit Plant ID: CNPJ"
label var codemun     		"Municipality Code"
label var subs_ibge   		"IBGE Subsector"
label var clas_cnae   		"CNAE 5-digit Industry Classification" 
label var mes_adm     		"Month of Admission"
label var mes_deslig  		"Month of separation"
label var cbo94  		    "Occupation"
label var real_rem_media   	"Real Mean Monthly Earnings in 2005 R$ -- Over Employment Spell within the Year"
label var real_rem_dez     	"Real Earnings Salary in 2005R$"
label var horas_contr 		"Contract Hours"
label var temp_empr   		"Tenure at the Plant"
label var grau_instr  		"Education Level (Raw Data)"
label var educ_25     		"Education Level (Corrected/Imputed)"
label var sexo        		"Gender (Raw Data)"
label var gender      		"Gender (Corrected/Imputed)"
label var idade       		"Age (Raw Data)"
label var age        		"Age (Corrected/Imputed)"

sort pis year

duplicates drop

save ${result}new_panel, replace

erase ${result}data_1994.dta
erase ${result}data_educ.dta
erase ${result}am_rais_final.dta
erase ${result}pis_list.dta
erase ${result}pis_sample.dta

log close
