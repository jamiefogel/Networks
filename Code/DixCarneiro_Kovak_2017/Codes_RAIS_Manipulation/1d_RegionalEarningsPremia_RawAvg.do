
* This file generates regional earnings premia and regional employment files
* from the raw RAIS data (1986 to 2010).
* We compute averages of log-earnings, without controls.
* Observation: our 2010 data does not feature PIS individual identifiers, 
* only CPF individual identifiers.

clear all
set more off
capture log close
set matsize 10000
set varabbrev off

capture log close

global root "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017"

global data1  "\\storage6\bases\DADOS\RESTRITO\RAIS\Stata\"
global data2  "${root}Data_Other\"
global result "${root}ProcessedData_RAIS\RegionalEarnPremia\"

log using ${result}RegionalEarnPremia_RawAvg.log, replace

set min_memory 21g

* correspondence between municialities and mmc's
use ${data2}rais_codemun_to_mmc_1991_2010
tostring mmc, replace
sort mmc
save ${data2}rais_codemun_to_mmc_1991_2010, replace

****************************************
* We use rem_dez as our earnings measure
****************************************

forvalues year = 1986/2009{
	
	clear all
	local y = substr("`year'", 3, 2) 

	use pis`y' codemun`y' mes_adm`y' mes_deslig`y' subs_ibge`y' rem_dez`y' grau_instr`y' sexo`y' idade`y' using ${data1}brasil`y'

	* Eliminate the year suffix from all the variable names
	foreach var of varlist *`y' {
		local newvar = regexr("`var'","`y'","")
		rename `var' `newvar'
	}

	display _N

	* Drop individuals with obviously wrong PIS numbers
	drop if trim(pis) == "0"
	drop if trim(pis) == "00000000000"
	drop if strlen(trim(pis)) ~= 11
	
	* idade is discrete before 1994 but continuous from 1994 onwards
	* We transform it into a discrete variable from 1994 onwards
	if `year' >= 1994 {
		gen age = .
		replace age = 1 if idade >= 10 & idade <= 14
		replace age = 2 if idade >= 15 & idade <= 17
		replace age = 3 if idade >= 18 & idade <= 24
		replace age = 4 if idade >= 25 & idade <= 29
		replace age = 5 if idade >= 30 & idade <= 39
		replace age = 6 if idade >= 40 & idade <= 49
		replace age = 7 if idade >= 50 & idade <= 64
		replace age = 8 if idade >= 65
		replace idade = age
		drop age
	}
	
	* grau_instr = 10 or 11 are only available for a few years (MA and PhD)
	replace grau_instr = 9 if grau_instr == 10 | grau_instr == 11
	
	* We only keep individuals with age 18 to 64
	* This yields 5 levels of age: 3, 4, 5, 6 and 7
	keep if idade >= 3 & idade <= 7
	* We only keep individuals with valid education categories
	keep if grau_instr >= 1 & grau_instr <= 9
	display _N
	

	* Only consider jobs in December with valid earnings
	keep if rem_dez ~=. & rem_dez ~= 0
	display _N
	* But we drop job observations if the individual was hired or fired in December, 
	* in order to guarantee that December earnings reflect payments for a full month
	drop if trim(mes_deslig) == "12" | trim(mes_adm) == "12"
	display _N
	
	* Drop "abnormal" individuals: individuals with multiple education levels
	* among different jobs in December, and individuals holding jobs in two different
	* geographic "large" regions -- Sul, Sudeste, Norte, Nordeste or Centro-Oeste
	sort pis
	by pis: egen max_educ = max(grau_instr)
	by pis: egen min_educ = min(grau_instr)
	gen region = substr(codemun,1,1)
	destring region, force replace
	by pis: egen max_region = max(region)
	by pis: egen min_region = min(region)
	* I allow a discrepancy of one level of education, but no more
	gen flag1 = (max_educ > min_educ + 1)
	gen flag2 = (max_region > min_region)
	drop if flag1 == 1 | flag2 == 1
	display _N
	drop flag* max_educ min_educ max_region min_region region
	
	* Keep only highest paying job in December
	gsort + pis - rem_dez
	by pis: gen obs = _n
	keep if obs == 1
	drop obs
	display _N	
	
	tab subs_ibge
	* Drop Other/Ignored Sectors
	drop if trim(subs_ibge) == "9999"
	* Drop Public Administration
	drop if trim(subs_ibge) == "5719"
	display _N

	* Merge to get mmc codes
    sort codemun
	merge m:1 codemun using ${data2}rais_codemun_to_mmc_1991_2010
	keep if _merge == 3
	drop _merge
	drop if trim(mmc) == "" | trim(mmc) == "."
	display _N
	
	* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
	* Zona Franca de Manaus
	drop if trim(mmc) == "23014" | trim(mmc) == "13007"

	sort mmc
	gen ones = 1
	* Number of observations per mmc
	by mmc: egen obs_dez = total(ones)
	drop ones

	gen log_rem = ln(rem_dez)
	* Generate categorical variables
	gen female = (sexo > 1)
	qui tab grau_instr, gen(educ)
	qui tab idade, gen(age)

	* Generate number of mmc's local (do it every year in case some mmcs do not appear in some years)
	sort mmc
	egen mmc2 = group(mmc)
	* Generate mmc categorical variables for mmc's
	qui tab mmc, gen(region)
	qui sum mmc2, meanonly
	local number_mmc = r(max)
	display `number_mmc'
	
	/*
	* Check if the correspondence between region`i' and mmc2 is "right"
	gen flag = 0
	set more off
	forvalues i = 1(1)`number_mmc'{
		qui replace flag = 1 if region`i' == 1 & mmc2 == `i'
	}
	sum flag, d
	* if r(max) ~= r(min) then we generate the variable flag once again, which will
	* make the program to be aborted
	if r(max) ~= r(min){
		gen flag = 0
	}
	drop flag
	*/
	
	* Same for industries
	egen subs_ibge2 = group(subs_ibge)
	* Generate mmc categorical variables for industries
	qui tab subs_ibge, gen(ind)
	qui sum subs_ibge2, meanonly
	local number_ind = r(max)
	display `number_ind'

	* Generate a correspondence table between mmc and mmc2
	preserve
	order pis codemun mmc mmc2
	keep mmc mmc2 obs_dez
	duplicates drop
	sort mmc2
	save ${result}mmc_obs_dez, replace
	restore

	* Regression without a constant: we recover parameters for all mmc's
	reg log_rem region1-region`number_mmc', noc vce(robust)

	clear
	
	* Generate dataset with the estimates
	matrix coeff_rem = e(b)'
	qui svmat coeff_rem
	matrix VAR_rem = e(V)
	matrix SE_rem = J(`number_mmc',1,0)
	forvalues i = 1(1)`number_mmc'{
	matrix SE_rem[`i',1] = sqrt(VAR_rem[`i',`i'])
	}
	qui svmat SE_rem
	gen mmc2 = _n
	keep if mmc2 <= `number_mmc'
	sort mmc2
	merge 1:1 mmc2 using ${result}mmc_obs_dez
	keep if _merge == 3
	drop _merge  
	rename coeff_rem1 coeff_rem_dez
	rename SE_rem1 SE_rem_dez
	drop mmc2 
	capture gen year = `year'
	order mmc year coeff_rem_dez SE_rem_dez obs_dez
	save ${result}mmc_reg_`year', replace

}

*********************************************************************************
* The 2010 dataset did not come with PIS, but we can use CPF in order to identify
* workers
*********************************************************************************

clear

set more off

use cpf10 codemun10 mes_adm10 mes_deslig10 subs_ibge10 rem_dez10 grau_instr10 sexo10 idade10 using ${data1}brasil10

* Eliminate the year suffix from all the variable names
foreach var of varlist *10 {
	local newvar = regexr("`var'","10","")
	rename `var' `newvar'
}

display _N

* Drop individuals with obviously wrong CPF numbers
drop if trim(cpf) == "0"
drop if trim(cpf) == "00000000000"
	
* idade is discrete before 1994 but continuous from 1994 onwards
* We transform it into a discrete variable from 1994 onwards
gen age = .
replace age = 1 if idade >= 10 & idade <= 14
replace age = 2 if idade >= 15 & idade <= 17
replace age = 3 if idade >= 18 & idade <= 24
replace age = 4 if idade >= 25 & idade <= 29
replace age = 5 if idade >= 30 & idade <= 39
replace age = 6 if idade >= 40 & idade <= 49
replace age = 7 if idade >= 50 & idade <= 64
replace age = 8 if idade >= 65
replace idade = age
drop age

* grau_instr = 10 or 11 are only available for a few years (MA and PhD)
replace grau_instr = 9 if grau_instr == 10 | grau_instr == 11
	
* We only keep individuals with age 18 to 64
* This yields 5 levels of age: 3, 4, 5, 6 and 7
keep if idade >= 3 & idade <= 7
* We only keep individuals with valid education categories
keep if grau_instr >= 1 & grau_instr <= 9
display _N


* Only consider jobs in December with valid earnings
keep if rem_dez ~=. & rem_dez ~= 0
display _N
* But we drop job observations if the individual was hired or fired in December, 
* in order to guarantee that December earnings reflect payments for a full month
drop if trim(mes_deslig) == "12" | trim(mes_adm) == "12"
display _N
	
* Drop "abnormal" individuals: individuals with multiple education levels
* among different jobs in December, and individuals holding jobs in two different
* geographic "large" regions -- Sul, Sudeste, Norte, Nordeste or Centro-Oeste
sort cpf
by cpf: egen max_educ = max(grau_instr)
by cpf: egen min_educ = min(grau_instr)
gen region = substr(codemun,1,1)
destring region, force replace
by cpf: egen max_region = max(region)
by cpf: egen min_region = min(region)
* I allow a discrepancy of one level of education, but no more
gen flag1 = (max_educ > min_educ + 1)
gen flag2 = (max_region > min_region)
drop if flag1 == 1 | flag2 == 1
display _N
drop flag* max_educ min_educ max_region min_region region
	
* Keep only highest paying job in December
gsort + cpf - rem_dez
by cpf: gen obs = _n
keep if obs == 1
drop obs
display _N	
	
tab subs_ibge
* Drop Other/Ignored Sectors
drop if trim(subs_ibge) == "9999"
* Drop Public Administration
drop if trim(subs_ibge) == "5719"
display _N


* Merge to get mmc codes
sort codemun
merge m:1 codemun using ${data2}rais_codemun_to_mmc_1991_2010
keep if _merge == 3
drop _merge
drop if trim(mmc) == "" | trim(mmc) == "."
display _N

* Dropping "23014" as it has 0 observations in 1991 and "13007" which is
* Zona Franca de Manaus
drop if trim(mmc) == "23014" 
drop if trim(mmc) == "13007"

sort mmc
gen ones = 1
* Number of observations per mmc
by mmc: egen obs_dez = total(ones)
drop ones

gen log_rem = ln(rem_dez)
* Generate categorical variables
gen female = (sexo > 1)
qui tab grau_instr, gen(educ)
qui tab idade, gen(age)

* Generate number of mmc's local (do it every year in case some mmcs do not appear in some years)
sort mmc
egen mmc2 = group(mmc)
* Generate mmc categorical variables for mmc's
qui tab mmc, gen(region)
qui sum mmc2, meanonly
local number_mmc = r(max)
display `number_mmc'

* Check if the correspondence between region`i' and mmc2 is "right"
gen flag = 0
set more off
forvalues i = 1(1)`number_mmc'{
	qui replace flag = 1 if region`i' == 1 & mmc2 == `i'
}
sum flag, d
* if r(max) ~= r(min) then we generate the variable flag once again, which will
* make the program to be aborted
if r(max) ~= r(min){
	gen flag = 0
}
drop flag
	
* Same for industries
egen subs_ibge2 = group(subs_ibge)
* Generate mmc categorical variables for industries
qui tab subs_ibge, gen(ind)
qui sum subs_ibge2, meanonly
local number_ind = r(max)
display `number_ind'

* Generate a correspondence table between mmc and mmc2
preserve
order cpf codemun mmc mmc2
keep mmc mmc2 obs_dez
duplicates drop
sort mmc2
save ${result}mmc_obs_dez, replace
restore

* Regression without a constant: we recover parameters for all mmc's
reg log_rem region1-region`number_mmc', noc vce(robust)

clear
	
* Generate dataset with the estimates
matrix coeff_rem = e(b)'
qui svmat coeff_rem
matrix VAR_rem = e(V)
matrix SE_rem = J(`number_mmc',1,0)
forvalues i = 1(1)`number_mmc'{
matrix SE_rem[`i',1] = sqrt(VAR_rem[`i',`i'])
}
qui svmat SE_rem
gen mmc2 = _n
keep if mmc2 <= `number_mmc'
sort mmc2
merge 1:1 mmc2 using ${result}mmc_obs_dez
keep if _merge == 3
drop _merge  
rename coeff_rem1 coeff_rem_dez
rename SE_rem1 SE_rem_dez
drop mmc2 
capture gen year = 2010
order mmc year coeff_rem_dez SE_rem_dez obs_dez
save ${result}mmc_reg_2010, replace

*********************************
* APPEND all years
*********************************

clear

use ${result}mmc_reg_1986, clear


forvalues year = 1987/2010{
	append using ${result}mmc_reg_`year'
}

label var mmc           "Minimum Comparable Microregions"
label var year          "Year"
label var coeff_rem_dez "mmc intercept: Mincer Regressions using Earnings in December"
label var SE_rem_dez    "Standard Error on mmc intercept: Mincer Regressions using Earnings in December"
label var obs_dez       "Number of observations in December"

sort mmc year

save ${result}mmcEarnPremia_rawavg_1986_2010, replace

forvalues year = 1986/2010{
	erase ${result}mmc_reg_`year'.dta
}

erase ${result}mmc_obs_dez.dta

log close
