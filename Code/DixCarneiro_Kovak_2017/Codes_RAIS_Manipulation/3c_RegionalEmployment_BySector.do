
* Computes aggregate formal employment for each region x industry cell. 
* Uses raw RAIS data (1986 to 2010).

clear all
set more off
capture log close
set matsize 10000
set varabbrev off

capture log close

global root "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Code/DixCarneiro_Kovak_2017/"

global data1  "//storage6/bases/DADOS/RESTRITO/RAIS/Stata/"
global data2  "${root}Data_Other/"
global result "${root}ProcessedData_RAIS/RegionalEmployment/"

log using ${result}RegionalEmployment_BySector.log, replace

set min_memory 21g

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
	keep if grau_instr >= 1 & grau_instr <= 9 & grau_instr ~= .
	display _N
	
	tab subs_ibge
	* Keep all sectors, exclude only public administration and other/ignored
	drop if trim(subs_ibge) == "5719"
	drop if trim(subs_ibge) == "9999"
	display _N
	
	* Only consider jobs in December with valid earnings
	keep if rem_dez ~=. & rem_dez ~= 0
	display _N
	* But we do not drop job observations if the individual was hired or fired in December,
	* as we did in the regional premia regressions
	
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

	* Merge to get mmc codes
    sort codemun
	merge m:1 codemun using ${data2}rais_codemun_to_mmc_1991_2010
	keep if _merge == 3
	drop _merge
	drop if trim(mmc) == "" | trim(mmc) == "."
	display _N
	
	gen ones = 1
	
	sort mmc subs_ibge
	by mmc subs_ibge: egen emp_dez  = total(ones)

	keep mmc subs_ibge emp_dez
	duplicates drop
	
	gen year = `year'
	
	sort mmc
	
	save ${result}MmcSubsIbgeEmp`year', replace

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
keep if grau_instr >= 1 & grau_instr <= 9 & grau_instr ~= .
display _N

tab subs_ibge
* Keep all sectors, exclude only public administration and other/ignored
drop if trim(subs_ibge) == "5719"
drop if trim(subs_ibge) == "9999"
display _N

* Only consider jobs in December with valid earnings
keep if rem_dez ~=. & rem_dez ~= 0
display _N
* But we do not drop job observations if the individual was hired or fired in December, 
* as we did in the regional premia regressions

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

* Merge to get mmc codes
sort codemun
merge m:1 codemun using ${data2}rais_codemun_to_mmc_1991_2010
keep if _merge == 3
drop _merge
drop if trim(mmc) == "" | trim(mmc) == "."
display _N

gen ones = 1
	
sort mmc subs_ibge
by mmc subs_ibge: egen emp_dez = total(ones)

keep mmc subs_ibge emp_dez
duplicates drop

gen year = 2010

sort mmc

save ${result}MmcSubsIbgeEmp2010, replace
	
*********************************
* APPEND all years
*********************************

clear

use ${result}MmcSubsIbgeEmp1986, clear

forvalues year = 1987/2010{
	append using ${result}MmcSubsIbgeEmp`year'
}

sort mmc subs_ibge year

save ${result}mmcEmployment_bysector_1986_2010, replace

forvalues year = 1986/2010{
	erase ${result}MmcSubsIbgeEmp`year'.dta
}

log close
