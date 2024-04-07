* This file constructs a panel of plants, fixing "holes" in the data -- that is,
* it imputes plant level information when plants mistakenly do not report
* a given year

clear

set more off

global root "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Code/DixCarneiro_Kovak_2017/"

global data1  "//storage6/bases/DADOS/RESTRITO/RAIS/Stata/"
global data2  "${root}Data_Other/"
global result "${root}ProcessedData_RAIS/Plants/PanelOfPlants/"

log using ${result}PanelOfPlants.log, replace

forvalues year = 1986/2010{

	local yr = substr("`year'",3,2)

	use ${data1}brasil`yr'

	duplicates drop

	if `year' < 2010{
		keep pis`yr' cnpj`yr' codemun`yr' subs_ibge`yr' rem_dez`yr' mes_adm`yr' mes_deslig`yr' temp_empr`yr'
		rename pis`yr' pis 
	}
	if `year' == 2010{
		keep cpf`yr' cnpj`yr' codemun`yr' subs_ibge`yr' rem_dez`yr' mes_adm`yr' mes_deslig`yr' temp_empr`yr'
		rename cpf`yr' cpf
	}

	rename cnpj`yr' cnpj
	rename codemun`yr' codemun
	rename subs_ibge`yr' subs_ibge
	rename rem_dez`yr' rem_dez
	rename mes_adm`yr' mes_adm
	rename mes_deslig`yr' mes_deslig
	rename temp_empr`yr' temp_empr

	* Only keep individuals with valid December earnings
	keep if rem_dez ~= . & rem_dez > 0

	if `year' < 2010{
		gsort + pis + cnpj - rem_dez
		by pis cnpj: gen obs = _n
	}
	if `year' == 2010{
		gsort + cpf + cnpj - rem_dez
		by cpf cnpj: gen obs = _n
	}
	keep if obs == 1
	drop obs

	* Headcount at the establishment level
	gen ones = 1
	sort cnpj
	by cnpj: egen emp = total(ones)
	by cnpj: egen max_temp_empr = max(temp_empr)
	replace temp_empr = . if temp_empr == max_temp_empr
	by cnpj: egen max2_temp_empr = max(temp_empr)

	sort cnpj subs_ibge
	by cnpj subs_ibge: egen count_subsibge = total(ones)

	sort cnpj codemun
	by cnpj codemun: egen count_codemun = total(ones)

	* We keep one single IBGE subsector for each establishment: the most frequently reported one
	preserve
	sort cnpj count_subsibge
	gsort + cnpj - count_subsibge
	by cnpj: gen obs = _n
	keep if obs == 1
	keep cnpj subs_ibge
	duplicates drop
	sort cnpj
	save ${data1}auxfile1, replace
	restore

	* We keep one single municipality for each establishment: the most frequently reported one
	preserve
	sort cnpj count_codemun
	gsort + cnpj - count_codemun
	by cnpj: gen obs = _n
	keep if obs == 1
	keep cnpj codemun
	duplicates drop
	sort cnpj
	save ${data1}auxfile2, replace
	restore

	* merge information on most reported IBGE subsector and municipality
	keep cnpj emp max_temp_empr max2_temp_empr
	duplicates drop
	sort cnpj
	merge 1:1 cnpj using ${data1}auxfile1
	drop _merge
	merge 1:1 cnpj using ${data1}auxfile2
	drop _merge

	rename max_temp_empr max_temp_empr`year'
	rename max2_temp_empr max2_temp_empr`year'
	rename emp emp`year'
	rename subs_ibge subs_ibge`year'
	rename codemun codemun`year'
	
	sort cnpj

	save ${result}Plants`year', replace

	erase ${data1}auxfile1.dta
	erase ${data1}auxfile2.dta

}

set more off

clear

use ${result}Plants1986

forvalues year = 1987/2010{

	merge 1:1 cnpj using ${result}Plants`year'
	drop _merge
	sort cnpj

}

reshape long max_temp_empr max2_temp_empr emp subs_ibge codemun, i(cnpj) j(year)

drop if trim(cnpj) == "0" | trim(cnpj) == "00000000000000"

sort cnpj

save ${result}PanelPlants, replace


* correspondence between municialities and mmc's
use ${data2}rais_codemun_to_mmc_1991_2010
sort mmc
save ${data2}rais_codemun_to_mmc_1991_2010, replace


*******************************************************************
* Imputing employment, municipality and industry for missing values
*******************************************************************

clear

use ${result}PanelPlants

drop if length(cnpj) ~= 14

destring cnpj, gen(cnpj_num) force
destring codemun, gen(codemun_num) force
destring subs_ibge, gen(subs_ibge_num) force

xtset cnpj_num year

**************
* Fixing holes
**************

*****************************************************
* Fixing one hole
* missing emp at t, non-missing emp at t-1 and at t+1
*****************************************************

gen fwd_emp  = F.emp
gen lag_emp  = L.emp

gen fwd_codemun  = F.codemun_num
gen lag_codemun  = L.codemun_num

gen fwd_subs_ibge  = F.subs_ibge_num
gen lag_subs_ibge  = L.subs_ibge_num

gen flag = (lag_emp ~= . & lag_emp > 0) & (fwd_emp ~= . & fwd_emp > 0) & emp == .

gen imputed = 0

replace emp = (lag_emp + fwd_emp)/2 if flag == 1
replace imputed = 1 if flag == 1
replace codemun_num = lag_codemun if flag == 1
replace subs_ibge_num = lag_subs_ibge if flag == 1

tostring codemun_num, gen(aux_mun) force
tostring subs_ibge_num, gen(aux_subs) force

replace codemun = aux_mun if flag == 1
replace subs_ibge = aux_subs if flag == 1

**********************************************************
* Fixing two holes
* missing emp at t and t+1, non-missing emp at t-1 and t+2
**********************************************************

gen fwd2_emp  = F2.emp
gen lag2_emp  = L2.emp

gen fwd2_codemun  = F2.codemun_num
gen lag2_codemun  = L2.codemun_num

gen fwd2_subs_ibge  = F2.subs_ibge_num
gen lag2_subs_ibge  = L2.subs_ibge_num

gen flag2 = (lag_emp ~= . & lag_emp > 0) & (emp == .) & (fwd_emp == .) & (fwd2_emp ~= . & fwd2_emp > 0)
gen flag3 = (lag2_emp ~= . & lag2_emp > 0) & (lag_emp == .) & (emp == .) & (fwd_emp ~= . & fwd_emp > 0)

replace emp = lag_emp  + (fwd2_emp - lag_emp)/3     if flag2 == 1
replace imputed = 1 if flag2 == 1
replace emp = lag2_emp + (fwd_emp - lag2_emp)*(2/3) if flag3 == 1
replace imputed = 1 if flag3 == 1

replace codemun_num = lag_codemun if flag2 == 1
replace codemun_num = lag2_codemun if flag3 == 1

replace subs_ibge_num = lag_subs_ibge if flag2 == 1
replace subs_ibge_num = lag2_subs_ibge if flag3 == 1

cap drop aux_mun
cap drop aux_subs

tostring codemun_num, gen(aux_mun) force
tostring subs_ibge_num, gen(aux_subs) force

replace codemun = aux_mun if flag2 == 1 | flag3 == 1
replace subs_ibge = aux_subs if flag2 == 1 | flag3 == 1

****************************************************************
* Fixing three holes
* missing emp at t,  t+1 and t+2, non-missing emp at t-1 and t+3
****************************************************************

gen fwd3_emp  = F3.emp
gen lag3_emp  = L3.emp

gen fwd3_codemun  = F3.codemun_num
gen lag3_codemun  = L3.codemun_num

gen fwd3_subs_ibge  = F3.subs_ibge_num
gen lag3_subs_ibge  = L3.subs_ibge_num

gen flag4 = (lag_emp ~= . & lag_emp > 0) & (emp == .) & (fwd_emp == .) & (fwd2_emp == .) & (fwd3_emp ~= . & fwd3_emp > 0)
gen flag5 = (lag2_emp ~= . & lag2_emp > 0) & (lag_emp == .) & (emp == .) & (fwd_emp == .) & (fwd2_emp ~= . & fwd2_emp > 0)
gen flag6 = (lag3_emp ~= . & lag3_emp > 0) & (lag2_emp == .) & (lag_emp == .) & (emp == .) & (fwd_emp ~= . & fwd_emp > 0)

replace emp = lag_emp  + (fwd3_emp - lag_emp)/4      if flag4 == 1
replace imputed = 1 if flag4 == 1
replace emp = lag2_emp + (fwd2_emp - lag2_emp)*(2/4) if flag5 == 1
replace imputed = 1 if flag5 == 1
replace emp = lag3_emp  + (fwd_emp - lag3_emp)*(3/4) if flag6 == 1
replace imputed = 1 if flag6 == 1

replace codemun_num = lag_codemun if flag4 == 1
replace codemun_num = lag2_codemun if flag5 == 1
replace codemun_num = lag3_codemun if flag6 == 1

replace subs_ibge_num = lag_subs_ibge if flag4 == 1
replace subs_ibge_num = lag2_subs_ibge if flag5 == 1
replace subs_ibge_num = lag3_subs_ibge if flag6 == 1

cap drop aux_mun
cap drop aux_subs

tostring codemun_num, gen(aux_mun) force
tostring subs_ibge_num, gen(aux_subs) force

replace codemun = aux_mun if flag4 == 1 | flag5 == 1 | flag6 == 1
replace subs_ibge = aux_subs if flag4 == 1 | flag5 == 1 | flag6 == 1

keep cnpj year emp subs_ibge codemun imputed max_temp_empr max2_temp_empr

replace emp = 0 if emp == .

sort cnpj year

save ${result}PanelPlantsFinal, replace


clear

use ${result}PanelPlantsFinal
sort codemun
merge m:1 codemun using ${data2}rais_codemun_to_mmc_1991_2010
drop if _merge == 2
drop _merge
sort cnpj year
save ${result}PanelPlantsFinal, replace


clear

use ${result}PanelPlantsFinal

preserve

destring subs_ibge, gen(subs_ibge_num) force
replace subs_ibge_num = . if subs_ibge_num == 9999
sort cnpj subs_ibge_num
by cnpj subs_ibge_num: egen count_heads = total(emp)
keep cnpj subs_ibge_num count_heads max_temp_empr max2_temp_empr
duplicates drop
drop if subs_ibge_num == .
gsort + cnpj - count_heads
by cnpj: gen obs = _n
keep if obs == 1
drop obs
sort cnpj
save ${result}tempfile, replace

restore

sort cnpj
merge m:1 cnpj using ${result}tempfile
drop _merge count_heads
erase ${result}tempfile.dta

tostring subs_ibge_num, replace force
rename subs_ibge_num subs_ibge0

sort cnpj year

save ${result}PanelPlantsFinal, replace



clear

use ${result}PanelPlantsFinal

* first year cnpj 14-digit appears in the sample
sort cnpj year
gen active = .
replace active = 1 if emp > 0
by cnpj: egen first_year = min(year*active)
drop active 

gen cnpj8 = substr(cnpj,1,8)

save ${result}PanelPlantsFinal, replace



forvalues year = 1986/2010{

	erase ${result}Plants`year'.dta
	
}

erase ${result}PanelPlants.dta

log close

