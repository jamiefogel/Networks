
* This file constructs a panel of plants, fixing "holes" in the data -- that is,
* it imputes plant level information when plants mistakenly do not report
* a given year.
* Code is similar to 4a_PanelOfPlants.do, but treat plant-region pairs as separate
* establishments. When a plant moves regions, that should be counted as exit from 
* the original region and entry into the destination region.

clear

set more off

global root "C:\Users\rd123\Dropbox\DixCarneiroKovakRodriguez\ReplicationFiles\"

global data1 "F:\RAIS\Data_Brazil\RAIS_Stata\"
global data2 "${root}Data_Other\"
global result "${root}ProcessedData_RAIS\JobDestruction_JobCreation\"

log using ${result}PanelOfPlantMMCpairs, replace

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

	display _N
	* Only keep individuals with valid December earnings
	keep if rem_dez ~= . & rem_dez > 0
	display _N
	
	* Drop wrong CNPJ's
	drop if trim(cnpj) == "00000000000000"
	display _N
	drop if length(cnpj) ~= 14
	display _N
	
	* Keep only 1 observation per ID (PIS or CPF) within the plant
	* If the ID is equal to zero, keep all of them -- a few firms report all
	* of their employees with a "0" ID. Some of them are quite large. Disregarding these
	* firms in some years can potentially create spurious job creation or destruction in
	* some years
	if `year' < 2010{
		gsort + cnpj + pis - rem_dez
		by cnpj pis: gen obs = _n
		replace obs = 1 if trim(pis) == "0" | trim(pis) == "00000000000"
	}
	if `year' == 2010{
		gsort + cnpj + cpf - rem_dez
		by cnpj cpf: gen obs = _n
		replace obs = 1 if trim(cpf) == "0" | trim(cpf) == "00000000000"
	}
	
	keep if obs == 1
	drop obs
	
	gen ones = 1
	sort cnpj codemun
	by cnpj codemun: egen count_codemun = total(ones)

	* We keep one single municipality for each establishment: the most frequently reported one
	preserve
	sort cnpj count_codemun
	gsort + cnpj - count_codemun
	by cnpj: gen obs = _n
	keep if obs == 1
	keep cnpj codemun
	duplicates drop
	sort cnpj
	save ${result}auxfile2, replace
	restore

	drop codemun
	sort cnpj
	merge m:1 cnpj using ${result}auxfile2
	drop _merge
	erase ${result}auxfile2.dta
	
	sort codemun
	merge m:1 codemun using ${data2}rais_codemun_to_mmc_1991_2010
	keep if _merge == 1 | _merge == 3
	drop _merge
	
	display _N
	drop if trim(mmc) == "" | trim(mmc) == "." | length(mmc) ~= 5
	display _N
	
	gen cnpj_mmc = cnpj + mmc
	sort cnpj_mmc subs_ibge
	by cnpj_mmc: egen emp = total(ones)
	by cnpj_mmc subs_ibge: egen emp_ind = total(ones)
	by cnpj_mmc: egen max_temp_empr = max(temp_empr)
	
	keep cnpj_mmc emp subs_ibge emp_ind max_temp_empr
	duplicates drop
	
	* rename emp emp`year' 
	
	gen year = `year'
	
	sort cnpj_mmc

	save ${result}Plants`year', replace
	
}

set more off

clear

use ${result}Plants1986

forvalues year = 1987/2010{

	append using ${result}Plants`year'
	
}

preserve
	* selecting unique subs_ibge for each cnpj+mcc combination
	sort cnpj_mmc subs_ibge
	by cnpj_mmc subs_ibge: egen count = total(emp_ind)
	keep cnpj_mmc subs_ibge count
	duplicates drop
	gsort + cnpj_mmc - count
	by cnpj_mmc: gen obs = _n
	by cnpj_mmc: egen max_obs = max(obs)
	drop if max_obs > 1 & subs_ibge == "9999"
	drop obs max_obs
	gsort + cnpj_mmc - count
	by cnpj_mmc: gen obs = _n
	keep if obs == 1
	keep cnpj_mmc subs_ibge
	duplicates drop
	rename subs_ibge subs_ibge0
	sort cnpj_mmc
	save ${result}cnpj_mmc_subs_ibge, replace
restore

sort cnpj_mmc
merge m:1 cnpj_mmc using ${result}cnpj_mmc_subs_ibge
drop _merge

erase ${result}cnpj_mmc_subs_ibge.dta

keep cnpj_mmc emp year subs_ibge0 max_temp_empr
duplicates drop

reshape wide emp max_temp_empr, i(cnpj_mmc subs_ibge0) j(year)

forvalues year = 1986/2010{
	replace emp`year' = 0 if emp`year' == .
}

reshape long emp max_temp_empr, i(cnpj_mmc subs_ibge0) j(year)

sort cnpj_mmc year
rename subs_ibge0 subs_ibge

save ${result}PanelPlants, replace


*******************************************************************
* Imputing employment, municipality and industry for missing values
*******************************************************************

clear

use ${result}PanelPlants

duplicates drop

egen cnpj_mmc_num = group(cnpj_mmc)

xtset cnpj_mmc_num year

**************
* Fixing holes
**************

*****************************************************
* Fixing one hole
* missing emp at t, non-missing emp at t-1 and at t+1
*****************************************************

gen fwd_emp  = F.emp
gen lag_emp  = L.emp

gen flag = (lag_emp ~= . & lag_emp > 0) & (fwd_emp ~= . & fwd_emp > 0) & emp == .

gen imputed = 0

replace emp = (lag_emp + fwd_emp)/2 if flag == 1
replace imputed = 1 if flag == 1

**********************************************************
* Fixing two holes
* missing emp at t and t+1, non-missing emp at t-1 and t+2
**********************************************************

gen fwd2_emp  = F2.emp
gen lag2_emp  = L2.emp

gen flag2 = (lag_emp ~= . & lag_emp > 0) & (emp == .) & (fwd_emp == .) & (fwd2_emp ~= . & fwd2_emp > 0)
gen flag3 = (lag2_emp ~= . & lag2_emp > 0) & (lag_emp == .) & (emp == .) & (fwd_emp ~= . & fwd_emp > 0)

replace emp = lag_emp  + (fwd2_emp - lag_emp)/3     if flag2 == 1
replace imputed = 1 if flag2 == 1
replace emp = lag2_emp + (fwd_emp - lag2_emp)*(2/3) if flag3 == 1
replace imputed = 1 if flag3 == 1

****************************************************************
* Fixing three holes
* missing emp at t,  t+1 and t+2, non-missing emp at t-1 and t+3
****************************************************************

gen fwd3_emp  = F3.emp
gen lag3_emp  = L3.emp

gen flag4 = (lag_emp ~= . & lag_emp > 0) & (emp == .) & (fwd_emp == .) & (fwd2_emp == .) & (fwd3_emp ~= . & fwd3_emp > 0)
gen flag5 = (lag2_emp ~= . & lag2_emp > 0) & (lag_emp == .) & (emp == .) & (fwd_emp == .) & (fwd2_emp ~= . & fwd2_emp > 0)
gen flag6 = (lag3_emp ~= . & lag3_emp > 0) & (lag2_emp == .) & (lag_emp == .) & (emp == .) & (fwd_emp ~= . & fwd_emp > 0)

replace emp = lag_emp  + (fwd3_emp - lag_emp)/4      if flag4 == 1
replace imputed = 1 if flag4 == 1
replace emp = lag2_emp + (fwd2_emp - lag2_emp)*(2/4) if flag5 == 1
replace imputed = 1 if flag5 == 1
replace emp = lag3_emp  + (fwd_emp - lag3_emp)*(3/4) if flag6 == 1
replace imputed = 1 if flag6 == 1

keep cnpj_mmc cnpj_mmc_num year emp imputed subs_ibge max_temp_empr

replace emp = 0 if emp == .

sort cnpj_mmc year

gen cnpj = substr(cnpj_mmc,1,14)
gen mmc  = substr(cnpj_mmc,15,5)

save ${result}PanelPlantsFinal, replace

gen year_entry = year - int(max_temp_empr / 12)
replace year_entry = . if year >= 1992
sort cnpj_mmc year
by cnpj_mmc: egen avg_year_entry = mean(year_entry)
gen age1991 = int(1991 - avg_year_entry)

save ${result}PanelPlantsFinal, replace

erase ${result}PanelPlants.dta

forvalues year = 1986/2010{
	erase ${result}Plants`year'.dta
}

log close
