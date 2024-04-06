******************************************************************************
* adao_shock_mmc.do
* Dix-Carneiro and Kovak AER replication files
*
* Calculates the shock in equation (16) of Rodrigo Adao's job market paper.
*
* Output: /Data/adao_shock_mmc.dta
******************************************************************************

cd "${root}Codes_Other"
log using adao_shock_mmc.txt, text replace


******************************
* calculate phi weights in equation (16) by mmc

use ../Data_Census/code_sample if year==1991

* merge Adao commodity industry codes
sort atividade
merge m:1 atividade using ../Data_Other/atividade_to_ind_adao
tab atividade if _merge == 1
tab atividade if _merge == 2 // nonsense code 30

* restrict sample
keep if ind_adao < . 
keep if employed == 1 // employed (only drops 1 person with a non-missing ind_adao code
keep if female == 0 // men only
keep if race == 1 // white only

* sum earnings at main job by each mmc x ind_adao 
collapse (sum) earn=ymain [pw=xweighti], by(mmc ind_adao)

* calculate weights by mmc x ind_adao
bysort mmc: egen totearn = sum(earn)
gen phi = earn / totearn
sum phi

* save weights
keep mmc ind_adao phi
sort mmc ind_adao

******************************
* calculate weighted average price by mmc and year

* merge prices with weights
expand 31
bysort mmc ind_adao: gen year = _n + 1980 - 1
tab year
sort ind_adao year
merge m:1 ind_adao year using ../Data_Other/commodity_prices_adao
drop _merge // perfect match

* calculate yearly weighted average
gen element = phi * price
collapse (sum) wgt_price=element, by(mmc year)

* changes in weighted average prices
reshape wide wgt_price, i(mmc) j(year)
forvalues yr = 1992/2010 {
	gen adao_shock_1991_`yr' = wgt_price`yr' - wgt_price1991
}
sum adao_shock_1991_2010

* save results
keep mmc adao*
sort mmc
save ../Data/adao_shock_mmc, replace


log close
cd "${root}"
