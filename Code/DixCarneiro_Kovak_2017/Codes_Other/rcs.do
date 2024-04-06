******************************************************************************
* rcs.do
* Dix-Carneiro and Kovak AER replication files
*
* calculates regional commodity price shocks using data from the IMF Primary
* Commodity Price Series.
*
* Output: /Data/rcs.dta
******************************************************************************

cd "${root}Codes_Other"
log using rcs.txt, text replace

***************************************
* Change in commodity prices from IMF data

use ../Data_Other/imf_data, clear
keep if inrange(year,1991,2010)

keep year imf_code imf_price
reshape wide imf_price, i(imf_code) j(year)

forvalues yr = 1992/2010 {
	gen dln_imf_price`yr' = ln(imf_price`yr') - ln(imf_price1991)
}

sort imf_code
sum dln_imf_price*
save dln_imf_price, replace

***************************************
* Detailed commodity industry shares by region

* 1991 industry employment shares for each industry
use ../Data_Census/code_sample.dta if year==1991
keep if atividade < .
collapse (sum) emp=xweighti, by(atividade mmc)
bysort mmc: egen totemp = sum(emp)
gen empshare = emp/totemp
keep mmc atividade empshare
order mmc atividade empshare
sort mmc atividade

* merge in commodity price changes
merge m:1 atividade using ../Data_Other/atividade_to_imf_code
tab atividade if _merge == 2, m // should be none
drop _merge
sort imf_code
merge m:1 imf_code using dln_imf_price
tab imf_code if _merge == 1, m // should all be missing
tab imf_code if _merge == 2, m // bunch of these for unused IMF indexes
drop _merge

* give non-commodity industries zero price change
forvalues yr = 1992/2010 {
	replace dln_imf_price`yr' = 0 if imf_code >= .
}

* save intermediate file
keep if mmc < .
save rcs_elements, replace

* generate weighted average with zeros on non-commodity industries
forvalues yr = 1992/2010 {
	gen element`yr' = empshare * dln_imf_price`yr'
}
collapse (sum) element*, by(mmc)
forvalues yr = 1992/2010 {
	rename element`yr' rcs_all`yr'
}
sum rcs_all*
sort mmc 
save rcs_all, replace

* generate weighted average across commodity industries only
use rcs_elements, clear
keep if imf_code < . // keep only commodity industries with IMF prices
bysort mmc: egen totshare = sum(empshare)
replace empshare = empshare / totshare // make shares sum to 1 across commodity industries
by mmc: egen test = sum(empshare)
sum test // should be all 1's
drop test
forvalues yr = 1992/2010 {
	gen element`yr' = empshare * dln_imf_price`yr'
}
collapse (sum) element*, by(mmc)
forvalues yr = 1992/2010 {
	rename element`yr' rcs_com`yr'
}
sum rcs_com*
sort mmc 
save rcs_com, replace

* combine both sets of shocks
merge 1:1 mmc using rcs_all
tab mmc if _merge < 3 // should be none
drop _merge
sort mmc
save ../Data/rcs, replace

erase dln_imf_price.dta
erase rcs_all.dta
erase rcs_com.dta
erase rcs_elements.dta

log close
cd "${root}"
