******************************************************************************
* mmc_rer.do
* Dix-Carneiro and Kovak AER replication files
*
* calculates regional commodity price shocks using data from the IMF Primary
* Commodity Price Series.
*
* Output: /Data/mmc_rer.dta
******************************************************************************

cd "${root}Codes_Other"
log using mmc_rer.txt, text replace

*******************************************
* Calculate real exchange rates by country and year using Penn World Tables data

use ../Data_Other/pwt71, clear

keep isocode year xrat ppp
gen br_xrat_tmp = xrat if isocode == "BRA"
bysort year: egen br_xrat = mean(br_xrat_tmp) // Brazil xrat in the relevant year
drop br_xrat_tmp
gen br_ppp_tmp = ppp if isocode == "BRA"
bysort year: egen br_ppp = mean(br_ppp_tmp) // Brazil ppp in the relevant year
drop br_ppp_tmp

drop if isocode == "BRA"

gen rer = (br_xrat / br_ppp) / (xrat / ppp)
sum rer
gen ner = (br_xrat) / (xrat)
sum ner

sort isocode year
save country_year_rer, replace

*******************************************
* Use 1989 Comtrade trade data to calculate trade shares across industries
* for each country

use ../Data_Other/comtrade_partner_imports_1989.dta
merge 1:1 partnercode hs1992 using ../Data_Other/comtrade_partner_exports_1989
* unmatched codes imply zero trade flows
replace imports = 0 if imports >= .
replace exports = 0 if exports >= .
drop _merge

** Collapse trade data to Industry level
sort hs1992
merge m:1 hs1992 using ../Data_Other/hs1992_to_industry
tab hs1992 if _merge == 1 // should be none
keep if _merge == 3
drop _merge

* drop petroleum trade
drop if inlist(floor(hs1992/100),2709,2710,2711)

* collapse to industry x comtrade country code
collapse (sum) imports exports, by(partnercode industry)
drop if industry >= .

* Restrict to PWT countries 
sort partnercode
merge m:1 partnercode using ../Data_Other/country_concord
tab partnercode if _merge == 1 // countries in comtrade that don't appear in PWT
tab isocode if _merge == 2 // countries in PWT but not comtrade (including Brazil)
keep if _merge == 3
drop _merge
keep isocode industry imports exports
order isocode industry imports exports
drop if inlist(isocode,"BUR","CSK","REU") // drop 3 countries without exchange rates

* Calculate shares
gen trade = imports + exports
bysort industry: egen tottrade = sum(trade)
by industry: egen totimports = sum(imports)
by industry: egen totexports = sum(exports)
gen tradesh = trade / tottrade
replace tradesh = 0 if tradesh >= .
gen importsh = imports / totimports
replace importsh = 0 if importsh >= .
gen exportsh = exports / totexports
replace exportsh = 0 if exportsh >= .

keep isocode industry tradesh importsh exportsh
order isocode industry tradesh importsh exportsh
sort isocode industry 

*******************************************
* Industry-level real exchange rates as trade-weighted averages across trading
* partner countries

*  format weights to match exchange rates
expand 25 // RER data from PWT go through 2010
bysort isocode industry: gen year = _n + 1986-1

* merge in yearly RER's
merge m:1 isocode year using country_year_rer
list isocode year if _merge == 1 // should be none
keep if _merge == 3 // all weights match up with exchange rate info
drop _merge

* generate sum elements
gen rer_trade_element = tradesh * rer
gen rer_import_element = importsh * rer
gen rer_export_element = exportsh * rer
gen ner_trade_element = tradesh * ner
gen ner_import_element = importsh * ner
gen ner_export_element = exportsh * ner

* collapse to sum across destinations for industry x year results
collapse (sum) rer_trade_element rer_import_element rer_export_element ///
               ner_trade_element ner_import_element ner_export_element, ///
			   by(industry year)

foreach v1 in rer ner {
  foreach v2 in trade import export {
    rename `v1'_`v2'_element `v1'_`v2'
  }
}			   
		   
sort industry year
erase country_year_rer.dta

*******************************************
* Region-level real exchange rates as weighted averages across industries

* change in log exchange rates
reshape wide rer* ner*, i(industry) j(year)
forvalues t = 1991/2010 {
  foreach v1 in trade import export {
    foreach v2 in rer ner {
	  gen dln_`v2'_`v1'_`t' = ln(`v2'_`v1'`t') - ln(`v2'_`v1'1990)
	}
  }
}
keep industry dln*
sort industry
save dln_rer_industry, replace

* weights (all workers, traded, without theta adjustment)
use ../Data/beta_industry, clear
keep mmc industry beta_t_notheta

* combine rer shocks and weights
sort industry
merge m:1 industry using dln_rer_industry
keep if _merge == 3
erase dln_rer_industry.dta

* calculate weighted averages by mmc
forvalues t = 1991/2010 {
  foreach v1 in trade import export {
    foreach v2 in rer ner {
	  gen element_`v2'_`v1'_`t' = beta_t_notheta * dln_`v2'_`v1'_`t'
	}
  }
}
collapse (sum) element*, by(mmc)

forvalues t = 1991/2010 {
  foreach v1 in trade import export {
    foreach v2 in rer ner {
      rename element_`v2'_`v1'_`t' `v2'_`v1'_`t'
	}
  }
}
sort mmc
save ../Data/mmc_rer, replace


log close
cd "${root}"
