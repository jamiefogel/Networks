******************************************************************************
* dlnrent.do
* Dix-Carneiro and Kovak AER replication files
*
* Calculates alculates regional commodity price shocks using data from the IMF Primary
* Commodity Price Series.
*
* Output: /Data/dlnrent.dta
******************************************************************************

cd "${root}Codes_Other"
log using rcs.txt, text replace


use ../Data_Census/rent91, clear
append using ../Data_Census/rent10

*********************
* restrict sample

* in both 1991 and 2010, 1 and 2 bedroom rental units account for more than
* 85% of the total stock
keep if inrange(bedrooms,1,2)
keep if inrange(walls,1,2) // masonry or wood framing
keep if bathrooms > 0
keep if inlist(sewer,1,2) // sewer or septic tank

*********************
* deflate and standardize currency

global baseyr = 2000
global svyyrvar = "year"
do ../Data_Census/census_deflators
do ../Data_Census/census_currency
gen real_rent = (rent/currency)/defl
bysort year: sum real_rent [aw=xweighth]

*********************
* merge in MMC geography

sort munic
merge m:1 munic using ../Data_Census/Auxiliary_Files/census_1991_munic_to_mmc_1991_2010
tab _merge if year == 1991
tab munic if _merge < 3 & year == 1991
drop _merge
rename mmc mmc91
merge m:1 munic using ../Data_Census/Auxiliary_Files/census_2010_munic_to_mmc_1991_2010
tab _merge if year == 2010
tab munic if _merge < 3 & year == 2010
drop _merge
rename mmc mmc10

gen mmc = mmc91 if year == 1991
replace mmc = mmc10 if year == 2010
drop mmc91 mmc10

*********************
* collapse and calculate changes

collapse (mean) real_rent [pw=xweighth], by(mmc year)
gen ln_real_rent = ln(real_rent)
keep ln_real_rent mmc year
reshape wide ln_real_rent, i(mmc) j(year)
gen dln_real_rent = ln_real_rent2010 - ln_real_rent1991
sum dln_real_rent, det

sort mmc
save ../Data/dlnrent, replace


log close
cd "${root}"
