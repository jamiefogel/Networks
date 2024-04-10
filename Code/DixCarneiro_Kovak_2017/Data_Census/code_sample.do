******************************************************************************
* code_sample_1970.do
* Dix-Carneiro and Kovak AER replication files
*
* Generate Census 1991, 2000, and 2010 variables for analysis and restrict
* sample
******************************************************************************

***************************
* Read in data and restrict sample

use cen91, clear
append using cen00
append using cen10
compress

***************************
* Sample restriction

keep if inrange(age,18,64)

***************************
* Calculate wages and deflate

* Calculate wages (generates wageall and wagemain)
gen wageall = (yalljob/4.33)/halljob
gen wagemain = (ymain/4.33)/hmain

* Common currency and deflate prices (generates currency and defl)
global baseyr = 2000
global svyyrvar = "year"
do census_deflators
do census_currency
gen rwageall = (wageall/currency)/defl
gen rwagemain = (wagemain/currency)/defl
gen ryalljob = (yalljob/currency)/defl
gen rymain = (ymain/currency)/defl

***************************
* Recode industries to IndMatch (Kovak AER 2013 definition)

* recode 1991 industries to IndMatch
sort atividade
merge m:1 atividade using ./Auxiliary_Files/pnad_to_indmatch
tab _merge
tab atividade if _merge == 1, m
tab atividade if _merge == 2, m  // nonsense atividade code 30 (one obs)
drop _merge
rename indmatch indmatch91

* recode 2000 and 2010 industries to IndMatch
rename cnae cnaedom
sort cnaedom
merge m:1 cnaedom using ./Auxiliary_Files/cnaedom_to_indmatch
tab cnaedom if _merge==1 & year==2000, m 
tab cnaedom if _merge==2 & year==2000, m
tab cnaedom if _merge==1 & year==2010, m
tab cnaedom if _merge==2 & year==2010, m
drop _merge
replace indmatch = indmatch91 if year == 1991
drop indmatch91

***************************
* Recode industries to IndLink (consistent definition for RAIS and Census)

sort indmatch
merge m:1 indmatch using ./Auxiliary_Files/indmatch_to_indlink
tab indmatch if _merge < 3, m
drop _merge // perfect match (only missing values)

***************************
* Recode industries to IndLinkN (includes nontraded subindustries)

* recode 1991 industries to indlinkn
merge m:1 atividade using ./Auxiliary_Files/atividade_to_indlinkn
tab _merge
tab atividade if _merge == 1, m
tab atividade if _merge == 2, m // nonsense atividade code 30 (one obs)
drop _merge
rename indlinkn indlinkn91

* recode 2000 and 2010 industries to indlinkn
merge m:1 cnaedom using ./Auxiliary_Files/cnaedom_to_indlinkn
tab cnaedom if _merge==1 & year==2000, m
tab cnaedom if _merge==2 & year==2000, m // codes without specific industries 
tab cnaedom if _merge==1 & year==2010, m
tab cnaedom if _merge==2 & year==2010, m  // codes without specific industries
drop _merge
replace indlinkn = indlinkn91 if year == 1991
drop indlinkn91

***************************
* Recode industries to Industry (new definition)

* recode 1991 industries to Industry
sort atividade
merge m:1 atividade using ./Auxiliary_Files/atividade_to_industry
tab _merge
tab atividade if _merge == 1, m // nonsense atividade code 30 (one obs)
tab atividade if _merge == 2, m  
drop _merge
rename industry industry91

* recode 2000 and 2010 industries to Industry
sort cnaedom
merge m:1 cnaedom using ./Auxiliary_Files/cnaedom_to_industry
tab cnaedom if _merge==1 & year==2000, m
tab cnaedom if _merge==2 & year==2000, m
tab cnaedom if _merge==1 & year==2010, m 
tab cnaedom if _merge==2 & year==2010, m
drop _merge
replace industry = industry91 if year == 1991
drop industry91

***************************
* Generate regression variables

gen lnrwmain = ln(rwagemain)   // log real wage in main job
gen agesq = age^2 / 1000       // age squared
gen agegroup = 1 if inrange(age,18,24) // age groups
replace agegroup = 2 if inrange(age,25,29)
replace agegroup = 3 if inrange(age,30,39)
replace agegroup = 4 if inrange(age,40,49)
replace agegroup = 5 if inrange(age,50,64)
tab agegroup, gen(ageflag) // age bin dummies
tab educ, gen(edflag) // education dummies
tab indmatch, m gen(indmatchflag) // Kovak (2013) industry dummies
tab indlink, m gen(indlinkflag) // consistent definition for RAIS and Census
tab industry, m gen(industryflag) // new industry dummies
tab indlinkn, m gen(indlinknflag) // breaks out nontradables into separate industries
gen city = (urbanrural == 1) | (urbanflag == 1)
tab race, gen(raceflag) // race dummies

***************************
* merge in MMC region indicators

* 1991-2010 consistent versions
sort munic
merge m:1 munic using ./Auxiliary_Files/census_1991_munic_to_mmc_1991_2010
tab _merge if year == 1991
tab munic if _merge < 3 & year == 1991
drop _merge
rename mmc mmc91
merge m:1 munic using ./Auxiliary_Files/census_2000_munic_to_mmc_1991_2010
tab _merge if year == 2000
tab munic if _merge < 3 & year == 2000
drop _merge
rename mmc mmc00
merge m:1 munic using ./Auxiliary_Files/census_2010_munic_to_mmc_1991_2010
tab _merge if year == 2010
tab munic if _merge < 3 & year == 2010
drop _merge
rename mmc mmc10

gen mmc = mmc91 if year == 1991
replace mmc = mmc00 if year == 2000
replace mmc = mmc10 if year == 2010
drop mmc91 mmc00 mmc10

* 1970-2010 consistent versions
rename mmc mmc_1991_2010
sort munic

merge m:1 munic using ./Auxiliary_Files/census_1991_munic_to_mmc_1970_2010
tab _merge if year == 1991
tab munic if _merge < 3 & year == 1991
drop _merge
rename mmc mmc91
merge m:1 munic using ./Auxiliary_Files/census_2000_munic_to_mmc_1970_2010
tab _merge if year == 2000
tab munic if _merge < 3 & year == 2000
drop _merge
rename mmc mmc00
merge m:1 munic using ./Auxiliary_Files/census_2010_munic_to_mmc_1970_2010
tab _merge if year == 2010
tab munic if _merge < 3 & year == 2010
drop _merge
rename mmc mmc10

gen mmc1970 = mmc91 if year == 1991
replace mmc1970 = mmc00 if year == 2000
replace mmc1970 = mmc10 if year == 2010
drop mmc91 mmc00 mmc10

rename mmc_1991_2010 mmc

***************************
* internal migration variable

* get current geography variables out of the way
rename munic munic_current 
rename mmc mmc_current 

* merge in mmc definitions for prior locaiton
rename munic5ya munic // for merge
sort munic
merge m:1 munic using ./Auxiliary_Files/census_1991_munic_to_mmc_1991_2010
tab _merge if year == 1991
tab munic if _merge < 3 & year == 1991 // no observations
drop _merge
rename mmc mmc91
merge m:1 munic using ./Auxiliary_Files/census_2000_munic_to_mmc_1991_2010
tab _merge if year == 2000
tab munic if _merge < 3 & year == 2000 // no observations
drop _merge
rename mmc mmc00
merge m:1 munic using ./Auxiliary_Files/census_2010_munic_to_mmc_1991_2010
tab _merge if year == 2010
tab munic if _merge < 3 & year == 2010 // only obs with unknown locations
drop _merge
rename mmc mmc10

* name prior location variables appropriately
gen mmc5ya = mmc91 if year == 1991
replace mmc5ya = mmc00 if year == 2000
replace mmc5ya = mmc10 if year == 2010
drop mmc91 mmc00 mmc10
rename munic munic5ya

* bring back current geography variables
rename munic_current munic
rename mmc_current mmc

***************************
* save data

save code_sample, replace
