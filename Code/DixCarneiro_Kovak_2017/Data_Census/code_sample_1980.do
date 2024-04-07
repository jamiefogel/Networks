******************************************************************************
* code_sample_1970.do
* Dix-Carneiro and Kovak AER replication files
*
* Generate Census 1970 variables for analysis and restrict sample
******************************************************************************

***************************
* Read in data and restrict sample
use cen80, clear

***************************
* Sample restriction
keep if inrange(age,18,64)

***************************
* Calculate wages and deflate

* Common currency and deflate prices (generates currency and defl)
global baseyr = 2000
global svyyrvar = "year"
do census_deflators
do census_currency
* Can't calculate hourly wage in 1980 due to hour bins rather than precise hours
*gen rwageall = (wageall/currency)/defl
*gen rwagemain = (wagemain/currency)/defl
*gen ryalljob = (yalljob/currency)/defl
gen rymain = (ymain/currency)/defl

***************************
* Recode industries to IndMatch (Kovak AER 2013 definition)

* recode 1991 industries to IndMatch
sort atividade
merge m:1 atividade using ./Auxiliary_Files/pnad_to_indmatch
tab _merge
tab atividade if _merge == 1, m
tab atividade if _merge == 2, m
drop _merge

***************************
* Recode industries to IndLink (consistent definition for RAIS and Census)

sort indmatch
merge m:1 indmatch using ./Auxiliary_Files/indmatch_to_indlink
tab indmatch if _merge < 3
drop _merge // perfect match (only missing values)

***************************
* Recode industries to IndLinkN (includes nontraded subindustries)

* recode industries to indlinkn
merge m:1 atividade using ./Auxiliary_Files/atividade_to_indlinkn
drop _merge

***************************
* Recode industries to Industry (new definition)

* recode 1991 industries to Industry
sort atividade
merge m:1 atividade using ./Auxiliary_Files/atividade_to_industry
tab _merge
tab atividade if _merge == 1, m // nonsense atividade code 30 (one obs)
tab atividade if _merge == 2, m  
drop _merge

***************************
* Generate regression variables

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
gen city = (urbanrural == 1)
tab race, gen(raceflag) // race dummies

***************************
* merge in MMC region indicators

* only relevant version is 1970-2010 consistent MMC's
sort munic
merge m:1 munic using ./Auxiliary_Files/census_1980_munic_to_mmc_1970_2010
tab munic if _merge < 3
drop _merge
rename mmc mmc1970

* merge in time-consistent mesoregs, built from 1980 municipios
drop mesoreg // this is the obsolete 1980 version
rename munic munic1980
sort munic1980
merge m:1 munic1980 using ./Auxiliary_Files/census_munic1980_to_munic1991
tab munic1980 if _merge < 3
drop _merge // perfect match
drop munic1980
rename munic1991 munic
sort munic
merge m:1 munic using ./Auxiliary_Files/census_1991_munic_to_c_mesoreg
tab munic if _merge ==1
drop _merge // perfect match for available codes in 1980

***************************
* internal migration variable

* get current geography variables out of the way
rename munic munic_current 
rename mmc1970 mmc_current 

* merge in mmc definitions for prior locaiton
rename munic5ya munic // for merge
sort munic
merge m:1 munic using ./Auxiliary_Files/census_1980_munic_to_mmc_1970_2010
tab munic if _merge < 3
drop _merge
rename mmc mmc19705ya
rename munic munic5ya

* bring back current geography variables
rename munic_current munic
rename mmc_current mmc1970

save code_sample_1980, replace

