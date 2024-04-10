******************************************************************************
* cen80code.do
* Dix-Carneiro and Kovak AER replication files
*
* Extract relevant variables from the 1980 Census, give intuitive names, 
* code values as needed, and combine files across states to generate
* cen80.dta.
******************************************************************************

foreach st in 11 12 13 14 15 16 21 22 23 24 25 26 27 28 29 31a 31b 32 33a 33b 35a 35b 35c 41 42 43 50 51 52 53 {

  use ./Census_Raw/1980/cen80st`st', clear

  gen hhid = v601
  bysort hhid: gen pernum = _n // no person number variable in survey

  gen xweighti = v604 // mean is 4.2 - corresponds to roughly 25% sample

  gen age = v606
  replace age = . if v606 == 999

  gen female = (v501 == 3)

  gen race = .
  replace race = 1 if v509 == 2 // branca
  replace race = 2 if v509 == 8 // parda
  replace race = 3 if v509 == 4 // preta
  replace race = 4 if v509 == 6 // amarela
  *** no code for indigena in 1980
  
  *** code IPUMS version of years of education based on available
  *** variables
  do cen80educ.do
  
  ** previdencia-based employment section (for comparison to 1991)
  gen employed = (v528==1)
  replace employed = . if v528 >= .
                        // employed,      pays prev.           ag worker(1,2), employee
  gen prev_formemp = 1 if (employed==1) & inlist(v534,2,4,6) & inlist(v533,1,2,6)
  replace prev_formemp = 0 if prev_formemp!=1
  gen prev_nonformemp = 1 if (employed==1) & (prev_formemp==0) // employed but not formally
  replace prev_nonformemp = 0 if prev_nonformemp!=1
                //  not employed   looking for work (either worked before or not)
  gen unemp = 1 if (employed!=1) & inlist(v529,1,2)
  replace unemp = 0 if unemp!=1
  gen nilf = 1 if (employed!=1) & (unemp!=1) // neither employed nor unemployed (i.e. not working but not searching)
  replace nilf = 0 if nilf!=1
  **  

  ** alternate employment section
  gen prev_anonformemp = 1 if (employed==1) & (prev_formemp==0) // employed but not formally
  replace prev_anonformemp = 0 if prev_anonformemp!=1
  replace prev_anonformemp = 0 if inlist(v533,8,9) // self employed, no answer
  **  

  gen atividade = v532 // same codes as 1991

  gen ymain = v607 if v607 < 9999999
  replace ymain = . if inlist(v607,9999999,.) // undeclared, unemployed
  gen mw_ymain = ymain / 4150 // main earnings in multiples of minimum wage
  
  gen hmain_bin = v535 // binned into <15, 15-29, 30-39, 40-48, >=49
  replace hmain_bin = . if v535 == 9
  
  gen yotherjob = v609 if v609 < 9999999
  replace yotherjob = . if inlist(v609,9999999,.) // undeclared, unemployed

  gen halljob_bin = v536

  gen state = v002
  gen mesoreg = state*100 + v003
  gen microreg = state*1000 + v004
  gen munic = state*10000 + v005

  gen urbanrural = v198 // city is coded as 1 (as in subsequent years)
  
  gen married = inrange(v526,1,3)
  
  * combine v518 (previous munic) with v517 (time in current munic)
  gen munic5ya = v518 if inrange(v516,0,4) // was in differenc munic 5 years ago
  replace munic5ya = munic if inrange(v516,5,8) // was still in current munic 5 years ago
  replace munic5ya = . if v516 == 9
  
  * calculate family size using household id and family indicator (v505)
  * v505 coding:
  * - multiple household members, all in same family: 0
  * - household contains only one individual: 5
  * - household contains multiple individuals, belonging to multiple
  *   families: 1, 2, or 3 indicating which family
  * - group quarters: 4
  bysort v601 v505: egen famsize = count(v505) if inrange(v505,0,3)
  replace famsize = 1 if inrange(v505,4,5)
  
  gen inschool = inrange(v520,1,8) | inrange(v521,1,8) | inrange(v522,1,8)
  
  keep hhid pernum xweighti age female race educ employed prev_formemp prev_nonformemp ///
       prev_anonformemp unemp nilf atividade ///
       ymain hmain_bin yotherjob halljob_bin state mesoreg microreg munic urbanrural married ///
	   munic5ya famsize inschool mw_ymain
  compress
  save cen80code`st', replace
}

use cen80code11, clear
foreach st in 12 13 14 15 16 21 22 23 24 25 26 27 28 29 31a 31b 32 33a 33b 35a 35b 35c 41 42 43 50 51 52 53 {
  append using cen80code`st'
}
gen year = 1980
compress
save cen80, replace

foreach st in 11 12 13 14 15 16 21 22 23 24 25 26 27 28 29 31a 31b 32 33a 33b 35a 35b 35c 41 42 43 50 51 52 53 {
  erase cen80code`st'.dta
}
