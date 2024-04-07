******************************************************************************
* cen10code.do
* Dix-Carneiro and Kovak AER replication files
*
* Extract relevant variables from the 2010 Census, give intuitive names, 
* code values as needed, and combine files across states to generate
* cen10.dta.
******************************************************************************

foreach st in AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {
  use ./Census_Raw/2010/CENSO10_`st', clear

  gen hhid = v0300
  destring hhid, replace
  
  gen pernum = v0504
  
  gen xweighti = v0010

  gen age = v6036

  gen female = (v0601 == 2)

  gen race = .
  replace race = 1 if v0606 == 1 // branca
  replace race = 2 if v0606 == 4 // parda
  replace race = 3 if v0606 == 2 // preta
  replace race = 4 if v0606 == 3 // amarela
  replace race = 5 if v0606 == 5 // indigena

  do educ_censo_2010 // Years of education code from Guilherme Hirata (PUC-Rio)
  rename anoest educ

  ** employment section
  
  gen employed = 1 if v0641==1 | v0642==1 | v0643==1 | v0644==1
  replace employed = 0 if employed!=1

  gen formemp = 1 if (employed==1) & inlist(v0648,1,2,3) // employed and (employee with card, military, or civil svc.)
  replace formemp = 0 if v0502==18 | inlist(v6461,5152,9111) // domestic workers (from household register, then occupations)
  replace formemp = 0 if formemp!=1

  gen nonformemp = 1 if (employed==1) & (formemp==0) // employed but not formally
  replace nonformemp = 0 if nonformemp!=1
  
                //  not employed   looking for work
  gen unemp = 1 if (employed!=1) & v0654==1
  replace unemp = 0 if unemp!=1

  gen nilf = 1 if (employed!=1) & (unemp!=1) // neither employed nor unemployed (i.e. not working but not searching)
  replace nilf = 0 if nilf!=1
  **  
  
  ** alternate employment section
  gen anonformemp = 1 if (employed==1) & (formemp==0) // employed but not formally
  replace anonformemp = 0 if anonformemp!=1  
  replace anonformemp = 0 if inlist(v0648,5,7) // self employed, unpaid
  **
  
  gen cnae = v6472 // retroactively compatible with Census 2000 (CNAE-Dom v1)
  
  gen ymain = v6511
  gen mw_ymain = v6514 if ymain > 0 & ymain < .

  gen hmain = v0653
  
  gen yotherjob = v6521
  gen yalljob = ymain + yotherjob
  drop yotherjob
  
  gen hotherjob = . // no longer asked in 2010 Census
  gen halljob = . // missing other job hours

  gen state = v0001
  
  destring v1002 v1003 v0002, replace
  
  gen mesoreg = 100*v0001 + v1002
  
  gen microreg = 1000*v0001 + v1003
  
  gen munic = 10000*v0001 + floor(v0002/10) // drop checksum digit
  
  gen urbanflag = (v1006==1)
  
  gen married = (v0640==1)
  
  gen munic5ya = floor(v6264/10) if v0626==1 // lived elsewhere in Brazil 5 years ago
  replace munic5ya = munic if v0624>=6 // lived here for at least 6 years (causes a skip for the munic 5ya question)
  replace munic5ya = munic if v0618==1 // always lived here (causes a skip over migration-related variables)
  * implicitly coded as missing if foreign country or not born yet
  
  gen famsize = .
  replace famsize = v5060 if v5020 < . // gives direct family size measure in v5060
  
  gen inschool = inrange(v0628,1,2)
  
  * commuting
  * v0660, v0662, and v0664
  gen commutemunic = munic if inrange(v0660,1,2) // work in house or same munic
  replace commutemunic = floor(v6604/10) if v0660==3 // work in other munic
  replace commutemunic = . if inlist(v6604,8888888,9999999) & v0660==3 // munic not specified
  replace commutemunic = .a if v0660==4 // other country
  replace commutemunic = . if v0660==5 // multiple work locations, they don't ask munic
  
  keep hhid pernum xweighti age female race educ employed formemp nonformemp ///
       anonformemp unemp nilf cnae ymain hmain yalljob halljob state mesoreg microreg ///
	   munic urbanflag married munic5ya famsize inschool commutemunic mw_ymain
  compress
  save cen10code`st', replace  
}

use cen10codeAC, clear
foreach st in AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {
  append using cen10code`st'
}
gen year = 2010
compress
save cen10, replace


foreach st in AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {
  erase cen10code`st'.dta
}
