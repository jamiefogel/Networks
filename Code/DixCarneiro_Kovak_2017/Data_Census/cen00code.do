******************************************************************************
* cen00code.do
* Dix-Carneiro and Kovak AER replication files
*
* Extract relevant variables from the 2000 Census, give intuitive names, 
* code values as needed, and combine files across states to generate
* cen00.dta.
******************************************************************************

foreach st in 11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 41 42 43 50 51 52 53 {

  use ./Census_Raw/2000/cen00st`st'com, clear

  gen hhid = v0300
  gen pernum = v0400

  gen xweighti = p001

  gen age = v4752

  gen female = (v0401 == 2)

  gen race = .
  replace race = 1 if v0408 == 1 // branca
  replace race = 2 if v0408 == 4 // parda
  replace race = 3 if v0408 == 2 // preta
  replace race = 4 if v0408 == 3 // amarela
  replace race = 5 if v0408 == 5 // indigena

  gen educ = .
  replace educ = 0 if inlist(v4300,0,30) // no instruction or adult literacy
  replace educ = v4300 if inrange(v4300,1,17) // years of ed
  replace educ = . if inlist(v4300,20) // undetermined

  ** employment section
  gen employed = 1 if v0439==1 | v0440==1 | v0441==1 | v0442==1 | v0443==1
  replace employed = 0 if employed!=1

  gen formemp = 1 if (employed==1) & (v0447==3) // employed and employee (not domestic) with work card
  replace formemp = 0 if formemp!=1

  gen nonformemp = 1 if (employed==1) & (formemp==0) // employed but not formally
  replace nonformemp = 0 if nonformemp!=1

                //  not employed   looking for work
  gen unemp = 1 if (employed!=1) & v0455==1
  replace unemp = 0 if unemp!=1

  gen nilf = 1 if (employed!=1) & (unemp!=1) // neither employed nor unemployed (i.e. not working but not searching)
  replace nilf = 0 if nilf!=1
  **  

  ** alternate employment section
  gen anonformemp = 1 if (employed==1) & (formemp==0) // employed but not formally
  replace anonformemp = 0 if anonformemp!=1
  replace anonformemp = 0 if inlist(v0447,6,7,8,9) // self employed, apprentice, unpaid, own consump
  **  
  
  gen cnae = v4462

  gen ymain = v4512
  gen mw_ymain = v4514 if ymain > 0 & ymain < .

  gen hmain = v0453

  gen yotherjob = v4522
  gen yalljob = ymain + yotherjob
  drop yotherjob
  
  gen hotherjob = v0454
  gen halljob = hmain + hotherjob
  drop hotherjob

  gen state = v0102

  gen mesoreg = v1002

  gen microreg = v1003

  gen munic = floor(v0103/10) // drop checksum digit

  gen urbanrural = v1005

  gen married = v0438 == 1

  gen munic5ya = floor(v4250/10) if inrange(v0424,3,4) // lived in a different municipio
  replace munic5ya = . if (floor(v4250/10) - 10000*floor(v4250/100000)) == 0 // unspecified municipio
  replace munic5ya = munic if inrange(v0424,1,2) // lived in this municipio 5 yrs ago
  replace munic5ya = munic if v0415 == 1 // always lived in this municipio (causes a skip over migration-related variables)
  * implicitly coded as missing if foreign country or not born yet v0424 = 5,6
  
  * living outside state of birth
  gen leftbirthst = .
  replace leftbirthst = 0 if v0415 == 1 // never left birth municipio
  replace leftbirthst = 0 if v0417 == 1 // lives in birth municipio (even if left at some point)
  replace leftbirthst = 0 if v0418 == 1 // born in current state
  replace leftbirthst = 1 if leftbirthst != 0
  replace leftbirthst = . if inlist(v0419,2,3) // omit non-native born
  
  * make famsize variable
  * - v7401-v7406 tells the number of people in each family within a household
  * - v7407-v7409 are all blank, since no family number is > 6
  * - v0404 family number, tells which v740x to look at for that individual
  gen famsize = .
  replace famsize = v7401 if v0404==1
  replace famsize = v7402 if v0404==2
  replace famsize = v7403 if v0404==3
  replace famsize = v7404 if v0404==4
  replace famsize = v7405 if v0404==5
  replace famsize = v7406 if v0404==6
  
  gen inschool = inrange(v0429,1,2)
  
  * commuting destination municipio
  gen commutemunic = floor(v4276/10) // drop checksum digit.
  replace commutemunic = . if mod(commutemunic,10000)==0 // if only report state set commute munic to missing
  replace commutemunic = . if v4276==0200006 // no work or school to commute to
  replace commutemunic = munic if v4276==0100008 // works in this municipio
  replace commutemunic = .a if floor(v4276/100000) >= 80 // works in other country
  
  keep hhid pernum xweighti age female race educ employed formemp nonformemp ///
       anonformemp unemp nilf cnae ymain hmain yalljob halljob state mesoreg microreg ///
	   munic urbanrural married munic5ya famsize inschool commutemunic leftbirthst ///
	   mw_ymain
  compress
  save cen00code`st', replace
}

use cen00code11, clear
foreach st in 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 41 42 43 50 51 52 53 {
  append using cen00code`st'
}
gen year = 2000
compress
save cen00, replace


foreach st in 11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 41 42 43 50 51 52 53 {
  erase cen00code`st'.dta
}
