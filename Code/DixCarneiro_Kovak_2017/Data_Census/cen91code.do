******************************************************************************
* cen91code.do
* Dix-Carneiro and Kovak AER replication files
*
* Extract relevant variables from the 1991 Census, give intuitive names, 
* code values as needed, and combine files across states to generate
* cen91.dta.
******************************************************************************

foreach st in 11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 36 41 42 43 50 51 52 53 {

  use ./Census_Raw/1991/cen91st`st'com, clear

  gen long hhid = v102
  gen pernum = v98

  gen xweighti = v7301 / (10^8)

  gen age = v3072

  gen female = (v301 == 2)

  gen race = .
  replace race = 1 if v309 == 1 // branca
  replace race = 2 if v309 == 4 // parda
  replace race = 3 if v309 == 2 // preta
  replace race = 4 if v309 == 3 // amarela
  replace race = 5 if v309 == 5 // indigena

  gen educ = .
  replace educ = 0 if inlist(v3241,0,30) // no instruction or adult literacy
  replace educ = v3241 if inrange(v3241,1,17) // years of ed
  replace educ = . if inlist(v3241,20,.) // undetermined or < 5 yrs old

  ** main employment section
  gen employed = 1 if inlist(v345,1,2) // worked regularly or occassionally in last year
  replace employed = 0 if employed!=1
                  // employed,    has work card    ag worker, employee (private, public, state enterprise)
  gen formemp = 1 if (employed==1) & (v350==1) & inlist(v349,1,6,7,8)
  replace formemp = 0 if formemp!=1
  gen nonformemp = 1 if (employed==1) & (formemp==0) // employed but not formally
  replace nonformemp = 0 if nonformemp!=1
                //  not employed   looking for work (either worked before or not)
  gen unemp = 1 if (employed!=1) & inlist(v358,1,2)
  replace unemp = 0 if unemp!=1
  gen nilf = 1 if (employed!=1) & (unemp!=1) // neither employed nor unemployed (i.e. not working but not searching)
  replace nilf = 0 if nilf!=1
  **

  ** alternate employment section
  gen anonformemp = 1 if (employed==1) & (formemp==0) // employed but not formally
  replace anonformemp = 0 if anonformemp!=1
  replace anonformemp = 0 if inlist(v349,9,11) // self employed, unpaid
  **
  
  ** previdencia-based employment section (for comparison to 1980)
                       // employed,       pays prev.  ag worker, employee (private, public, state enterprise)
  gen prev_formemp = 1 if (employed==1) & (v353==1) & inlist(v349,1,6,7,8)
  replace prev_formemp = 0 if prev_formemp!=1
  gen prev_nonformemp = 1 if (employed==1) & (prev_formemp==0) // employed but not formally
  replace prev_nonformemp = 0 if prev_nonformemp!=1
  **
  
  ** alternate previdencia-based employment section
  gen prev_anonformemp = 1 if (employed==1) & (prev_formemp==0) // employed but not formally
  replace prev_anonformemp = 0 if prev_anonformemp!=1
  replace prev_anonformemp = 0 if inlist(v349,9,11) // self employed, unpaid
  **
  
  * auxiliary variables for informality study
  gen selfemployed = (v349==9)
  replace selfemployed = . if v349 >= .
  gen workcard = (v350==1)
  replace workcard = . if v350 >= .

  gen atividade = v347

  gen ymain = v356 if v356 < 9999999
  replace ymain = . if inlist(v356,9999999,.) // undeclared, unemployed
  gen mw_ymain = ymain / 36161.60

  gen hmain = v354

  gen yotherjob = v357 if v357 < 9999999
  replace yotherjob = . if inlist(v357,9999999,.)
  gen yalljob = ymain + yotherjob
  drop yotherjob
  
  gen hotherjob = v355
  gen halljob = hmain + hotherjob
  drop hotherjob

  gen state = v1101

  gen mesoreg = state*100 + v7001

  gen microreg = state*1000 + v7002

  gen munic = state*10000 + v1102

  gen urbanrural = v1061

  gen married = inrange(v3342,1,3)
  
  gen munic5ya = v321*10000 + v3211 if inrange(v321,11,53) & inrange(v3211,1,7220) // valid state code and munic code
  replace munic5ya = munic if v321 == 70 // in this municipio
  replace munic5ya = munic if v314 == 1 // born in this municipio and always lived here (causes a skip over migration-related variables)
  * implicitly coded as missing if reporting brazil, unspecified v321 == 54 
  *                                foreign country v321 == 80
  *                                state ignored v321 == 99
  *                                municipio ignored or ill defined v3211 == 0
  *                                people under age 5 v3211 == .
  
  gen famsize = v3041 + v3042
  
  gen inschool = inrange(v324,1,8) | inrange(v325,1,5) | inrange(v326,1,6)
  
  keep hhid pernum xweighti age female race educ employed formemp nonformemp prev_formemp prev_nonformemp ///
       anonformemp prev_anonformemp unemp nilf atividade ymain hmain yalljob halljob state mesoreg microreg ///
	   munic urbanrural married munic5ya famsize inschool ///
	   selfemployed workcard mw_ymain
  compress
  save cen91code`st', replace
}

use cen91code11, clear
foreach st in 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 36 41 42 43 50 51 52 53 {
  append using cen91code`st'
}
gen year = 1991
compress
save cen91, replace

foreach st in 11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 36 41 42 43 50 51 52 53 {
  erase cen91code`st'.dta
}
