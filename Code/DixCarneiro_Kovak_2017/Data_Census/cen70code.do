******************************************************************************
* cen70code.do
* Dix-Carneiro and Kovak AER replication files
*
* Extract relevant variables from the 1970 Census, give intuitive names, 
* code values as needed, and combine files across states to generate
* cen70.dta.
******************************************************************************

foreach st in AC AL AM AP BA CE DF ES FN GB GO MA MG MT PA PB PE PI PR RJ ///
              RN RO RR RS SC SE SP {

  use ./Census_Raw/1970/CENSO70_`st', clear

  gen xweighti = v054

  gen age = v027 if inlist(v026,3,4)
  replace age = 0 if inlist(v026,1,2)

  gen female = (v023==1)

  * no race variable in 1970

  *** code IPUMS version of years of education based on available
  *** variables
  do cen70educ.do

  ** employment
  gen employed = (v043==7)  // includes looking for work
  replace employed = . if v043 >= .
  
  * no information on formality (either via carteira or previdencia)
  
  gen atividade1970 = v045 // same classification as 1991 but different codes
  
  gen ymain = v041 if v041 < 9999
  
  gen state = UF

  * cod70 has municipio code

  gen urbanrural = 1 if v004==0
  replace urbanrural = 0 if inlist(v004,1,2)
  
  gen married = inrange(v040,1,3)
  
  gen othermunic5ya = 1 if inrange(v032,1,5)
  replace othermunic5ya = 0 if inrange(v032,6,8)

  bysort id_dom num_fam: egen famsize = count(num_fam)
  
  gen inschool = (v043==1)
  
  keep xweighti age female educ employed atividade1970 ymain state ///
       urbanrural married othermunic5ya famsize inschool cod70
  
  compress
  save cen70code`st', replace
}

use cen70codeAC, clear
foreach st in AL AM AP BA CE DF ES FN GB GO MA MG MT PA PB PE PI PR RJ ///
              RN RO RR RS SC SE SP {
  append using cen70code`st'
}
gen year = 1970
compress
save cen70, replace

foreach st in AC AL AM AP BA CE DF ES FN GB GO MA MG MT PA PB PE PI PR RJ ///
              RN RO RR RS SC SE SP {
  erase cen70code`st'.dta
}
