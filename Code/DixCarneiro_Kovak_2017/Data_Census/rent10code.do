******************************************************************************
* rent10code.do
* Dix-Carneiro and Kovak AER replication files
*
* Extract relevant variables from the 2010 Census for Moretti (2013) price
* index analysis.
******************************************************************************

*************************
* Get household only extract from 2010 Census

foreach st in AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {

  use ./Census_Raw/2010/CENSO10_`st', clear

  * keep only household variables
  keep v0001 v0002 v0011 v0300 v0010 v1001 v1002 v1003 v1004 v1006 v4001 ///
       v4002 v0201 v2011 v2012 v0202 v0203 v6203 v0204 v6204 v0205 v0206 /// 
	   v0207 v0208 v0209 v0210 v0211 v0212 v0213 v0214 v0215 v0216 v0217 ///
	   v0218 v0219 v0220 v0221 v0222 v0301 v0401 v0402 v0701 v6529 v6530 ///
	   v6531 v6532 v6600 v6210 
  duplicates drop

  gen year = 2010
  gen xweighth = v0010
  
  * restrict sample to renters in permanent structures
  keep if v0201==3 // renting
  keep if v4001==1 // permanent structure

  * geography
  gen state = v0001
  destring v1002 v1003 v0002, replace
  gen mesoreg = 100*v0001 + v1002
  gen microreg = 1000*v0001 + v1003
  gen munic = 10000*v0001 + floor(v0002/10) // drop checksum digit

  * monthly rent (nominal)
  gen rent = v2011
  
  * features
  gen urban = (v1006==1)
  
  gen walls = 1 if inlist(v0202,1,2) // masonry (alvenaria)
  replace walls = 2 if v0202==3 // timber (madeira apraelhada)
  replace walls = 3 if v0202==5 // bare mud (taipa n‹o revestida)
  replace walls = 4 if v0202==6 // improvised materials (material aproveitado)
  replace walls = 5 if v0202==7 // straw (palha)
  replace walls = 5 if inlist(v0202,4,8) // other
  
  gen rooms = v0203 // number of rooms
  gen bedrooms = v0204 // number of bedrooms
  gen bathrooms = v0205 // number of bathrooms

  gen sewer = 0 if v0206==2 // none
  replace sewer = 1 if v0207==1 // modern sewer system
  replace sewer = 2 if v0207==2 // septic tank
  replace sewer = 3 if v0207==3 // rustic pit
  replace sewer = 4 if v0207==4 // trench
  replace sewer = 5 if inlist(v0207,5,6) // river,lake,sea, other
  
  gen public_water = v0208==1
  
  keep year xweighth state mesoreg microreg munic rent urban walls ///
       rooms bedrooms bathrooms sewer public_water
  compress
  save rent10code`st', replace
}

use rent10codeAC, clear
foreach st in AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {
  append using rent10code`st'
}
compress
save rent10, replace


foreach st in AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO {
  erase rent10code`st'.dta
}


