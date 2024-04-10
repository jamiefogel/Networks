******************************************************************************
* rent91code.do
* Dix-Carneiro and Kovak AER replication files
*
* Extract relevant variables from the 1991 Census for Moretti (2013) price
* index analysis.
******************************************************************************

foreach st in 11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 36 41 42 43 50 51 52 53 {

  use ./Census_Raw/1991/cen91st`st', clear

  gen year = 1991
  keep if v99==1 // household records only
  gen long hhid = v102 // household ID
  gen xweighth = v7300 / (10^8) // household weight

  * restrict sample to renters in permanent structures
  keep if v208==3 // renting
  keep if v201==1 // permanent structure
  
  * geography
  gen state = v1101
  gen mesoreg = state*100 + v7001
  gen microreg = state*1000 + v7002
  gen munic = state*10000 + v1102
  
  * monthly rent (nominal)
  gen rent = v209
  replace rent = . if inlist(rent,0,999999)

  * features
  gen urban = inlist(v1061,1,2,3)
  
  gen walls = 1 if v203==1 // masonry (alvenaria)
  replace walls = 2 if v203==2 // timber (madeira aparelhada)
  replace walls = 3 if v203==3 // bare mud (taipa n‹o revestida)
  replace walls = 4 if v203==4 // improvised materials (material aproveitado)
  replace walls = 5 if v203==5 // straw (palha)
  replace walls = 6 if v203==6 // other
  
  gen rooms = v211 // number of rooms
  gen bedrooms = v212 // number of bedrooms
  gen bathrooms = v213 // number of bathrooms
  
  gen sewer = 0 if v206==0 // none
  replace sewer = 1 if v206==1 // modern sewer system
  replace sewer = 2 if inlist(v206,2,3) // septic tank (connected to sewer or not)
  replace sewer = 3 if v206==4 // rustic pit
  replace sewer = 4 if v206==5 // trench
  replace sewer = 5 if v206==6 // other 

  gen public_water = inlist(v205,1,4)
  
  keep year hhid xweighth state mesoreg microreg munic rent urban walls ///
       rooms bedrooms bathrooms sewer public_water
  compress
  save rent91code`st', replace
}

use rent91code11, clear
foreach st in 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 36 41 42 43 50 51 52 53 {
  append using rent91code`st'
}
compress
save rent91, replace

foreach st in 11 12 13 14 15 16 17 21 22 23 24 25 26 27 28 29 31 32 33 35 36 41 42 43 50 51 52 53 {
  erase rent91code`st'.dta
}

