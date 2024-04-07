*******************************************************
* census_deflators.do
* Dix-Carneiro and Kovak AER replication files
*
* Generates price deflators for use with Census data
*
* Input: baseyr - global containing the survey year
*                 that will serve as the base year
*                 for the deflators.
*        svyyrvar - global containing the name of the
*                 variable defining the survey year
*                 for each observation.
*
* Output: defl - variable containing the appropriate
*                deflator for each observation.
*******************************************************

local defl1960 = 0.000000000000007
local defl1970 = 0.000000000000264
local defl1980 = 0.000000000005748
local defl1991 = 0.000067244146018
local defl2000 = 0.890629059684618
local defl2010 = 1.7385882995697 // calculated in inpc_ipeadata.xlsx

capture drop defl
gen double defl = .

foreach yr in 1960 1970 1980 1991 2000 2010 {
  replace defl = `defl`yr'' if $svyyrvar == `yr'
}

replace defl = defl / `defl$baseyr'
