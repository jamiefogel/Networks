
version 14.2
clear all
set more off
set matsize 11000
unicode encoding set "latin1"
set seed 34317154

* Gtools work on server
if c(username)=="mfelix"{
	local tool "g"
}
else{
	local tool ""
}

else if c(username)=="p13861161" & c(os)=="Windows" {
	global encrypted 		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara"
	global dictionaries		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdictionaries\harmonized"
	global deIDrais			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\raisdeidentified"
	global monopsonies		"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\monopsonies"
	global public			"\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\replicate_mayara\publicdata"
}

else if c(username)=="p13861161" & c(os)=="Unix" {
	global encrypted 		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara"
	global dictionaries		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdictionaries/harmonized"
	global deIDrais			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/raisdeidentified"
	global monopsonies		"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies"
	global public			"/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/publicdata"
}

local outdate		= 20210802
local premiadate	= 20210802
local baseyear 		= 1991
local baseyear_n 	= 91

local lagyears		= 3
local baseyear_o1 	= `baseyear'+3
local baseyear_o2	= `baseyear'+6
local baseyear_p1	= `baseyear'-3
local baseyear_p2	= `baseyear'-5

local setup 			= 1
local eta_regs 			= 1
local usesample 		= 0			/* Use 10% random sample of firms*/


* Make folders with output date if they don't yet exist
cap mkdir "${monopsonies}/csv/`outdate'"
cap mkdir "${monopsonies}/eps/`outdate'"
cap mkdir "${monopsonies}/dta/coeffs/`outdate'"

/*
local tarvars 		"lnT"
local allspecs 		"l"
local wagevars 		"lndp"
local allmodels 	"l"			/* b: back to 1985; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"cnae95"
local allsamp 		"all up`baseyear_n'mkt"
*/

local allspecs 		"l"
local alltars 		"lnT"  //lnE
local allwages 		"lndp" //lndw
local allmodels 	"l"			/* b: back to 1985; l: long distance to 1991; s: 3-year short distnaces */
local allclust 		"firm " //  cnae95 fe_ro
local allsamp 		"all" //T up`baseyear_n'mkt up`baseyear_n' explib Tnexplib"

local mainwage 	"lndp"
local mainclust "firm" // XX cnae95
local maintar	"lnT"

* Specification FEs
local labsorb "fe_ro"			/* When spec is m, absorb mmc-cbo942d */


local path "3states_mmc_cbo942d"
u "${monopsonies}/sas/eta_changes_regsfile_`path'_keepyearsvars.dta"

isid year gamma fakeid_firm 

tab year
/*
       year |      Freq.     Percent        Cum.
------------+-----------------------------------
       1986 |    628,813       21.45       21.45
       1988 |    722,994       24.67       46.12
       1991 |    749,501       25.57       71.69
       1994 |    470,956       16.07       87.76
       1997 |    358,851       12.24      100.00
------------+-----------------------------------
      Total |  2,931,115      100.00
*/


bysort cbo942d mmc year: egen temp = total(emp)
gen diff = temp - bemp

* bemp is market employment in a given year. Note that bemp is not equal to the sum of emp within a gamma-year, however it is highly correlated. Created in rais_050
* emp is firm-gamma employment in a given year. Created in rais_040....py
* bwshare - firm’s share of total earnings in its market.
* beshare - firm’s share of total employment in its market.
* bTwshare - firm’s share of earnings within the subset of firms in the market that are considered “tradable.”
* bTeshare - firm’s share of employment within the subset of firms in the market that are considered “tradable.”
* T - dummy for being in tradable sector
* lndp - the wage var used in regressions. Firm-market earnings premium (FE). 
*		- It was renamed from dprem_zro in  "${monopsonies}/sas/rais_lnearn_premia_firm_`path'_`premiadate'.dta". 
*		- Created by reghdfe lndecearn female age2-age5 educ2-educ9 , absorb(dprem_zro=fe_zro) noconstant keepsingletons
* lndw - not used in main regressions.It was renamed from davgw_zro in  "${monopsonies}/sas/rais_lnearn_premia_firm_`path'_`premiadate'.dta"

gen firm_count = 1
collapse (sum) bemp firm_count (mean)
