/*
	Mayara Felix
	
	Split files for 2003-2008 into state files. They are too large
*/

clear all
set more off
unicode encoding set "latin1"
set seed 34317154

* Gtools work on server
if c(username)=="mfelix"{
	local tool "g"
}
else{
	local tool ""
}

* Mayara mounting on server
if c(username)=="mfelix"{
	global encrypted 		"/proj/patkin/raismount_mfelix"
	global dictionaries		"/proj/patkin/raisdictionaries/harmonized"
	global deIDrais			"/proj/patkin/raisdeidentified"
	global monopsonies		"/proj/patkin/projects/monopsonies"
	global public			"/proj/patkin/publicdata"
}
* Mayara mounting locally on Mac
else if c(username)=="mayara"{
	global encrypted 		"/Volumes/raismount_mfelix"
	global dictionaries		"/Volumes/proj_atkin/raisdictionaries/harmonized"
	global deIDrais			"/Volumes/proj_atkin/raisdeidentified"
	global monopsonies		"/Volumes/proj_atkin/projects/monopsonies"
	global public			"/Volumes/proj_atkin/publicdata"
	
}
* Mayara mounting locally on Windows
else if c(username)=="Mayara"{
	global encrypted 		"Z:"
	global dictionaries		"M:/raisdictionaries/harmonized"
	global deIDrais			"M:/raisdeidentified"
	global monopsonies		"M:/projects/monopsonies"
	global public			"M:/publicdata"
}

* Change directory to encrypted output folder just in case something is saved
* to current directory
cd "${encrypted}/output"

* Output folder for rais .dta files
global importedrais			"${encrypted}/output/dta/importedrais"
global IDlinksrais			"${encrypted}/output/dta/IDlinksrais"

local updatedate		= 20191213

* Region-specific files
local f_northeast	"Nordeste2003		nordeste	nordeste05	nordeste06	nordeste07"
local f_north		"Norte2003			Norte	norte05	norte06	norte07"
local f_centro		"Centro_Oeste2003	centro_oeste	centro05	centro06 centooeste07"
local f_south		"Sul2003			sul	sul05	sul06	sul07"
local f_southeast	"ES_RJ_MG2003		sudeste	sudeste05	sudeste06	sudeste07"

* State numbers needed to split region files into states
local s11 "RO"
local s12 "AC"
local s13 "AM"
local s14 "RR"
local s15 "PA"
local s16 "AP"
local s17 "TO"
local s21 "MA"
local s22 "PI"
local s23 "CE"
local s24 "RN"
local s25 "PB"
local s26 "PE"
local s27 "AL"
local s28 "SE"
local s29 "BA"
local s31 "MG"
local s32 "ES"
local s33 "RJ"
local s35 "SP"
local s41 "PR"
local s42 "SC"
local s43 "RS"
local s50 "MS"
local s51 "MT"
local s52 "GO"
local s53 "DF"

local yNordeste2003 	= 2003
local ynordeste 		= 2004
local ynordeste05 		= 2005
local ynordeste06 		= 2006
local ynordeste07 		= 2007

local yNorte2003 		= 2003
local yNorte 			= 2004
local ynorte05 			= 2005
local ynorte06 			= 2006
local ynorte07 			= 2007

local yCentro_Oeste2003 	= 2003
local ycentro_oeste 		= 2004
local ycentro05 			= 2005
local ycentro06 			= 2006
local ycentooeste07 		= 2007

local ySul2003 				= 2003
local ysul 					= 2004
local ysul05 				= 2005
local ysul06 				= 2006
local ysul07 				= 2007

local yES_RJ_MG2003				= 2003
local ysudeste 					= 2004
local ysudeste05 				= 2005
local ysudeste06 				= 2006
local ysudeste07 				= 2007

***** Booleans *****
local 	split_regions		= 1		/* Split region files into state-specific */

* Display date and time
di "Starting split regions code on $S_DATE at $S_TIME"

******************* Split regions *******************

if `split_regions'==1{

		foreach region in north southeast northeast centro south{	
		
			foreach f in `f_`region''{
			
				di "Splitting file deID_`f'.dta into states"

				u "${deIDrais}/dta/`updatedate'/deID_`f'.dta"  /*if _n<1000*/, clear
				
				tostring municipality, gen(state)
				replace state = substr(state,1,2)
				destring state, replace
				
				levelsof state, local(states)
				foreach x in `states'{
					local cleanname "`s`x''`y`f''"
					di "Will save file deID_`cleanname'ID.dta"
					preserve
						keep if state==`x'
						drop state
						saveold "${deIDrais}/dta/`updatedate'/deID_`cleanname'ID.dta", replace
					restore
				}
			
				
			} /* Close files within region */
		} /* Close regions */
}

* Display date and time
di "Ending split regions code on $S_DATE at $S_TIME"
