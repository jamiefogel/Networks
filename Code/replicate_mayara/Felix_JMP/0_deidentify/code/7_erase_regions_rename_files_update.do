/*
	Mayara Felix
	
	Erase region files and rename other files to fit state-year standard
	
*/

clear all
set more off
unicode encoding set "latin1"
set seed 34317154

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

local updatedate		= 20191213

* Region-specific files
local f_northeast	"Nordeste2003		nordeste	nordeste05	nordeste06	nordeste07"
local f_north		"Norte2003			Norte	norte05	norte06	norte07"
local f_centro		"Centro_Oeste2003	centro_oeste	centro05	centro06 centooeste07"
local f_south		"Sul2003			sul	sul05	sul06	sul07"
local f_southeast	"ES_RJ_MG2003		sudeste	sudeste05	sudeste06	sudeste07"

***** Booleans *****
local 	erase_regions		= 0		/* Erase large region files after they have been split*/
local 	rename_files		= 0		/* Rename files */
local   rename_more			= 0		/* Rename additional files */
local	move_estabs			= 1		/* Move establishment files to subfolder */

* Erase region files
if `erase_regions'==1{
	cd "${deIDrais}/dta/`updatedate'"
	foreach region in north southeast northeast centro south{	
		
			foreach f in `f_`region''{
				shell rm "deID_`f'.dta"
			}	
	}
}

if `rename_files'==1{
	cd "${deIDrais}/dta/`updatedate'"
	
	shell mv deID_sp2003.dta 	deID_SP2003ID.dta
	shell mv deID_SP.dta		deID_SP2004ID.dta
	shell mv deID_sp105.dta		deID_SP2005ID1.dta
	shell mv deID_sp05.dta		deID_SP2005ID2.dta
	shell mv deID_sp106.dta		deID_SP2006ID1.dta
	shell mv deID_sp06.dta		deID_SP2006ID2.dta
	shell mv deID_sp07.dta		deID_SP2007ID.dta
	shell mv "deID_sp08 1.dta"	deID_SP2008ID1.dta
	shell mv deID_sp08.dta		deID_SP2008ID2.dta
}

if `rename_more'==1{
	cd "${deIDrais}/dta/`updatedate'"
	
	shell mv deID_Estb2014.dta 		deID_Estb2014ID.dta
	shell mv deID_ESTB2015ID.dta	deID_Estb2015ID.dta
	shell mv deID_GO2009ID2.dta		deID_GO2009ID.dta
}

if `move_estabs'==1{
	cap mkdir "${deIDrais}/dta/`updatedate'/establishment_info_files/"
	
	cd "${deIDrais}/dta/`updatedate'"
	shell mv deID_Estb*	"${deIDrais}/dta/`updatedate'/establishment_info_files/"

}

