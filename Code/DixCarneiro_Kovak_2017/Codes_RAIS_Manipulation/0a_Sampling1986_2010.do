clear

* Sample workers in order to construct a panel spanning 1986-2010
* Obtain list of all IDs that show up between 1986 and 2010
* Select 3% random sample of these IDs
* Run code 0b_Panel1986_2010 in order to construct the panel with the
* individuals selected in this code

global root "\\storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\DixCarneiro_Kovak_2017\"

global rais "\\storage6\bases\DADOS\RESTRITO\RAIS\Stata\"
global result "${root}ProcessedData_RAIS\Panel_1986_2010\"

cap log close

log using ${result}Sampling1986_2010.log, replace

set more off

forvalues year = 1986/2010{

	local yr = substr("`year'",3,2)
	
	use ${rais}brasil`yr'

	keep if (rem_dez`yr' ~= 0 & rem_dez`yr' ~= .) | (rem_media`yr' ~= 0 & rem_media`yr' ~= .)

	keep pis`yr'

	duplicates drop

	rename pis`yr' pis
	
	sort pis

	save ${result}pis`yr', replace
	
}	

clear

use ${result}pis86

forvalues year = 1987/2010{
	
	local yr = substr("`year'",3,2)
	append using ${result}pis`yr'
	erase ${result}pis`yr'.dta
}	

erase ${result}pis86.dta

sort pis
duplicates drop

/*1� passo - qualquer PIS com tamanho diferente de 11 � eliminado*/	
drop if length(trim(pis)) ~= 11 

/*programa para verifica��o da consist�ncia do PIS - f�rmula encotrada na internet e muito consistente*/
gen dig1    = substr(pis,1,1)
gen dig2    = substr(pis,2,1)
gen dig3    = substr(pis,3,1)
gen dig4    = substr(pis,4,1)
gen	dig5    = substr(pis,5,1)
gen	dig6    = substr(pis,6,1)
gen	dig7    = substr(pis,7,1)
gen	dig8    = substr(pis,8,1)
gen	dig9    = substr(pis,9,1)
gen	dig10   = substr(pis,10,1)
gen dig_pis = substr(pis,11,1)

forvalues i = 1(1)10{
	destring dig`i', replace
}
destring dig_pis, replace

gen formula = (dig1*3 + dig2*2 + dig3*9 + dig4*8 + dig5*7 + dig6*6 + dig7*5 + dig8*4 + dig9*3 + dig10*2)
		
gen digito1=(11-mod(formula,11))

replace digito1 = 0 if digito1 == 10 | digito1 == 11
		
gen pis_invalido = .
replace pis_invalido = 1 if dig_pis ~= digito1
replace	pis_invalido = 0 if dig_pis == digito1		

tab pis_invalido

* We drop all invalid PIS codes before sampling
drop if pis_invalido == 1

keep pis

save ${result}pis_list, replace

set seed 8634493

gsample 3.0, percent wor

save ${result}pis_sample, replace

********************************************************************************
********************************************************************************

clear

set more off

forvalues year = 1986/1993{
	
	clear all
	local y = substr("`year'", 3, 2) 

	use pis`y' cnpj`y' subs_ibge`y' cbo94_`y' codemun`y' grau_instr`y' sexo`y' idade`y' ///
    rem_dez`y' temp_empr`y' mes_adm`y' mes_deslig`y' rem_media`y' using ${rais}brasil`y' 
	
	rename cbo94_`y' cbo94`y'
	
	* Eliminate the year suffix from all the variable names
	foreach var of varlist *`y' {
		local newvar = regexr("`var'","`y'","")
		rename `var' `newvar'
	}	

	duplicates drop

	sort pis
	merge m:1 pis using ${result}pis_sample
	keep if _merge == 3 | _merge == 2
	drop _merge
	
	gen year = `year'

	save ${result}sample`y', replace
	
}	

********************************************************************************
********************************************************************************

forvalues year = 1994/2010{
	
	clear all
	local y = substr("`year'", 3, 2) 

	use pis`y' cnpj`y' subs_ibge`y' cbo94_`y' codemun`y' grau_instr`y' sexo`y' idade`y' ///
    rem_dez`y' temp_empr`y' mes_adm`y' mes_deslig`y' rem_media`y' clas_cnae`y' ///
	horas_contr`y' using ${rais}brasil`y' 
	
	rename cbo94_`y' cbo94`y'
	
	* Eliminate the year suffix from all the variable names
	foreach var of varlist *`y' {
		local newvar = regexr("`var'","`y'","")
		rename `var' `newvar'
	}	

	duplicates drop

	sort pis
	merge m:1 pis using ${result}pis_sample
	keep if _merge == 3 | _merge == 2
	drop _merge
	
	gen year = `year'

	save ${result}sample`y', replace
	
}	

********************************************************************************
********************************************************************************

clear

use ${result}sample86

forvalues year = 1987/2010{

	local y = substr("`year'", 3, 2) 
	append using ${result}sample`y'
	
}

sort pis year

save ${result}am_rais_final, replace

forvalues year = 1986/2010{

	local y = substr("`year'", 3, 2) 
	erase ${result}sample`y'.dta
	
}

log close
