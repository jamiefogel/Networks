clear

* Sample workers in order to construct a panel spanning 1986-2010
* Obtain list of all IDs that show up between 1986 and 2010
* Select 3% random sample of these IDs
* Run code 0b_Panel1986_2010 in order to construct the panel with the
* individuals selected in this code

global root "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/Data/DixCarneiroKovak_ReplicationFiles/"

global rais "//storage6/bases/DADOS/RESTRITO/RAIS/Stata/"
global result "${root}ProcessedData_RAIS/Panel_1986_2010/"

cap log close

log using ${result}Sampling1986_2010.log, replace

set more off

forvalues year = 1986/2010{

	local yr = substr("`year'",3,2)
	
	if  inrange(`year', 2003, 2006){
		use pis rem_media rem_dez  using ${rais}brasil`year'
		rename (rem_media rem_dez) (rem_med_sm rem_dez_sm)
	}
	else{
		use pis rem_med_sm rem_dez_sm  using ${rais}brasil`year'
	}
	
	keep if (rem_dez_sm ~= 0 & rem_dez_sm ~= .) | (rem_med_sm ~= 0 & rem_med_sm ~= .)

	keep pis

	duplicates drop

	*rename pis`year' pis
	
	sort pis

	save ${result}pis`year', replace
	
}	

clear

use ${result}pis1986

forvalues year = 1987/2010{
	
	local yr = substr("`year'",3,2)
	append using ${result}pis`year'
	erase ${result}pis`year'.dta
}	

erase ${result}pis1986.dta

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
	di "`year'"
	
	clear all
	local y = substr("`year'", 3, 2) 

	*use pis cnpj subs_ibge cbo94 codemun grau_instr sexo idade ///
    *rem_dez temp_empr mes_adm mes_deslig rem_media  using ${rais}brasil`year' 
	use pis cnpj subs_ibge cbo1994 codemun grau_instr genero fx_etaria ///
    rem_dez_sm temp_empr mes_adm mes_deslig rem_med_sm  using ${rais}brasil`year' 
	
	*rename cbo94_`y' cbo94`y'
	
	* Eliminate the year suffix from all the variable names
	*foreach var of varlist *`y' {
	*	local newvar = regexr("`var'","`y'","")
	*	rename `var' `newvar'
	*}	

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
	if `year'<=2002 {
		local cbo cbo1994
		local clas_cnae clas_cnae
	}
	else{
		local cbo cbo2002
		local clas_cnae clas_cnae10
	}
	*use pis cnpj subs_ibge cbo94_ codemun grau_instr sexo idade ///
    *rem_dez temp_empr mes_adm mes_deslig rem_media clas_cnae ///
	*horas_contr  using ${rais}brasil`year' 
		
	if  inrange(`year', 2003, 2006){
		use pis cnpj subs_ibge `cbo' codemun grau_instr genero idade ///
		rem_dez temp_empr mes_adm mes_deslig rem_media `clas_cnae' ///
		horas_contr  using ${rais}brasil`year' 
		rename (rem_media rem_dez) (rem_med_sm rem_dez_sm)
	}
	else{
		use pis cnpj_raiz subs_ibge `cbo' codemun grau_instr genero idade ///
		rem_dez_sm temp_empr mes_adm mes_deslig rem_med_sm `clas_cnae' ///
		horas_contr  using ${rais}brasil`year' 
	}
	
	

	
	* Eliminate the year suffix from all the variable names
	*foreach var of varlist *`y' {
	*	local newvar = regexr("`var'","`y'","")
	*	rename `var' `newvar'
	*}	

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
